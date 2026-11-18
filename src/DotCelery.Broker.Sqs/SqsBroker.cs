using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using DotCelery.Core.Abstractions;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Models;
using DotCelery.Core.Security;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Broker.Sqs;

/// <summary>
/// Amazon SQS message broker.
/// </summary>
/// <remarks>
/// <para>
/// A queue maps to an SQS queue by name. Delivery is at-least-once: a message that is not
/// settled becomes visible again when its visibility timeout expires, and its consumer keeps
/// the visibility renewed while it processes it, so a task that runs long is not run twice.
/// </para>
/// <para>
/// A message with a future ETA is published with a delivery delay, and a message returned to
/// the broker keeps its visibility hidden until its ETA, so a scheduled task does not spin
/// through a consumer that waits on it. SQS delays at most 15 minutes; a further ETA is
/// published with the maximum delay and the remainder is waited out by the worker.
/// </para>
/// <para>
/// A rejected message is deleted, which discards it: dead-lettering is the queue's redrive
/// policy, which moves a message a number of times received, not a per-message decision.
/// </para>
/// </remarks>
public sealed partial class SqsBroker : IMessageBroker, IBrokerCapabilities
{
    /// <summary>The message attribute carrying the HMAC signature of the body.</summary>
    internal const string SignatureAttribute = "x-dotcelery-signature";

    // SQS caps a publish delay at 15 minutes and a visibility timeout at 12 hours
    private const int MaxDelaySeconds = 900;
    private const int MaxVisibilitySeconds = 43_200;

    private readonly SqsBrokerOptions _options;
    private readonly ConcurrentDictionary<string, ConsumeSession> _deliveries = new();
    private readonly SemaphoreSlim _deliveryLock = new(1, 1);
    private readonly ILogger<SqsBroker> _logger;
    private readonly IDeadLetterStore? _deadLetterStore;
    private readonly IMessageSecurityValidator? _messageSecurityValidator;
    private readonly IQueueMetrics? _queueMetrics;
    private readonly AmazonSQSClient _client;
    private readonly ConcurrentDictionary<string, string> _queueUrls = new(StringComparer.Ordinal);
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="SqsBroker"/> class.
    /// </summary>
    /// <param name="options">The broker options.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="deadLetterStore">Optional dead letter store for messages that cannot be read.</param>
    /// <param name="messageSecurityValidator">Optional message security validator.</param>
    /// <param name="queueMetrics">Optional queue metrics store the publishes are recorded in.</param>
    public SqsBroker(
        IOptions<SqsBrokerOptions> options,
        ILogger<SqsBroker> logger,
        IDeadLetterStore? deadLetterStore = null,
        IMessageSecurityValidator? messageSecurityValidator = null,
        IQueueMetrics? queueMetrics = null
    )
    {
        ArgumentNullException.ThrowIfNull(options);

        _options = options.Value;
        _logger = logger;
        _deadLetterStore = deadLetterStore;
        _messageSecurityValidator = messageSecurityValidator;
        _queueMetrics = queueMetrics;

        var config = new AmazonSQSConfig
        {
            RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(_options.Region),
            AuthenticationRegion = _options.Region,
        };

        if (!string.IsNullOrEmpty(_options.ServiceUrl))
        {
            config.ServiceURL = _options.ServiceUrl;
        }

        _client = _options.AccessKey is { Length: > 0 } accessKey
            ? new AmazonSQSClient(
                new BasicAWSCredentials(accessKey, _options.SecretKey ?? string.Empty),
                config
            )
            : new AmazonSQSClient(config);
    }

    /// <inheritdoc />
    public MessageBrokerCapabilities Capabilities =>
        new()
        {
            // A publish delay hides a scheduled message, and its consumer keeps the delivery
            // invisible while it processes it
            NativeDelay = true,
            RedeliveryOnConsumerStop = true,
            HoldsRequeuedMessagesUntilEta = true,
            RejectsStaleSettlements = true,
            MaxMessageSizeBytes =
                _options.MaxMessageSizeBytes > 0 ? _options.MaxMessageSizeBytes : null,
        };

    /// <inheritdoc />
    public async ValueTask PublishAsync(
        TaskMessage message,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(message);

        var body = JsonSerializer.SerializeToUtf8Bytes(
            message,
            DotCeleryJsonContext.Default.TaskMessage
        );

        if (_options.MaxMessageSizeBytes > 0 && body.Length > _options.MaxMessageSizeBytes)
        {
            throw new InvalidOperationException(
                $"Message size ({body.Length} bytes) exceeds the maximum ({_options.MaxMessageSizeBytes} bytes)."
            );
        }

        var queueUrl = await GetQueueUrlAsync(message.Queue, cancellationToken)
            .ConfigureAwait(false);

        var request = new SendMessageRequest
        {
            QueueUrl = queueUrl,
            MessageBody = Encoding.UTF8.GetString(body),
            DelaySeconds = DelaySecondsUntil(message.Eta),
        };

        var signature = _messageSecurityValidator?.Sign(body);
        if (!string.IsNullOrEmpty(signature))
        {
            request.MessageAttributes = new Dictionary<string, MessageAttributeValue>
            {
                [SignatureAttribute] = new() { DataType = "String", StringValue = signature },
            };
        }

        await _client.SendMessageAsync(request, cancellationToken).ConfigureAwait(false);

        // The message is accepted by the queue, so its waiting count follows what was published
        await _queueMetrics
            .RecordEnqueueAsync(message.Queue, _logger, cancellationToken)
            .ConfigureAwait(false);

        _logger.LogDebug(
            "Published message {MessageId} to queue {Queue}",
            message.Id,
            message.Queue
        );
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<BrokerMessage> ConsumeAsync(
        IReadOnlyList<string> queues,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(queues);

        var messageChannel = Channel.CreateBounded<BrokerMessage>(
            new BoundedChannelOptions(Math.Max(1, _options.BatchSize) * 2)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = true,
            }
        );

        using var consumeCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // The deliveries of this consumption, so their visibility is renewed while it is alive
        // and left to expire when it stops
        var session = new ConsumeSession();
        var receiving = ReceiveAsync(queues, messageChannel.Writer, consumeCts.Token);
        var renewing = RenewVisibilityAsync(session, consumeCts.Token);

        try
        {
            await foreach (
                var message in messageChannel
                    .Reader.ReadAllAsync(cancellationToken)
                    .ConfigureAwait(false)
            )
            {
                if (message.DeliveryTag is string receiptHandle)
                {
                    // Ownership is tracked for every delivery, not only when visibility is
                    // renewed, so a settlement after this consumer stops is refused.
                    session.Track(message.Queue, receiptHandle);
                    _deliveries[receiptHandle] = session;
                }

                yield return message;
            }
        }
        finally
        {
            await consumeCts.CancelAsync().ConfigureAwait(false);
            await WaitQuietlyAsync(receiving).ConfigureAwait(false);
            await WaitQuietlyAsync(renewing).ConfigureAwait(false);
            foreach (var (_, receiptHandle) in session.Snapshot())
            {
                _deliveries.TryRemove(receiptHandle, out _);
                session.Forget(receiptHandle);
            }

            // Messages received but never handed over are made visible again at once, so they
            // are not hidden for the whole visibility timeout after a consumer stops
            var undelivered = new List<BrokerMessage>();
            while (messageChannel.Reader.TryRead(out var message))
            {
                undelivered.Add(message);
            }

            await ReturnUndeliveredAsync(undelivered).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async ValueTask AckAsync(
        BrokerMessage message,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        var (queueUrl, receiptHandle) = await DeliveryAsync(message, cancellationToken)
            .ConfigureAwait(false);

        await _deliveryLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await _client
                .DeleteMessageAsync(queueUrl, receiptHandle, cancellationToken)
                .ConfigureAwait(false);
            ForgetDelivery(receiptHandle);
        }
        finally
        {
            _deliveryLock.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask RejectAsync(
        BrokerMessage message,
        bool requeue = false,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        var (queueUrl, receiptHandle) = await DeliveryAsync(message, cancellationToken)
            .ConfigureAwait(false);

        await _deliveryLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!requeue)
            {
                // Discard; a dead letter queue is the queue's redrive policy
                await _client
                    .DeleteMessageAsync(queueUrl, receiptHandle, cancellationToken)
                    .ConfigureAwait(false);
                ForgetDelivery(receiptHandle);
                return;
            }

            await _client
                .ChangeMessageVisibilityAsync(
                    new ChangeMessageVisibilityRequest
                    {
                        QueueUrl = queueUrl,
                        ReceiptHandle = receiptHandle,
                        VisibilityTimeout = VisibilityUntil(message.Message.Eta),
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
            ForgetDelivery(receiptHandle);
        }
        finally
        {
            _deliveryLock.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsHealthyAsync(CancellationToken cancellationToken = default)
    {
        if (_disposed)
        {
            return false;
        }

        try
        {
            await _client
                .ListQueuesAsync(new ListQueuesRequest { MaxResults = 1 }, cancellationToken)
                .ConfigureAwait(false);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogDebug(ex, "SQS health check failed");
            return false;
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return ValueTask.CompletedTask;
        }

        _disposed = true;
        _client.Dispose();
        return ValueTask.CompletedTask;
    }

    // The seconds a publish hides a scheduled message: SQS delays at most 15 minutes, and a
    // further ETA is waited out by the worker
    private static int DelaySecondsUntil(DateTimeOffset? eta)
    {
        if (eta is not { } deliveryTime)
        {
            return 0;
        }

        var delay = deliveryTime - DateTimeOffset.UtcNow;
        if (delay <= TimeSpan.Zero)
        {
            return 0;
        }

        return (int)Math.Min(Math.Ceiling(delay.TotalSeconds), MaxDelaySeconds);
    }

    // The seconds a returned message stays hidden: until its ETA, capped at 12 hours, so a
    // message the worker returned to the broker is not delivered again before it is due
    private static int VisibilityUntil(DateTimeOffset? eta)
    {
        if (eta is not { } deliveryTime)
        {
            return 0;
        }

        var delay = deliveryTime - DateTimeOffset.UtcNow;
        if (delay <= TimeSpan.Zero)
        {
            return 0;
        }

        return (int)Math.Min(Math.Ceiling(delay.TotalSeconds), MaxVisibilitySeconds);
    }

    private async ValueTask<(string QueueUrl, string ReceiptHandle)> DeliveryAsync(
        BrokerMessage message,
        CancellationToken cancellationToken,
        bool requireOwnership = true
    )
    {
        ArgumentNullException.ThrowIfNull(message);

        if (message.DeliveryTag is not string receiptHandle)
        {
            throw new ArgumentException(
                "Invalid delivery tag type, expected string",
                nameof(message)
            );
        }

        if (requireOwnership && !_deliveries.ContainsKey(receiptHandle))
        {
            throw new InvalidOperationException(
                $"Message {message.Message.Id} cannot be settled: it was settled already, or its consumer stopped."
            );
        }

        var queueUrl = await GetQueueUrlAsync(message.Queue, cancellationToken)
            .ConfigureAwait(false);

        return (queueUrl, receiptHandle);
    }

    private async ValueTask<string> GetQueueUrlAsync(
        string queue,
        CancellationToken cancellationToken
    )
    {
        if (_queueUrls.TryGetValue(queue, out var known))
        {
            return known;
        }

        string queueUrl;

        try
        {
            var response = await _client
                .GetQueueUrlAsync(queue, cancellationToken)
                .ConfigureAwait(false);
            queueUrl = response.QueueUrl;
        }
        catch (QueueDoesNotExistException) when (_options.AutoCreateQueues)
        {
            var created = await _client
                .CreateQueueAsync(
                    new CreateQueueRequest
                    {
                        QueueName = queue,
                        Attributes = new Dictionary<string, string>
                        {
                            ["VisibilityTimeout"] = (
                                (int)Math.Max(0, _options.VisibilityTimeout.TotalSeconds)
                            ).ToString(CultureInfo.InvariantCulture),
                            ["MessageRetentionPeriod"] = (
                                (int)Math.Max(0, _options.MessageRetention.TotalSeconds)
                            ).ToString(CultureInfo.InvariantCulture),
                        },
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
            queueUrl = created.QueueUrl;
        }

        _queueUrls[queue] = queueUrl;
        return queueUrl;
    }

    private static TimeSpan NextFailureDelay(TimeSpan current) =>
        current <= TimeSpan.Zero ? TimeSpan.FromSeconds(1)
        : current >= TimeSpan.FromSeconds(30) ? TimeSpan.FromSeconds(30)
        : current * 2;

    private static async Task WaitQuietlyAsync(Task task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The receive and renewal tasks report their own failures
        }
    }

    // The deliveries one consumption handed to its caller and has not settled
    private sealed class ConsumeSession
    {
        private readonly Lock _lock = new();
        private readonly List<(string Queue, string ReceiptHandle)> _deliveries = [];

        public void Track(string queue, string receiptHandle)
        {
            lock (_lock)
            {
                _deliveries.RemoveAll(delivery => delivery.ReceiptHandle == receiptHandle);
                _deliveries.Add((queue, receiptHandle));
            }
        }

        public void Forget(string receiptHandle)
        {
            lock (_lock)
            {
                _deliveries.RemoveAll(delivery => delivery.ReceiptHandle == receiptHandle);
            }
        }

        public List<(string Queue, string ReceiptHandle)> Snapshot()
        {
            lock (_lock)
            {
                return [.. _deliveries];
            }
        }
    }
}
