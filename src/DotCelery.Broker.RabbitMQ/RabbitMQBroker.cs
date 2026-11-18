using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Channels;
using DotCelery.Core.Abstractions;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Models;
using DotCelery.Core.Security;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace DotCelery.Broker.RabbitMQ;

/// <summary>
/// RabbitMQ message broker implementation.
/// </summary>
public sealed partial class RabbitMQBroker : IMessageBroker, IBrokerCapabilities
{
    private readonly RabbitMQBrokerOptions _options;
    private readonly ILogger<RabbitMQBroker> _logger;
    private readonly IDeadLetterStore? _deadLetterStore;
    private readonly DeadLetterOptions _deadLetterOptions;
    private readonly IMessageSecurityValidator? _messageSecurityValidator;
    private readonly IQueueMetrics? _queueMetrics;
    private readonly IReadOnlyDictionary<string, object?> _queueArguments;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly SemaphoreSlim _publishChannelLock = new(1, 1);

    // The channel a message arrived on, so it is settled on that channel and no other. The key
    // is the delivery's identity, not the instance, because a caller may settle an equivalent
    // copy, for example one with an ETA attached before a requeue.
    private readonly ConcurrentDictionary<DeliveryKey, ConsumerChannel> _deliveries = new();

    // AOT-friendly type info for TaskMessage serialization
    private static JsonTypeInfo<TaskMessage> TaskMessageTypeInfo =>
        DotCeleryJsonContext.Default.TaskMessage;

    private IConnection? _connection;
    private IChannel? _publishChannel;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMQBroker"/> class.
    /// </summary>
    /// <param name="options">The broker options.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="deadLetterStore">Optional dead letter store for deserialization failures.</param>
    /// <param name="messageSecurityValidator">Optional message security validator.</param>
    /// <param name="queueMetrics">Optional queue metrics store the publishes are recorded in.</param>
    public RabbitMQBroker(
        IOptions<RabbitMQBrokerOptions> options,
        ILogger<RabbitMQBroker> logger,
        IDeadLetterStore? deadLetterStore = null,
        IMessageSecurityValidator? messageSecurityValidator = null,
        IQueueMetrics? queueMetrics = null
    )
        : this(
            options,
            logger,
            Options.Create(new DeadLetterOptions()),
            deadLetterStore,
            messageSecurityValidator,
            queueMetrics
        ) { }

    /// <summary>Initializes the broker with explicit dead-letter settings.</summary>
    public RabbitMQBroker(
        IOptions<RabbitMQBrokerOptions> options,
        ILogger<RabbitMQBroker> logger,
        IOptions<DeadLetterOptions> deadLetterOptions,
        IDeadLetterStore? deadLetterStore,
        IMessageSecurityValidator? messageSecurityValidator,
        IQueueMetrics? queueMetrics
    )
    {
        _options = options.Value;
        _logger = logger;
        _deadLetterStore = deadLetterStore;
        _deadLetterOptions = deadLetterOptions.Value;
        _messageSecurityValidator = messageSecurityValidator;
        _queueMetrics = queueMetrics;

        // Snapshot the arguments, so later edits to the options do not change what queues are
        // declared with
        _queueArguments = new Dictionary<string, object?>(_options.QueueArguments);
    }

    /// <inheritdoc />
    public async ValueTask PublishAsync(
        TaskMessage message,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(message);

        await _publishChannelLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            var channel = await GetPublishChannelAsync(cancellationToken).ConfigureAwait(false);

            if (_options.AutoDeclareQueues)
            {
                await EnsureQueueDeclaredAsync(channel, message.Queue, cancellationToken)
                    .ConfigureAwait(false);
            }

            // Serialize using AOT-friendly type info
            var body = JsonSerializer.SerializeToUtf8Bytes(message, TaskMessageTypeInfo);

            // Validate message size
            if (_options.MaxMessageSizeBytes > 0 && body.Length > _options.MaxMessageSizeBytes)
            {
                throw new InvalidOperationException(
                    $"Message size ({body.Length} bytes) exceeds maximum allowed size ({_options.MaxMessageSizeBytes} bytes)."
                );
            }

            var properties = new BasicProperties
            {
                Persistent = true,
                MessageId = message.Id,
                Type = message.Task,
                ContentType = "application/json",
                Timestamp = new AmqpTimestamp(message.Timestamp.ToUnixTimeSeconds()),
                Priority = (byte)Math.Clamp(message.Priority, 0, 9),
                CorrelationId = message.CorrelationId,
            };

            var signature = _messageSecurityValidator?.Sign(body);
            if (!string.IsNullOrEmpty(signature))
            {
                properties.Headers = new Dictionary<string, object?>
                {
                    ["x-dotcelery-signature"] = Encoding.UTF8.GetBytes(signature),
                };
            }

            if (message.Expires.HasValue)
            {
                var ttl = message.Expires.Value - DateTimeOffset.UtcNow;
                if (ttl > TimeSpan.Zero)
                {
                    properties.Expiration = ((long)ttl.TotalMilliseconds).ToString(
                        System.Globalization.CultureInfo.InvariantCulture
                    );
                }
            }

            await channel
                .BasicPublishAsync(
                    exchange: _options.Exchange,
                    routingKey: message.Queue,
                    mandatory: _options.MandatoryPublish,
                    basicProperties: properties,
                    body: body,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
        }
        finally
        {
            _publishChannelLock.Release();
        }

        // The publish is confirmed, so the message is in the queue and its waiting count follows
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
    /// <remarks>
    /// The loop owns its channel and rebuilds it, and its consumers, after the channel or its
    /// connection is lost, so the stream pauses across a broker restart instead of ending.
    /// Anything the lost channel had delivered is requeued by the broker and delivered again on
    /// the new channel.
    /// </remarks>
    public async IAsyncEnumerable<BrokerMessage> ConsumeAsync(
        IReadOnlyList<string> queues,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(queues);

        // Create a channel to buffer messages
        var messageChannel = Channel.CreateBounded<BrokerMessage>(
            new BoundedChannelOptions(100)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false,
            }
        );

        using var consumingCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var consuming = ConsumeUntilLostAsync(queues, messageChannel.Writer, consumingCts.Token);

        try
        {
            await foreach (
                var message in messageChannel
                    .Reader.ReadAllAsync(cancellationToken)
                    .ConfigureAwait(false)
            )
            {
                yield return message;
            }
        }
        finally
        {
            await consumingCts.CancelAsync().ConfigureAwait(false);

            try
            {
                await consuming.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error while stopping the consumer");
            }

            messageChannel.Writer.TryComplete();
        }
    }

    /// <inheritdoc />
    public async ValueTask AckAsync(
        BrokerMessage message,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(message);

        if (message.DeliveryTag is not DeliveryReceipt receipt)
        {
            throw new ArgumentException("Invalid delivery tag", nameof(message));
        }

        var consumerChannel = RequireDeliveringChannel(message, "acknowledged");
        await consumerChannel.AckAsync(receipt.Tag, cancellationToken).ConfigureAwait(false);
        _deliveries.TryRemove(KeyOf(message), out _);

        _logger.LogDebug("Acknowledged message {MessageId}", message.Message.Id);
    }

    /// <inheritdoc />
    public async ValueTask RejectAsync(
        BrokerMessage message,
        bool requeue = false,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(message);

        if (message.DeliveryTag is not DeliveryReceipt receipt)
        {
            throw new ArgumentException("Invalid delivery tag", nameof(message));
        }

        var consumerChannel = RequireDeliveringChannel(message, "rejected");
        await consumerChannel
            .RejectAsync(receipt.Tag, requeue, cancellationToken)
            .ConfigureAwait(false);
        _deliveries.TryRemove(KeyOf(message), out _);

        _logger.LogDebug(
            "Rejected message {MessageId} (requeue: {Requeue})",
            message.Message.Id,
            requeue
        );
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
            var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
            return connection.IsOpen;
        }
        catch
        {
            return false;
        }
    }

    /// <inheritdoc />
    public MessageBrokerCapabilities Capabilities =>
        new()
        {
            Priority = true,
            RedeliveryOnConsumerStop = true,
            RejectsStaleSettlements = true,
            MaxMessageSizeBytes =
                _options.MaxMessageSizeBytes > 0 ? _options.MaxMessageSizeBytes : null,
        };

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        await _publishChannelLock.WaitAsync().ConfigureAwait(false);

        try
        {
            if (_publishChannel is not null)
            {
                await _publishChannel.CloseAsync().ConfigureAwait(false);
                _publishChannel.Dispose();
            }

            if (_connection is not null)
            {
                await _connection.CloseAsync().ConfigureAwait(false);
                _connection.Dispose();
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error during RabbitMQ broker disposal");
        }
        finally
        {
            _publishChannelLock.Release();
        }
        _connectionLock.Dispose();
        _deliveries.Clear();

        _logger.LogInformation("RabbitMQ broker disposed");
    }
}
