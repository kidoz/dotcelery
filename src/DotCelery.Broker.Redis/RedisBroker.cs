using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Channels;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Security;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace DotCelery.Broker.Redis;

/// <summary>
/// Redis Streams implementation of <see cref="IMessageBroker"/>.
/// Uses Redis Streams with consumer groups for reliable message delivery.
/// </summary>
public sealed partial class RedisBroker : IMessageBroker, IBrokerCapabilities
{
    private readonly RedisBrokerOptions _options;
    private readonly ILogger<RedisBroker> _logger;
    private readonly IMessageSecurityValidator? _messageSecurityValidator;
    private readonly IQueueMetrics? _queueMetrics;
    private readonly string _consumerName;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly HashSet<string> _initializedGroups = [];

    // Deliveries handed to the consumer and not settled yet, by delivery tag. Their claims are
    // renewed while they are processed, and they are removed from the stream when settled.
    private readonly ConcurrentDictionary<string, RedisDelivery> _deliveries = new(
        StringComparer.Ordinal
    );

    private static readonly TimeSpan MinFailureDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxFailureDelay = TimeSpan.FromSeconds(30);

    // AOT-friendly type info for TaskMessage serialization
    private static JsonTypeInfo<TaskMessage> TaskMessageTypeInfo =>
        DotCeleryJsonContext.Default.TaskMessage;

    private ConnectionMultiplexer? _connection;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisBroker"/> class.
    /// </summary>
    /// <param name="options">The broker options.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="messageSecurityValidator">Optional message security validator.</param>
    /// <param name="queueMetrics">Optional queue metrics store the publishes are recorded in.</param>
    public RedisBroker(
        IOptions<RedisBrokerOptions> options,
        ILogger<RedisBroker> logger,
        IMessageSecurityValidator? messageSecurityValidator = null,
        IQueueMetrics? queueMetrics = null
    )
    {
        _options = options.Value;
        ArgumentException.ThrowIfNullOrWhiteSpace(_options.DeadLetterStreamPrefix);
        _logger = logger;
        _messageSecurityValidator = messageSecurityValidator;
        _queueMetrics = queueMetrics;
        _consumerName =
            _options.ConsumerName
            ?? $"{Environment.MachineName}-{Environment.ProcessId}-{Guid.NewGuid():N}";

        _logger.LogInformation(
            "Redis broker initialized with consumer name: {ConsumerName}",
            _consumerName
        );
    }

    /// <inheritdoc />
    public async ValueTask PublishAsync(
        TaskMessage message,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(message);

        await PublishCoreAsync(message, cancellationToken).ConfigureAwait(false);

        // The entry is in the stream, so the queue's waiting count follows what was published
        await _queueMetrics
            .RecordEnqueueAsync(message.Queue, _logger, cancellationToken)
            .ConfigureAwait(false);
    }

    // Publishes without recording an enqueue: requeues go through here so a returned message is
    // not counted as a fresh enqueue
    private async Task PublishCoreAsync(TaskMessage message, CancellationToken cancellationToken)
    {
        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var streamKey = GetStreamKey(message.Queue);

        var entries = CreateStreamEntry(message);

        // Add to stream with optional trimming
        if (_options.MaxStreamLength.HasValue)
        {
            await db.StreamAddAsync(
                    streamKey,
                    entries,
                    maxLength: _options.MaxStreamLength.Value,
                    useApproximateMaxLength: true
                )
                .ConfigureAwait(false);
        }
        else
        {
            await db.StreamAddAsync(streamKey, entries).ConfigureAwait(false);
        }

        _logger.LogDebug("Published message {MessageId} to stream {Stream}", message.Id, streamKey);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<BrokerMessage> ConsumeAsync(
        IReadOnlyList<string> queues,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(queues);

        if (queues.Count == 0)
        {
            yield break;
        }

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);

        // Ensure consumer groups exist for all queues
        if (_options.AutoCreateStreams)
        {
            foreach (var queue in queues)
            {
                await EnsureConsumerGroupAsync(db, GetStreamKey(queue)).ConfigureAwait(false);
            }
        }

        // Build stream keys
        var streamKeys = queues.Select(GetStreamKey).ToArray();

        // Create a channel for buffering messages
        var messageChannel = Channel.CreateBounded<BrokerMessage>(
            new BoundedChannelOptions(_options.PrefetchCount * 2)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = true,
            }
        );

        // Stops the read loop when the caller stops enumerating, not only on cancellation
        using var readCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // The deliveries of this consumption, so their claims are renewed while it is alive and
        // left to expire when it stops
        var session = new ConsumeSession($"{_consumerName}-{Guid.NewGuid():N}");

        // Start background task to read from streams
        var readTask = ReadStreamMessagesAsync(
            db,
            streamKeys,
            messageChannel.Writer,
            session,
            readCts.Token
        );

        // Keeps the claims on messages that are still being processed
        var renewTask = RenewClaimsAsync(db, session, readCts.Token);

        // Yield messages from the channel
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
            await readCts.CancelAsync().ConfigureAwait(false);

            // Ensure read task completes
            try
            {
                await readTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error in stream read task during cleanup");
            }

            try
            {
                await renewTask.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error in claim renewal task during cleanup");
            }

            // Messages read from the stream but never handed to the caller can be returned right
            // away; returning them needs their delivery ownership still tracked
            var undelivered = new List<BrokerMessage>();
            while (messageChannel.Reader.TryRead(out var message))
            {
                undelivered.Add(message);
            }

            await ReturnUndeliveredAsync(undelivered).ConfigureAwait(false);

            // Deliveries this session left unsettled are no longer in flight: they stay pending
            // in the group and the next consumer reclaims them once their claim expires, so
            // stopping a consumption never pins a message forever
            foreach (var delivery in session.Deliveries)
                ForgetDelivery(delivery);
            foreach (var streamKey in streamKeys)
                await RemoveIdleConsumerAsync(db, streamKey, session.ConsumerName)
                    .ConfigureAwait(false);
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
        cancellationToken.ThrowIfCancellationRequested();

        if (message.DeliveryTag is not string deliveryTag)
        {
            throw new ArgumentException(
                "Invalid delivery tag type, expected string",
                nameof(message)
            );
        }

        var delivery = RequireOwned(message, deliveryTag);

        var (streamKey, messageId) = (delivery.StreamKey, delivery.MessageId);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        await SettleEntryAsync(db, delivery).ConfigureAwait(false);

        ForgetDelivery(delivery);

        _logger.LogDebug(
            "Acknowledged message {MessageId} from stream {Stream}",
            messageId,
            streamKey
        );
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
        cancellationToken.ThrowIfCancellationRequested();

        if (message.DeliveryTag is not string deliveryTag)
        {
            throw new ArgumentException(
                "Invalid delivery tag type, expected string",
                nameof(message)
            );
        }

        var delivery = RequireOwned(message, deliveryTag);

        var (streamKey, messageId) = (delivery.StreamKey, delivery.MessageId);
        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);

        if (requeue)
        {
            if (streamKey == GetStreamKey(message.Message.Queue))
            {
                await SettleEntryAsync(db, delivery, CreateStreamEntry(message.Message))
                    .ConfigureAwait(false);
            }
            else
            {
                // Redis Cluster can place the destination in another slot. Publish first so
                // an interruption before settlement leaves a duplicate rather than losing work.
                await EnsurePendingOwnerAsync(db, delivery).ConfigureAwait(false);
                await PublishCoreAsync(message.Message, cancellationToken).ConfigureAwait(false);
                await SettleEntryAsync(db, delivery).ConfigureAwait(false);
            }
            ForgetDelivery(delivery);

            _logger.LogDebug(
                "Rejected and requeued message {MessageId} from stream {Stream}",
                messageId,
                streamKey
            );
        }
        else
        {
            // Acknowledge to remove from pending list (message is lost)
            await SettleEntryAsync(db, delivery).ConfigureAwait(false);
            ForgetDelivery(delivery);

            _logger.LogDebug(
                "Rejected and removed message {MessageId} from stream {Stream}",
                messageId,
                streamKey
            );
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
            var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
            await db.PingAsync().ConfigureAwait(false);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Redis health check failed");
            return false;
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// A message whose consumer stopped is reclaimed and delivered again once its claim
    /// expires, so redelivery follows <c>RedisBrokerOptions.ClaimTimeout</c>.
    /// </remarks>
    public MessageBrokerCapabilities Capabilities { get; } =
        new() { RedeliveryOnConsumerStop = true, RejectsStaleSettlements = true };

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        if (_connection is not null)
        {
            await _connection.CloseAsync().ConfigureAwait(false);
            _connection.Dispose();
        }

        _connectionLock.Dispose();

        _logger.LogInformation("Redis broker disposed");
    }

    private async Task ReadStreamMessagesAsync(
        IDatabase db,
        string[] streamKeys,
        ChannelWriter<BrokerMessage> writer,
        ConsumeSession session,
        CancellationToken cancellationToken
    )
    {
        var failureDelay = TimeSpan.Zero;
        var nextPendingCheck = 0L;
        var recreateGroups = false;

        try
        {
            // Keep reading through transient errors: a consumer that stops here would leave the
            // worker running without receiving messages.
            while (!cancellationToken.IsCancellationRequested && !_disposed)
            {
                var received = 0;

                try
                {
                    if (recreateGroups)
                    {
                        foreach (var streamKey in streamKeys)
                        {
                            await EnsureConsumerGroupAsync(db, streamKey).ConfigureAwait(false);
                        }

                        recreateGroups = false;
                    }

                    // Reclaim messages left pending by consumers that stopped without acknowledging
                    if (Environment.TickCount64 >= nextPendingCheck)
                    {
                        received += await ProcessPendingMessagesAsync(
                                db,
                                streamKeys,
                                writer,
                                session,
                                cancellationToken
                            )
                            .ConfigureAwait(false);
                        nextPendingCheck =
                            Environment.TickCount64
                            + (long)_options.PendingCheckInterval.TotalMilliseconds;
                    }

                    foreach (var streamKey in streamKeys)
                    {
                        received += await ReadNewMessagesAsync(
                                db,
                                streamKey,
                                writer,
                                session,
                                cancellationToken
                            )
                            .ConfigureAwait(false);
                    }

                    failureDelay = TimeSpan.Zero;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (RedisServerException ex)
                    when (_options.AutoCreateStreams && IsMissingGroupError(ex))
                {
                    // The stream or its consumer group was removed (for example, by FLUSHDB)
                    _logger.LogWarning(
                        ex,
                        "Consumer group {Group} is missing, recreating it",
                        _options.ConsumerGroupName
                    );
                    ForgetConsumerGroups(streamKeys);
                    recreateGroups = true;
                    failureDelay = NextFailureDelay(failureDelay);
                }
                catch (Exception ex)
                {
                    failureDelay = NextFailureDelay(failureDelay);
                    _logger.LogError(
                        ex,
                        "Error reading from Redis streams, retrying in {Delay}",
                        failureDelay
                    );
                }

                // Poll again immediately while messages are flowing
                var delay =
                    failureDelay > TimeSpan.Zero ? failureDelay
                    : received == 0 ? _options.BlockTimeout
                    : TimeSpan.Zero;

                if (delay > TimeSpan.Zero)
                {
                    try
                    {
                        await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }
        }
        finally
        {
            writer.Complete();
        }
    }

    private async Task<int> ReadNewMessagesAsync(
        IDatabase db,
        string streamKey,
        ChannelWriter<BrokerMessage> writer,
        ConsumeSession session,
        CancellationToken cancellationToken
    )
    {
        // Use ">" to read only new messages in consumer group
        var entries = await db.StreamReadGroupAsync(
                streamKey,
                _options.ConsumerGroupName,
                session.ConsumerName,
                ">", // Only new messages
                _options.PrefetchCount,
                noAck: false
            )
            .ConfigureAwait(false);

        if (entries is null || entries.Length == 0)
        {
            return 0;
        }

        var batch = new List<RedisDelivery>(entries.Length);
        foreach (var entry in entries)
        {
            var brokerMessage = ParseStreamEntry(streamKey, entry, session);
            if (brokerMessage is not null)
            {
                batch.Add(brokerMessage);
            }
            else
            {
                await QuarantineEntryAsync(db, streamKey, entry.Id, session.ConsumerName)
                    .ConfigureAwait(false);
            }
        }

        await WriteBatchAsync(db, writer, batch, session, cancellationToken).ConfigureAwait(false);
        return entries.Length;
    }

    /// <summary>
    /// Hands a batch read from Redis to the consumer. If consumption stops part-way, the
    /// messages that were not handed over are returned to their streams.
    /// </summary>
    /// <summary>
    /// Returns messages that were read from Redis but never handed to the consumer.
    /// Without this they stay pending for this consumer until another consumer reclaims
    /// them after <see cref="RedisBrokerOptions.ClaimTimeout"/>.
    /// </summary>
    private async Task ReturnUndeliveredAsync(List<BrokerMessage> messages)
    {
        foreach (var message in messages)
        {
            try
            {
                await RejectAsync(message, requeue: true, CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to return undelivered message {MessageId}; it will be reclaimed after the claim timeout",
                    message.Message.Id
                );
            }
        }
    }

    private static bool IsMissingGroupError(RedisServerException ex) =>
        ex.Message.StartsWith("NOGROUP", StringComparison.Ordinal);

    private static TimeSpan NextFailureDelay(TimeSpan current)
    {
        if (current <= TimeSpan.Zero)
        {
            return MinFailureDelay;
        }

        var next = current * 2;
        return next > MaxFailureDelay ? MaxFailureDelay : next;
    }
}
