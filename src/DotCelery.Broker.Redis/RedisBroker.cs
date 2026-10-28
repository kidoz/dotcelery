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
public sealed class RedisBroker : IMessageBroker, IBrokerCapabilities
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
            _options.ConsumerName ?? $"{Environment.MachineName}-{Environment.ProcessId}";

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

        // Serialize the message to JSON using AOT-friendly type info
        var payload = JsonSerializer.SerializeToUtf8Bytes(message, TaskMessageTypeInfo);
        var signature = _messageSecurityValidator?.Sign(payload);

        // Validate message size
        if (_options.MaxMessageSizeBytes > 0 && payload.Length > _options.MaxMessageSizeBytes)
        {
            throw new InvalidOperationException(
                $"Message size ({payload.Length} bytes) exceeds maximum allowed size ({_options.MaxMessageSizeBytes} bytes)."
            );
        }

        // Build stream entry
        var entries = string.IsNullOrEmpty(signature)
            ? new NameValueEntry[]
            {
                new("payload", payload),
                new(
                    "timestamp",
                    DateTimeOffset
                        .UtcNow.ToUnixTimeMilliseconds()
                        .ToString(System.Globalization.CultureInfo.InvariantCulture)
                ),
            }
            : new NameValueEntry[]
            {
                new("payload", payload),
                new("signature", signature),
                new(
                    "timestamp",
                    DateTimeOffset
                        .UtcNow.ToUnixTimeMilliseconds()
                        .ToString(System.Globalization.CultureInfo.InvariantCulture)
                ),
            };

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
            foreach (var tag in session.Tags)
            {
                _deliveries.TryRemove(tag, out _);
            }
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

        var (streamKey, messageId) = ParseDeliveryTag(deliveryTag);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        await SettleEntryAsync(db, delivery, requeue: false).ConfigureAwait(false);

        _deliveries.TryRemove(deliveryTag, out _);

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

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);

        await SettleEntryAsync(db, delivery, requeue).ConfigureAwait(false);
        _deliveries.TryRemove(deliveryTag, out _);
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

        var batch = new List<BrokerMessage>(entries.Length);
        foreach (var entry in entries)
        {
            var brokerMessage = ParseStreamEntry(streamKey, entry);
            if (brokerMessage is not null)
            {
                batch.Add(brokerMessage);
            }
            else
            {
                await QuarantineEntryAsync(db, streamKey, entry.Id).ConfigureAwait(false);
            }
        }

        await WriteBatchAsync(writer, batch, session, cancellationToken).ConfigureAwait(false);
        return entries.Length;
    }

    /// <summary>
    /// Hands a batch read from Redis to the consumer. If consumption stops part-way, the
    /// messages that were not handed over are returned to their streams.
    /// </summary>
    private async Task WriteBatchAsync(
        ChannelWriter<BrokerMessage> writer,
        List<BrokerMessage> batch,
        ConsumeSession session,
        CancellationToken cancellationToken
    )
    {
        // Track the entire read batch before exposing its first delivery. If enumeration stops
        // after that first delivery, every buffered entry can still be returned immediately.
        var owned = new List<BrokerMessage>(batch.Count);
        foreach (var message in batch)
            if (
                await TrackDeliveryAsync(message, session, CancellationToken.None)
                    .ConfigureAwait(false)
            )
                owned.Add(message);

        for (var i = 0; i < owned.Count; i++)
        {
            try
            {
                await writer.WriteAsync(owned[i], cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                await ReturnUndeliveredAsync(owned[i..]).ConfigureAwait(false);
                throw;
            }
        }
    }

    // A delivery is pending in Redis from the moment the group hands it out until it is settled,
    // and a pending delivery idle for ClaimTimeout is reclaimed, so the consumer keeps its claim
    // alive while the message is being processed. Without this, a task that runs longer than the
    // claim timeout would be handed to another worker and run twice.
    private async Task RenewClaimsAsync(
        IDatabase db,
        ConsumeSession session,
        CancellationToken cancellationToken
    )
    {
        var interval = _options.ClaimTimeout / 3;
        if (interval <= TimeSpan.Zero)
        {
            return;
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            var live = new List<RedisDelivery>();
            foreach (var tag in session.Tags)
            {
                if (_deliveries.TryGetValue(tag, out var delivery))
                {
                    live.Add(delivery);
                }
            }

            if (live.Count == 0)
            {
                continue;
            }

            foreach (var delivery in live)
            {
                try
                {
                    // Check and renew in one operation, so renewal never steals a newer claim.
                    await db.ScriptEvaluateAsync(
                            OwnershipCheckScript
                                + "redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[3], 0, ARGV[2], 'JUSTID'); return 1",
                            [(RedisKey)delivery.StreamKey],
                            DeliveryArguments(delivery)
                        )
                        .ConfigureAwait(false);
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                {
                    _logger.LogDebug(
                        ex,
                        "Failed to renew claim on stream {Stream}",
                        delivery.StreamKey
                    );
                }
            }
        }
    }

    private async Task<bool> TrackDeliveryAsync(
        BrokerMessage message,
        ConsumeSession session,
        CancellationToken cancellationToken
    )
    {
        var deliveryTag = (string)message.DeliveryTag;
        var (streamKey, messageId) = ParseDeliveryTag(deliveryTag);
        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var count = (long)
            await db.ScriptEvaluateAsync(
                    "local p = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1); "
                        + "if #p == 0 or p[1][2] ~= ARGV[3] then return 0 end; return p[1][4]",
                    [(RedisKey)streamKey],
                    [(RedisValue)_options.ConsumerGroupName, messageId, session.ConsumerName]
                )
                .ConfigureAwait(false);
        if (count == 0)
            return false;
        _deliveries[deliveryTag] = new RedisDelivery(
            streamKey,
            messageId,
            session.ConsumerName,
            count
        );
        session.Track(deliveryTag);
        return true;
    }

    // The deliveries one consumption session handed to its caller; claims are renewed for
    // these and only these, so an unsettled delivery of a session that stopped does not keep
    // its claim alive after the session is gone
    private sealed class ConsumeSession(string consumerName)
    {
        public string ConsumerName { get; } = consumerName;

        private readonly ConcurrentDictionary<string, byte> _tags = new(StringComparer.Ordinal);

        public void Track(string deliveryTag) => _tags.TryAdd(deliveryTag, 0);

        public IReadOnlyCollection<string> Tags => [.. _tags.Keys];
    }

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

    private async Task<int> ProcessPendingMessagesAsync(
        IDatabase db,
        string[] streamKeys,
        ChannelWriter<BrokerMessage> writer,
        ConsumeSession session,
        CancellationToken cancellationToken
    )
    {
        var minIdleTime = (long)_options.ClaimTimeout.TotalMilliseconds;
        var reclaimed = 0;

        foreach (var streamKey in streamKeys)
        {
            try
            {
                // Get pending entries for this consumer group, regardless of
                // which consumer originally received them.
                var pending = await db.StreamPendingMessagesAsync(
                        streamKey,
                        _options.ConsumerGroupName,
                        _options.PrefetchCount,
                        RedisValue.Null
                    )
                    .ConfigureAwait(false);

                if (pending is null || pending.Length == 0)
                {
                    continue;
                }

                // Filter messages that have been idle long enough
                var idlePending = pending
                    .Where(p => p.IdleTimeInMilliseconds >= minIdleTime)
                    .Select(p => p.MessageId)
                    .ToArray();

                if (idlePending.Length == 0)
                {
                    continue;
                }

                // Claim the idle messages
                var claimed = await db.StreamClaimAsync(
                        streamKey,
                        _options.ConsumerGroupName,
                        session.ConsumerName,
                        minIdleTime,
                        idlePending
                    )
                    .ConfigureAwait(false);

                var batch = new List<BrokerMessage>(claimed.Length);
                foreach (var entry in claimed)
                {
                    var brokerMessage = ParseStreamEntry(streamKey, entry);
                    if (brokerMessage is not null)
                    {
                        _logger.LogDebug(
                            "Reclaimed pending message {MessageId} from stream {Stream}",
                            entry.Id,
                            streamKey
                        );

                        batch.Add(brokerMessage);
                    }
                    else
                    {
                        await QuarantineEntryAsync(db, streamKey, entry.Id).ConfigureAwait(false);
                    }
                }

                await WriteBatchAsync(writer, batch, session, cancellationToken)
                    .ConfigureAwait(false);
                reclaimed += batch.Count;
                foreach (var previousConsumer in pending.Select(p => p.ConsumerName).Distinct())
                    await RemoveIdleConsumerAsync(db, streamKey, previousConsumer)
                        .ConfigureAwait(false);
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                _logger.LogWarning(
                    ex,
                    "Error processing pending messages for stream {Stream}",
                    streamKey
                );
            }
        }

        return reclaimed;
    }

    private async Task RemoveIdleConsumerAsync(IDatabase db, string streamKey, RedisValue consumer)
    {
        try
        {
            await db.ScriptEvaluateAsync(
                    "local p = redis.call('XPENDING', KEYS[1], ARGV[1], '-', '+', 1, ARGV[2]); "
                        + "if #p == 0 then return redis.call('XGROUP', 'DELCONSUMER', KEYS[1], ARGV[1], ARGV[2]) end; return 0",
                    [(RedisKey)streamKey],
                    [(RedisValue)_options.ConsumerGroupName, consumer]
                )
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not clean up Redis consumer {Consumer}", consumer);
        }
    }

    private RedisDelivery RequireOwned(BrokerMessage message, string deliveryTag)
    {
        if (!_deliveries.TryGetValue(deliveryTag, out var delivery))
        {
            throw new InvalidOperationException(
                $"Message {message.Message.Id} cannot be settled: it was settled already, or its consumer stopped."
            );
        }
        return delivery;
    }

    // Preserve the exact entry before acknowledging it. The Lua script makes quarantine
    // and removal atomic, including a retry after an ambiguous connection failure.
    private Task<RedisResult> QuarantineEntryAsync(
        IDatabase db,
        string streamKey,
        RedisValue entryId
    ) =>
        db.ScriptEvaluateAsync(
            "local entries = redis.call('XRANGE', KEYS[1], ARGV[2], ARGV[2]); "
                + "if #entries == 0 then return 0 end; "
                + "redis.call('XADD', KEYS[2], '*', 'source-stream', KEYS[1], 'source-id', ARGV[2], "
                + "'reason', 'Unreadable or invalid payload', unpack(entries[1][2])); "
                + "redis.call('XACK', KEYS[1], ARGV[1], ARGV[2]); "
                + "return redis.call('XDEL', KEYS[1], ARGV[2])",
            [(RedisKey)streamKey, (RedisKey)(_options.DeadLetterStreamPrefix + streamKey)],
            [(RedisValue)_options.ConsumerGroupName, entryId]
        );

    private const string OwnershipCheckScript =
        "local p = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1); "
        + "if #p == 0 or p[1][2] ~= ARGV[3] or p[1][4] ~= tonumber(ARGV[4]) then return 0 end; ";

    private RedisValue[] DeliveryArguments(RedisDelivery delivery) =>
        [
            _options.ConsumerGroupName,
            delivery.MessageId,
            delivery.ConsumerName,
            delivery.DeliveryCount,
        ];

    private async Task SettleEntryAsync(IDatabase db, RedisDelivery delivery, bool requeue)
    {
        var result = (long)
            await db.ScriptEvaluateAsync(
                    OwnershipCheckScript
                        + (
                            requeue
                                ? "local entries = redis.call('XRANGE', KEYS[1], ARGV[2], ARGV[2]); "
                                    + "if #entries == 0 then return 0 end; redis.call('XADD', KEYS[1], '*', unpack(entries[1][2])); "
                                : ""
                        )
                        + "redis.call('XACK', KEYS[1], ARGV[1], ARGV[2]); redis.call('XDEL', KEYS[1], ARGV[2]); return 1",
                    [(RedisKey)delivery.StreamKey],
                    DeliveryArguments(delivery)
                )
                .ConfigureAwait(false);
        if (result == 0)
            throw new InvalidOperationException(
                "The Redis delivery is no longer owned by this consumer."
            );
    }

    private BrokerMessage? ParseStreamEntry(string streamKey, StreamEntry entry)
    {
        try
        {
            var payload = entry["payload"];
            if (payload.IsNullOrEmpty)
            {
                _logger.LogWarning("Stream entry {EntryId} has no payload, skipping", entry.Id);
                return null;
            }

            var signature = entry["signature"];
            var signatureText = signature.IsNullOrEmpty ? null : signature.ToString();
            var payloadBytes = (byte[])payload!;
            if (
                _options.MaxMessageSizeBytes > 0
                && payloadBytes.Length > _options.MaxMessageSizeBytes
            )
            {
                _logger.LogWarning("Rejecting oversized Redis stream entry {EntryId}", entry.Id);
                return null;
            }
            if (!_messageSecurityValidator.IsSignatureValid(payloadBytes, signatureText))
            {
                _logger.LogWarning(
                    "Stream entry {EntryId} failed message signature validation",
                    entry.Id
                );
                return null;
            }

            // Deserialize using AOT-friendly type info
            var taskMessage = JsonSerializer.Deserialize(payloadBytes, TaskMessageTypeInfo);

            if (taskMessage is null)
            {
                _logger.LogWarning(
                    "Failed to deserialize stream entry {EntryId}, skipping",
                    entry.Id
                );
                return null;
            }

            // Extract queue name from stream key
            var queue = streamKey.StartsWith(_options.StreamKeyPrefix, StringComparison.Ordinal)
                ? streamKey[_options.StreamKeyPrefix.Length..]
                : streamKey;

            return new BrokerMessage
            {
                Message = taskMessage,
                DeliveryTag = CreateDeliveryTag(streamKey, entry.Id),
                Queue = queue,
                ReceivedAt = DateTimeOffset.UtcNow,
                RawBody = payloadBytes,
                Signature = signatureText,
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error parsing stream entry {EntryId}", entry.Id);
            return null;
        }
    }

    private async Task EnsureConsumerGroupAsync(IDatabase db, string streamKey)
    {
        lock (_initializedGroups)
        {
            if (_initializedGroups.Contains(streamKey))
            {
                return;
            }
        }

        try
        {
            // Try to create the consumer group
            // The '0' position means all messages (including historical)
            await db.StreamCreateConsumerGroupAsync(
                    streamKey,
                    _options.ConsumerGroupName,
                    "0",
                    createStream: true
                )
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Created consumer group {Group} for stream {Stream}",
                _options.ConsumerGroupName,
                streamKey
            );
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
        {
            // Group already exists, that's fine
            _logger.LogDebug(
                "Consumer group {Group} already exists for stream {Stream}",
                _options.ConsumerGroupName,
                streamKey
            );
        }

        lock (_initializedGroups)
        {
            _initializedGroups.Add(streamKey);
        }
    }

    private void ForgetConsumerGroups(string[] streamKeys)
    {
        lock (_initializedGroups)
        {
            foreach (var streamKey in streamKeys)
            {
                _initializedGroups.Remove(streamKey);
            }
        }
    }

    private async Task<IDatabase> GetDatabaseAsync(CancellationToken cancellationToken)
    {
        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        return connection.GetDatabase(_options.Database);
    }

    private async Task<ConnectionMultiplexer> GetConnectionAsync(
        CancellationToken cancellationToken
    )
    {
        // The multiplexer reconnects on its own after a connection drops, so it is created once
        // and reused. Replacing it while disconnected would leak connections.
        if (_connection is not null)
        {
            return _connection;
        }

        await _connectionLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_connection is not null)
            {
                return _connection;
            }

            var configOptions = ConfigurationOptions.Parse(_options.ConnectionString);
            configOptions.DefaultDatabase = _options.Database;
            configOptions.ConnectTimeout = (int)_options.ConnectTimeout.TotalMilliseconds;
            configOptions.SyncTimeout = (int)_options.SyncTimeout.TotalMilliseconds;
            configOptions.AbortOnConnectFail = _options.AbortOnConnectFail;

            _connection = await ConnectionMultiplexer
                .ConnectAsync(configOptions)
                .ConfigureAwait(false);

            // The connection string is not logged: it commonly embeds credentials
            _logger.LogInformation("Connected to Redis");

            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private string GetStreamKey(string queue) => $"{_options.StreamKeyPrefix}{queue}";

    /// <summary>
    /// A delivery that is pending in Redis until it is settled.
    /// </summary>
    /// <param name="StreamKey">The stream the entry belongs to.</param>
    /// <param name="MessageId">The stream entry id.</param>
    /// <param name="ConsumerName">The unique consumption session.</param>
    /// <param name="DeliveryCount">The pending delivery generation.</param>
    private readonly record struct RedisDelivery(
        string StreamKey,
        string MessageId,
        string ConsumerName,
        long DeliveryCount
    );

    private static string CreateDeliveryTag(string streamKey, RedisValue messageId) =>
        $"{Guid.NewGuid():N}:{streamKey}:{messageId}";

    private static (string StreamKey, string MessageId) ParseDeliveryTag(string deliveryTag)
    {
        // The tag is "{generation}:{stream key}:{message id}". The stream key can contain colons, but a
        // stream message id ("timestamp-sequence") never does, so the boundary is the last colon.
        var lastColonIndex = deliveryTag.LastIndexOf(':');
        if (lastColonIndex < 0)
        {
            throw new ArgumentException(
                $"Invalid delivery tag format: {deliveryTag}",
                nameof(deliveryTag)
            );
        }

        var messageId = deliveryTag[(lastColonIndex + 1)..];
        if (!messageId.Contains('-'))
        {
            throw new ArgumentException(
                $"Could not parse delivery tag: {deliveryTag}",
                nameof(deliveryTag)
            );
        }

        return (deliveryTag[(deliveryTag.IndexOf(':') + 1)..lastColonIndex], messageId);
    }
}
