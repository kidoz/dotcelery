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
public sealed class RedisBroker : IMessageBroker
{
    private readonly RedisBrokerOptions _options;
    private readonly ILogger<RedisBroker> _logger;
    private readonly IMessageSecurityValidator? _messageSecurityValidator;
    private readonly string _consumerName;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly HashSet<string> _initializedGroups = [];

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
    public RedisBroker(
        IOptions<RedisBrokerOptions> options,
        ILogger<RedisBroker> logger,
        IMessageSecurityValidator? messageSecurityValidator = null
    )
    {
        _options = options.Value;
        _logger = logger;
        _messageSecurityValidator = messageSecurityValidator;
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

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var streamKey = GetStreamKey(message.Queue);

        // Serialize the message to JSON using AOT-friendly type info
        var payload = JsonSerializer.Serialize(message, TaskMessageTypeInfo);
        var signature = _messageSecurityValidator?.Sign(Encoding.UTF8.GetBytes(payload));

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

        // Start background task to read from streams
        var readTask = ReadStreamMessagesAsync(
            db,
            streamKeys,
            messageChannel.Writer,
            readCts.Token
        );

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
        ArgumentNullException.ThrowIfNull(message);

        if (message.DeliveryTag is not string deliveryTag)
        {
            throw new ArgumentException(
                "Invalid delivery tag type, expected string",
                nameof(message)
            );
        }

        var (streamKey, messageId) = ParseDeliveryTag(deliveryTag);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        await db.StreamAcknowledgeAsync(streamKey, _options.ConsumerGroupName, messageId)
            .ConfigureAwait(false);

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

        if (message.DeliveryTag is not string deliveryTag)
        {
            throw new ArgumentException(
                "Invalid delivery tag type, expected string",
                nameof(message)
            );
        }

        var (streamKey, messageId) = ParseDeliveryTag(deliveryTag);
        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);

        if (requeue)
        {
            // Add the copy before acknowledging the original: a failure in between leaves a
            // duplicate delivery instead of losing the message.
            await PublishAsync(message.Message, cancellationToken).ConfigureAwait(false);
            await db.StreamAcknowledgeAsync(streamKey, _options.ConsumerGroupName, messageId)
                .ConfigureAwait(false);
            _logger.LogDebug(
                "Rejected and requeued message {MessageId} from stream {Stream}",
                messageId,
                streamKey
            );
        }
        else
        {
            // Acknowledge to remove from pending list (message is lost)
            await db.StreamAcknowledgeAsync(streamKey, _options.ConsumerGroupName, messageId)
                .ConfigureAwait(false);

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
        CancellationToken cancellationToken
    )
    {
        // Use ">" to read only new messages in consumer group
        var entries = await db.StreamReadGroupAsync(
                streamKey,
                _options.ConsumerGroupName,
                _consumerName,
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
                await db.StreamAcknowledgeAsync(streamKey, _options.ConsumerGroupName, entry.Id)
                    .ConfigureAwait(false);
            }
        }

        await WriteBatchAsync(writer, batch, cancellationToken).ConfigureAwait(false);
        return entries.Length;
    }

    /// <summary>
    /// Hands a batch read from Redis to the consumer. If consumption stops part-way, the
    /// messages that were not handed over are returned to their streams.
    /// </summary>
    private async Task WriteBatchAsync(
        ChannelWriter<BrokerMessage> writer,
        List<BrokerMessage> batch,
        CancellationToken cancellationToken
    )
    {
        for (var i = 0; i < batch.Count; i++)
        {
            try
            {
                await writer.WriteAsync(batch[i], cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                await ReturnUndeliveredAsync(batch[i..]).ConfigureAwait(false);
                throw;
            }
        }
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
                        _consumerName,
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
                        await db.StreamAcknowledgeAsync(
                                streamKey,
                                _options.ConsumerGroupName,
                                entry.Id
                            )
                            .ConfigureAwait(false);
                    }
                }

                await WriteBatchAsync(writer, batch, cancellationToken).ConfigureAwait(false);
                reclaimed += batch.Count;
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
            var payloadText = payload.ToString();
            var payloadBytes = Encoding.UTF8.GetBytes(payloadText);
            if (!IsSignatureValid(payloadBytes, signatureText))
            {
                _logger.LogWarning(
                    "Stream entry {EntryId} failed message signature validation",
                    entry.Id
                );
                return null;
            }

            // Deserialize using AOT-friendly type info
            var taskMessage = JsonSerializer.Deserialize(payloadText, TaskMessageTypeInfo);

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

    private bool IsSignatureValid(byte[] payload, string? signature)
    {
        if (_messageSecurityValidator is null)
        {
            return true;
        }

        var validation = _messageSecurityValidator.Validate(
            new TaskMessage
            {
                Id = "unknown",
                Task = "unknown",
                Args = [],
                ContentType = "application/json",
                Timestamp = DateTimeOffset.UtcNow,
            },
            signature
        );

        if (validation.ErrorCode == MessageValidationError.MissingSignature)
        {
            return false;
        }

        return string.IsNullOrEmpty(signature)
            || _messageSecurityValidator.VerifySignature(payload, signature);
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

            _logger.LogInformation(
                "Connected to Redis at {ConnectionString}",
                _options.ConnectionString
            );

            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private string GetStreamKey(string queue) => $"{_options.StreamKeyPrefix}{queue}";

    private static string CreateDeliveryTag(string streamKey, RedisValue messageId) =>
        $"{streamKey}:{messageId}";

    private static (string StreamKey, string MessageId) ParseDeliveryTag(string deliveryTag)
    {
        var lastColonIndex = deliveryTag.LastIndexOf(':');
        if (lastColonIndex < 0)
        {
            throw new ArgumentException(
                $"Invalid delivery tag format: {deliveryTag}",
                nameof(deliveryTag)
            );
        }

        // Find the second-to-last colon (stream key can contain colons)
        var messageIdPart = deliveryTag[(lastColonIndex + 1)..];

        // If messageId looks like a stream ID (contains a dash), the last colon was correct
        // Otherwise, we need to find the stream:messageId boundary differently
        // Stream IDs are in format: timestamp-sequence (e.g., "1234567890123-0")
        if (messageIdPart.Contains('-'))
        {
            var streamKey = deliveryTag[..lastColonIndex];
            return (streamKey, messageIdPart);
        }

        // Handle edge case where stream key might end with the full message ID
        // Search for pattern like ":1234567890123-0" at the end
        for (var i = deliveryTag.Length - 1; i >= 0; i--)
        {
            if (deliveryTag[i] == ':')
            {
                var potentialMessageId = deliveryTag[(i + 1)..];
                if (potentialMessageId.Contains('-'))
                {
                    return (deliveryTag[..i], potentialMessageId);
                }
            }
        }

        throw new ArgumentException(
            $"Could not parse delivery tag: {deliveryTag}",
            nameof(deliveryTag)
        );
    }
}
