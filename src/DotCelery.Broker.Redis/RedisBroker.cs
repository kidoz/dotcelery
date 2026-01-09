using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Channels;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
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
    private readonly string _consumerName;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly HashSet<string> _initializedGroups = [];

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
    public RedisBroker(IOptions<RedisBrokerOptions> options, ILogger<RedisBroker> logger)
    {
        _options = options.Value;
        _logger = logger;
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

        // Validate message size
        if (_options.MaxMessageSizeBytes > 0 && payload.Length > _options.MaxMessageSizeBytes)
        {
            throw new InvalidOperationException(
                $"Message size ({payload.Length} bytes) exceeds maximum allowed size ({_options.MaxMessageSizeBytes} bytes)."
            );
        }

        // Build stream entry
        var entries = new NameValueEntry[]
        {
            new("payload", payload),
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
                await EnsureConsumerGroupAsync(db, queue, cancellationToken).ConfigureAwait(false);
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

        // Start background task to read from streams
        var readTask = ReadStreamMessagesAsync(
            db,
            streamKeys,
            messageChannel.Writer,
            cancellationToken
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
            // Leave in pending list - another consumer will claim it after ClaimTimeout
            _logger.LogDebug(
                "Message {MessageId} left in pending list for reclaim from stream {Stream}",
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
        try
        {
            while (!cancellationToken.IsCancellationRequested && !_disposed)
            {
                // First, check for pending messages (recovery)
                await ProcessPendingMessagesAsync(db, streamKeys, writer, cancellationToken)
                    .ConfigureAwait(false);

                // Read new messages from each stream
                // Use ">" to read only new messages in consumer group
                foreach (var streamKey in streamKeys)
                {
                    var entries = await db.StreamReadGroupAsync(
                            streamKey,
                            _options.ConsumerGroupName,
                            _consumerName,
                            ">", // Only new messages
                            _options.PrefetchCount,
                            noAck: false
                        )
                        .ConfigureAwait(false);

                    if (entries is not null && entries.Length > 0)
                    {
                        foreach (var entry in entries)
                        {
                            var brokerMessage = ParseStreamEntry(streamKey, entry);
                            if (brokerMessage is not null)
                            {
                                await writer
                                    .WriteAsync(brokerMessage, cancellationToken)
                                    .ConfigureAwait(false);
                            }
                        }
                    }
                }

                // Small delay between polling iterations to avoid busy-waiting
                try
                {
                    await Task.Delay(_options.BlockTimeout, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error reading from Redis streams");
        }
        finally
        {
            writer.Complete();
        }
    }

    private async Task ProcessPendingMessagesAsync(
        IDatabase db,
        string[] streamKeys,
        ChannelWriter<BrokerMessage> writer,
        CancellationToken cancellationToken
    )
    {
        var minIdleTime = (long)_options.ClaimTimeout.TotalMilliseconds;

        foreach (var streamKey in streamKeys)
        {
            try
            {
                // Get pending entries for this consumer group
                var pending = await db.StreamPendingMessagesAsync(
                        streamKey,
                        _options.ConsumerGroupName,
                        _options.PrefetchCount,
                        _consumerName
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

                        await writer
                            .WriteAsync(brokerMessage, cancellationToken)
                            .ConfigureAwait(false);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Error processing pending messages for stream {Stream}",
                    streamKey
                );
            }
        }
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

            // Deserialize using AOT-friendly type info
            var taskMessage = JsonSerializer.Deserialize(payload.ToString(), TaskMessageTypeInfo);

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
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error parsing stream entry {EntryId}", entry.Id);
            return null;
        }
    }

    private async Task EnsureConsumerGroupAsync(
        IDatabase db,
        string queue,
        CancellationToken cancellationToken
    )
    {
        var streamKey = GetStreamKey(queue);

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

    private async Task<IDatabase> GetDatabaseAsync(CancellationToken cancellationToken)
    {
        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        return connection.GetDatabase(_options.Database);
    }

    private async Task<ConnectionMultiplexer> GetConnectionAsync(
        CancellationToken cancellationToken
    )
    {
        if (_connection?.IsConnected == true)
        {
            return _connection;
        }

        await _connectionLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_connection?.IsConnected == true)
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
