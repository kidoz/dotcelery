using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Backend.Redis.Serialization;
using DotCelery.Core.Abstractions;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.DeadLetter;

/// <summary>
/// Redis implementation of <see cref="IDeadLetterStore"/>.
/// Uses a sorted set for message storage and a hash for message data.
/// </summary>
public sealed class RedisDeadLetterStore : IDeadLetterStore
{
    private readonly RedisDeadLetterStoreOptions _options;
    private readonly IMessageBroker _broker;
    private readonly IMessageSerializer _serializer;
    private readonly ILogger<RedisDeadLetterStore> _logger;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    // AOT-friendly type info
    private static JsonTypeInfo<DeadLetterMessage> DeadLetterMessageTypeInfo =>
        RedisBackendJsonContext.Default.DeadLetterMessage;

    private ConnectionMultiplexer? _connection;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisDeadLetterStore"/> class.
    /// </summary>
    public RedisDeadLetterStore(
        IMessageBroker broker,
        IMessageSerializer serializer,
        IOptions<RedisDeadLetterStoreOptions> options,
        ILogger<RedisDeadLetterStore> logger
    )
    {
        _broker = broker;
        _serializer = serializer;
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async ValueTask StoreAsync(
        DeadLetterMessage message,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var json = JsonSerializer.Serialize(message, DeadLetterMessageTypeInfo);

        // Store in sorted set (score = timestamp) and hash
        var transaction = db.CreateTransaction();
        _ = transaction.SortedSetAddAsync(
            _options.IndexKey,
            message.Id,
            message.Timestamp.ToUnixTimeMilliseconds()
        );
        _ = transaction.HashSetAsync(_options.DataKey, message.Id, json);

        // Set expiry if configured
        if (message.ExpiresAt.HasValue)
        {
            var expiry = message.ExpiresAt.Value - DateTimeOffset.UtcNow;
            if (expiry > TimeSpan.Zero)
            {
                _ = transaction.KeyExpireAsync(_options.DataKey, expiry, ExpireWhen.HasNoExpiry);
            }
        }

        await transaction.ExecuteAsync().ConfigureAwait(false);

        // Enforce max messages limit
        await EnforceMaxMessagesAsync(db).ConfigureAwait(false);

        _logger.LogDebug("Stored dead letter message {MessageId}", message.Id);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<DeadLetterMessage> GetAllAsync(
        int limit = 100,
        int offset = 0,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);

        // Get message IDs from sorted set (newest first)
        var ids = await db.SortedSetRangeByRankAsync(
                _options.IndexKey,
                -offset - limit,
                -offset - 1,
                Order.Descending
            )
            .ConfigureAwait(false);

        foreach (var id in ids)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var json = await db.HashGetAsync(_options.DataKey, id.ToString()).ConfigureAwait(false);
            if (!json.HasValue)
            {
                continue;
            }

            var message = JsonSerializer.Deserialize((string)json!, DeadLetterMessageTypeInfo);
            if (message is not null)
            {
                yield return message;
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask<DeadLetterMessage?> GetAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var json = await db.HashGetAsync(_options.DataKey, messageId).ConfigureAwait(false);

        if (!json.HasValue)
        {
            return null;
        }

        return JsonSerializer.Deserialize((string)json!, DeadLetterMessageTypeInfo);
    }

    /// <inheritdoc />
    public async ValueTask<bool> RequeueAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var json = await db.HashGetAsync(_options.DataKey, messageId).ConfigureAwait(false);

        if (!json.HasValue)
        {
            return false;
        }

        var message = JsonSerializer.Deserialize((string)json!, DeadLetterMessageTypeInfo);
        if (message is null)
        {
            return false;
        }

        // Deserialize original task message and publish
        var taskMessage = _serializer.Deserialize<TaskMessage>(message.OriginalMessage);
        if (taskMessage is null)
        {
            _logger.LogWarning(
                "Failed to deserialize original message for DLQ entry {MessageId}",
                messageId
            );
            return false;
        }

        await _broker.PublishAsync(taskMessage, cancellationToken).ConfigureAwait(false);

        // Remove from DLQ
        await DeleteAsync(messageId, cancellationToken).ConfigureAwait(false);

        _logger.LogInformation(
            "Requeued dead letter message {MessageId} as task {TaskId}",
            messageId,
            taskMessage.Id
        );
        return true;
    }

    /// <inheritdoc />
    public async ValueTask<bool> DeleteAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);

        var transaction = db.CreateTransaction();
        _ = transaction.SortedSetRemoveAsync(_options.IndexKey, messageId);
        _ = transaction.HashDeleteAsync(_options.DataKey, messageId);

        var success = await transaction.ExecuteAsync().ConfigureAwait(false);
        return success;
    }

    /// <inheritdoc />
    public async ValueTask<long> GetCountAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        return await db.SortedSetLengthAsync(_options.IndexKey).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<long> PurgeAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var count = await db.SortedSetLengthAsync(_options.IndexKey).ConfigureAwait(false);

        var transaction = db.CreateTransaction();
        _ = transaction.KeyDeleteAsync(_options.IndexKey);
        _ = transaction.KeyDeleteAsync(_options.DataKey);

        await transaction.ExecuteAsync().ConfigureAwait(false);

        _logger.LogInformation("Purged {Count} messages from dead letter queue", count);
        return count;
    }

    /// <inheritdoc />
    public async ValueTask<long> CleanupExpiredAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var now = DateTimeOffset.UtcNow;
        long count = 0;

        // Get all message IDs and check expiry
        var ids = await db.HashKeysAsync(_options.DataKey).ConfigureAwait(false);

        foreach (var id in ids)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var json = await db.HashGetAsync(_options.DataKey, id).ConfigureAwait(false);
            if (!json.HasValue)
            {
                continue;
            }

            var message = JsonSerializer.Deserialize((string)json!, DeadLetterMessageTypeInfo);
            if (message?.ExpiresAt is not null && message.ExpiresAt < now)
            {
                var idString = id.ToString();
                if (await DeleteAsync(idString, cancellationToken).ConfigureAwait(false))
                {
                    count++;
                }
            }
        }

        if (count > 0)
        {
            _logger.LogInformation(
                "Cleaned up {Count} expired messages from dead letter queue",
                count
            );
        }

        return count;
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
            await _connection.DisposeAsync().ConfigureAwait(false);
        }

        _connectionLock.Dispose();
    }

    private async ValueTask<IDatabase> GetDatabaseAsync(CancellationToken cancellationToken)
    {
        if (_connection is not null && _connection.IsConnected)
        {
            return _connection.GetDatabase();
        }

        await _connectionLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_connection is not null && _connection.IsConnected)
            {
                return _connection.GetDatabase();
            }

            _connection?.Dispose();
            _connection = await ConnectionMultiplexer
                .ConnectAsync(_options.Configuration)
                .ConfigureAwait(false);

            _logger.LogDebug("Connected to Redis for dead letter store");
            return _connection.GetDatabase();
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private async Task EnforceMaxMessagesAsync(IDatabase db)
    {
        var count = await db.SortedSetLengthAsync(_options.IndexKey).ConfigureAwait(false);
        if (count <= _options.MaxMessages)
        {
            return;
        }

        // Remove oldest messages (lowest scores = oldest timestamps)
        var toRemove = count - _options.MaxMessages;
        var oldestIds = await db.SortedSetRangeByRankAsync(_options.IndexKey, 0, toRemove - 1)
            .ConfigureAwait(false);

        foreach (var id in oldestIds)
        {
            await db.SortedSetRemoveAsync(_options.IndexKey, id).ConfigureAwait(false);
            await db.HashDeleteAsync(_options.DataKey, id.ToString()).ConfigureAwait(false);
        }

        _logger.LogDebug("Removed {Count} old messages from dead letter queue", oldestIds.Length);
    }
}
