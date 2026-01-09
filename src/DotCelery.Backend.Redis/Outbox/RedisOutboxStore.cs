using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Backend.Redis.Serialization;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Outbox;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.Outbox;

/// <summary>
/// Redis implementation of <see cref="IOutboxStore"/>.
/// Uses a combination of hash storage and sorted sets for efficient message management.
/// </summary>
public sealed class RedisOutboxStore : IOutboxStore
{
    private readonly RedisOutboxStoreOptions _options;
    private readonly ILogger<RedisOutboxStore> _logger;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    // AOT-friendly type info
    private static JsonTypeInfo<OutboxMessage> OutboxMessageTypeInfo =>
        RedisBackendJsonContext.Default.OutboxMessage;

    private ConnectionMultiplexer? _connection;
    private long _sequenceNumber;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisOutboxStore"/> class.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <param name="logger">The logger.</param>
    public RedisOutboxStore(
        IOptions<RedisOutboxStoreOptions> options,
        ILogger<RedisOutboxStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async ValueTask StoreAsync(
        OutboxMessage message,
        object? transaction = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(message);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);

        var sequenceNumber = Interlocked.Increment(ref _sequenceNumber);
        var storedMessage = message with { SequenceNumber = sequenceNumber };
        var json = JsonSerializer.Serialize(storedMessage, OutboxMessageTypeInfo);
        var key = GetMessageKey(message.Id);

        var tx = db.CreateTransaction();

#pragma warning disable CA2012 // Use ValueTasks correctly - Redis transaction requires this pattern
        _ = tx.StringSetAsync(key, json);
        _ = tx.SortedSetAddAsync(_options.PendingSetKey, message.Id, sequenceNumber);
#pragma warning restore CA2012

        await tx.ExecuteAsync().ConfigureAwait(false);

        _logger.LogDebug("Stored outbox message {MessageId}", message.Id);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<OutboxMessage> GetPendingAsync(
        int limit = 100,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);

        // Get pending message IDs ordered by sequence number
        var messageIds = await db.SortedSetRangeByRankAsync(_options.PendingSetKey, 0, limit - 1)
            .ConfigureAwait(false);

        foreach (var messageId in messageIds)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                yield break;
            }

            var key = GetMessageKey((string)messageId!);
            var json = await db.StringGetAsync(key).ConfigureAwait(false);

            if (json.IsNullOrEmpty)
            {
                // Message was deleted, remove from pending set
                await db.SortedSetRemoveAsync(_options.PendingSetKey, messageId)
                    .ConfigureAwait(false);
                continue;
            }

            var message = JsonSerializer.Deserialize((string)json!, OutboxMessageTypeInfo);
            if (message is not null && message.Status == OutboxMessageStatus.Pending)
            {
                yield return message;
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask MarkDispatchedAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var key = GetMessageKey(messageId);

        var json = await db.StringGetAsync(key).ConfigureAwait(false);
        if (json.IsNullOrEmpty)
        {
            return;
        }

        var message = JsonSerializer.Deserialize((string)json!, OutboxMessageTypeInfo);
        if (message is null)
        {
            return;
        }

        var updatedMessage = message with
        {
            Status = OutboxMessageStatus.Dispatched,
            DispatchedAt = DateTimeOffset.UtcNow,
        };

        var updatedJson = JsonSerializer.Serialize(updatedMessage, OutboxMessageTypeInfo);

        var tx = db.CreateTransaction();

#pragma warning disable CA2012 // Use ValueTasks correctly - Redis transaction requires this pattern
        _ = tx.StringSetAsync(key, updatedJson, _options.DispatchedMessageTtl);
        _ = tx.SortedSetRemoveAsync(_options.PendingSetKey, messageId);
#pragma warning restore CA2012

        await tx.ExecuteAsync().ConfigureAwait(false);

        _logger.LogDebug("Marked outbox message {MessageId} as dispatched", messageId);
    }

    /// <inheritdoc />
    public async ValueTask MarkFailedAsync(
        string messageId,
        string errorMessage,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var key = GetMessageKey(messageId);

        var json = await db.StringGetAsync(key).ConfigureAwait(false);
        if (json.IsNullOrEmpty)
        {
            return;
        }

        var message = JsonSerializer.Deserialize((string)json!, OutboxMessageTypeInfo);
        if (message is null)
        {
            return;
        }

        var newAttempts = message.Attempts + 1;
        var newStatus = newAttempts >= 5 ? OutboxMessageStatus.Failed : message.Status;

        var updatedMessage = message with
        {
            Status = newStatus,
            Attempts = newAttempts,
            LastError = errorMessage,
        };

        var updatedJson = JsonSerializer.Serialize(updatedMessage, OutboxMessageTypeInfo);
        await db.StringSetAsync(key, updatedJson).ConfigureAwait(false);

        // Remove from pending set if failed permanently
        if (newStatus == OutboxMessageStatus.Failed)
        {
            await db.SortedSetRemoveAsync(_options.PendingSetKey, messageId).ConfigureAwait(false);
        }

        _logger.LogDebug(
            "Marked outbox message {MessageId} as failed (attempt {Attempt})",
            messageId,
            newAttempts
        );
    }

    /// <inheritdoc />
    public async ValueTask<long> GetPendingCountAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        return await db.SortedSetLengthAsync(_options.PendingSetKey).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<long> CleanupAsync(
        TimeSpan olderThan,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Redis handles TTL-based expiration automatically for dispatched messages
        // This method can be used for manual cleanup of failed messages

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var cutoff = DateTimeOffset.UtcNow - olderThan;
        long removed = 0;

        // Get all pending message IDs to check for stale entries
        var messageIds = await db.SortedSetRangeByRankAsync(_options.PendingSetKey)
            .ConfigureAwait(false);

        foreach (var messageId in messageIds)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var key = GetMessageKey((string)messageId!);
            var json = await db.StringGetAsync(key).ConfigureAwait(false);

            if (json.IsNullOrEmpty)
            {
                // Orphaned entry in sorted set
                await db.SortedSetRemoveAsync(_options.PendingSetKey, messageId)
                    .ConfigureAwait(false);
                removed++;
                continue;
            }

            var message = JsonSerializer.Deserialize((string)json!, OutboxMessageTypeInfo);
            if (message is null)
            {
                continue;
            }

            // Remove old failed messages
            if (message.Status == OutboxMessageStatus.Failed && message.CreatedAt < cutoff)
            {
                await db.KeyDeleteAsync(key).ConfigureAwait(false);
                await db.SortedSetRemoveAsync(_options.PendingSetKey, messageId)
                    .ConfigureAwait(false);
                removed++;
            }
        }

        _logger.LogDebug("Cleaned up {Count} outbox messages", removed);
        return removed;
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

        _logger.LogInformation("Redis outbox store disposed");
    }

    private string GetMessageKey(string messageId) => $"{_options.MessageKeyPrefix}{messageId}";

    private async Task<IConnectionMultiplexer> GetConnectionAsync(
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
                "Connected to Redis for outbox store at {ConnectionString}",
                _options.ConnectionString
            );

            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private async Task<IDatabase> GetDatabaseAsync(CancellationToken cancellationToken)
    {
        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        return connection.GetDatabase(_options.Database);
    }
}
