using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Backend.Redis.Serialization;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.DelayedMessageStore;

/// <summary>
/// Redis implementation of delayed message store using sorted sets.
/// Messages are stored with their delivery time as the score for efficient retrieval.
/// </summary>
public sealed class RedisDelayedMessageStore : IDelayedMessageStore
{
    private readonly RedisDelayedMessageStoreOptions _options;
    private readonly ILogger<RedisDelayedMessageStore> _logger;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    // AOT-friendly type info
    private static JsonTypeInfo<TaskMessage> TaskMessageTypeInfo =>
        RedisBackendJsonContext.Default.TaskMessage;

    private ConnectionMultiplexer? _connection;
    private bool _disposed;

    // Lua script for atomic get and remove of due messages
    private const string GetDueMessagesScript = """
        local key = KEYS[1]
        local maxScore = ARGV[1]
        local limit = ARGV[2]

        local results = redis.call('ZRANGEBYSCORE', key, '-inf', maxScore, 'LIMIT', 0, limit)

        if #results > 0 then
            redis.call('ZREM', key, unpack(results))
        end

        return results
        """;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisDelayedMessageStore"/> class.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <param name="logger">The logger.</param>
    public RedisDelayedMessageStore(
        IOptions<RedisDelayedMessageStoreOptions> options,
        ILogger<RedisDelayedMessageStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async ValueTask AddAsync(
        TaskMessage message,
        DateTimeOffset deliveryTime,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(message);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);

        var json = JsonSerializer.Serialize(message, TaskMessageTypeInfo);
        var score = deliveryTime.ToUnixTimeMilliseconds();

        // Check if there's an existing entry for this task ID
        // If so, we need to remove the old sorted set entry to prevent duplicates
        var existingJson = await db.HashGetAsync(_options.TaskIdMappingKey, message.Id)
            .ConfigureAwait(false);

        // Store message in sorted set with delivery time as score
        // Also store a mapping from taskId to the JSON for removal
        var transaction = db.CreateTransaction();

#pragma warning disable CA2012 // Use ValueTasks correctly - Redis transaction requires this pattern
        // Remove old entry if it exists (different JSON = different sorted set member)
        if (!existingJson.IsNullOrEmpty)
        {
            _ = transaction.SortedSetRemoveAsync(
                _options.DelayedMessagesKey,
                (string)existingJson!
            );
        }

        _ = transaction.SortedSetAddAsync(_options.DelayedMessagesKey, json, score);
        _ = transaction.HashSetAsync(_options.TaskIdMappingKey, message.Id, json);
#pragma warning restore CA2012

        await transaction.ExecuteAsync().ConfigureAwait(false);

        _logger.LogDebug(
            "Added delayed message {TaskId} for delivery at {DeliveryTime}",
            message.Id,
            deliveryTime
        );
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<TaskMessage> GetDueMessagesAsync(
        DateTimeOffset now,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var maxScore = now.ToUnixTimeMilliseconds();

        // Use Lua script for atomic get and remove
        var results = await db.ScriptEvaluateAsync(
                GetDueMessagesScript,
                [_options.DelayedMessagesKey],
                [maxScore, _options.BatchSize]
            )
            .ConfigureAwait(false);

        if (results.IsNull)
        {
            yield break;
        }

        foreach (var item in (RedisValue[])results!)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (item.IsNullOrEmpty)
            {
                continue;
            }

            var json = (string)item!;
            var message = JsonSerializer.Deserialize(json, TaskMessageTypeInfo);

            if (message is null)
            {
                _logger.LogWarning("Failed to deserialize delayed message: {Json}", json);
                continue;
            }

            // Remove from taskId mapping
            await db.HashDeleteAsync(_options.TaskIdMappingKey, message.Id).ConfigureAwait(false);

            _logger.LogDebug("Retrieved due message {TaskId}", message.Id);
            yield return message;
        }
    }

    /// <inheritdoc />
    public async ValueTask<bool> RemoveAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);

        // Get the JSON from the taskId mapping
        var json = await db.HashGetAsync(_options.TaskIdMappingKey, taskId).ConfigureAwait(false);

        if (json.IsNullOrEmpty)
        {
            return false;
        }

        // Remove from both sorted set and mapping
        var transaction = db.CreateTransaction();

#pragma warning disable CA2012 // Use ValueTasks correctly - Redis transaction requires this pattern
        _ = transaction.SortedSetRemoveAsync(_options.DelayedMessagesKey, (string)json!);
        _ = transaction.HashDeleteAsync(_options.TaskIdMappingKey, taskId);
#pragma warning restore CA2012

        await transaction.ExecuteAsync().ConfigureAwait(false);

        _logger.LogDebug("Removed delayed message {TaskId}", taskId);
        return true;
    }

    /// <inheritdoc />
    public async ValueTask<long> GetPendingCountAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        return await db.SortedSetLengthAsync(_options.DelayedMessagesKey).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<DateTimeOffset?> GetNextDeliveryTimeAsync(
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);

        // Get the first element (lowest score = earliest delivery time)
        var results = await db.SortedSetRangeByRankWithScoresAsync(
                _options.DelayedMessagesKey,
                0,
                0
            )
            .ConfigureAwait(false);

        if (results.Length == 0)
        {
            return null;
        }

        var score = results[0].Score;
        return DateTimeOffset.FromUnixTimeMilliseconds((long)score);
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

        _logger.LogInformation("Redis delayed message store disposed");
    }

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
                "Connected to Redis for delayed message store at {ConnectionString}",
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
