using DotCelery.Core.Partitioning;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.Partitioning;

/// <summary>
/// Redis implementation of <see cref="IPartitionLockStore"/>.
/// Uses Redis strings with expiry for distributed locks.
/// </summary>
public sealed class RedisPartitionLockStore : IPartitionLockStore
{
    private readonly RedisPartitionLockStoreOptions _options;
    private readonly ILogger<RedisPartitionLockStore> _logger;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    private ConnectionMultiplexer? _connection;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisPartitionLockStore"/> class.
    /// </summary>
    public RedisPartitionLockStore(
        IOptions<RedisPartitionLockStoreOptions> options,
        ILogger<RedisPartitionLockStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async ValueTask<bool> TryAcquireAsync(
        string partitionKey,
        string taskId,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(partitionKey);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var key = GetLockKey(partitionKey);

        // Try to set the lock with NX (only if not exists) and expiry
        var acquired = await db.StringSetAsync(key, taskId, timeout, When.NotExists)
            .ConfigureAwait(false);

        if (acquired)
        {
            _logger.LogDebug(
                "Acquired partition lock {PartitionKey} for task {TaskId}",
                partitionKey,
                taskId
            );
            return true;
        }

        // Check if we already hold the lock (idempotent behavior)
        var currentHolder = await db.StringGetAsync(key).ConfigureAwait(false);
        if (currentHolder == taskId)
        {
            // Extend the lock
            await db.KeyExpireAsync(key, timeout).ConfigureAwait(false);
            return true;
        }

        return false;
    }

    /// <inheritdoc />
    public async ValueTask<bool> ReleaseAsync(
        string partitionKey,
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(partitionKey);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var key = GetLockKey(partitionKey);

        // Use Lua script for atomic check-and-delete
        const string script = """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            else
                return 0
            end
            """;

        var result = await db.ScriptEvaluateAsync(script, [key], [taskId]).ConfigureAwait(false);

        var released = (int)result == 1;
        if (released)
        {
            _logger.LogDebug(
                "Released partition lock {PartitionKey} for task {TaskId}",
                partitionKey,
                taskId
            );
        }

        return released;
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsLockedAsync(
        string partitionKey,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(partitionKey);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var key = GetLockKey(partitionKey);

        return await db.KeyExistsAsync(key).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetLockHolderAsync(
        string partitionKey,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(partitionKey);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var key = GetLockKey(partitionKey);

        var holder = await db.StringGetAsync(key).ConfigureAwait(false);
        return holder.IsNullOrEmpty ? null : (string)holder!;
    }

    /// <inheritdoc />
    public async ValueTask<bool> ExtendAsync(
        string partitionKey,
        string taskId,
        TimeSpan extension,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(partitionKey);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var key = GetLockKey(partitionKey);

        // Use Lua script for atomic check-and-extend
        const string script = """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('pexpire', KEYS[1], ARGV[2])
            else
                return 0
            end
            """;

        var result = await db.ScriptEvaluateAsync(
                script,
                [key],
                [taskId, (long)extension.TotalMilliseconds]
            )
            .ConfigureAwait(false);

        return (int)result == 1;
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
        _logger.LogInformation("Redis partition lock store disposed");
    }

    private string GetLockKey(string partitionKey) => $"{_options.LockKeyPrefix}{partitionKey}";

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
            _logger.LogInformation("Connected to Redis for partition lock store");

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
