using System.Text.Json;
using DotCelery.Backend.Redis.Serialization;
using DotCelery.Core.Execution;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.Execution;

/// <summary>
/// Redis implementation of <see cref="ITaskExecutionTracker"/>.
/// Uses Redis strings with expiry for distributed execution tracking.
/// </summary>
public sealed class RedisTaskExecutionTracker : ITaskExecutionTracker
{
    private readonly RedisTaskExecutionTrackerOptions _options;
    private readonly ILogger<RedisTaskExecutionTracker> _logger;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    // Fallback options for internal ExecutionData type
    private static JsonSerializerOptions JsonOptions => RedisJsonHelper.FallbackOptions;

    private ConnectionMultiplexer? _connection;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisTaskExecutionTracker"/> class.
    /// </summary>
    public RedisTaskExecutionTracker(
        IOptions<RedisTaskExecutionTrackerOptions> options,
        ILogger<RedisTaskExecutionTracker> logger
    )
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async ValueTask<bool> TryStartAsync(
        string taskName,
        string taskId,
        string? key = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskName);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var lockKey = GetLockKey(taskName, key);
        var expiry = timeout ?? _options.DefaultTimeout;
        var now = DateTimeOffset.UtcNow;

        var info = new ExecutionData
        {
            TaskId = taskId,
            Key = key,
            StartedAt = now,
            ExpiresAt = now.Add(expiry),
        };

        var json = JsonSerializer.Serialize(info, JsonOptions);

        // Try to set with NX (only if not exists)
        var acquired = await db.StringSetAsync(lockKey, json, expiry, When.NotExists)
            .ConfigureAwait(false);

        if (acquired)
        {
            // Add to index for enumeration
            await db.HashSetAsync(_options.ExecutionIndexKey, lockKey, json).ConfigureAwait(false);
            _logger.LogDebug(
                "Started execution tracking for {TaskName} task {TaskId}",
                taskName,
                taskId
            );
            return true;
        }

        // Check if we already hold the lock
        var existing = await db.StringGetAsync(lockKey).ConfigureAwait(false);
        if (!existing.IsNullOrEmpty)
        {
            var existingInfo = JsonSerializer.Deserialize<ExecutionData>(
                (string)existing!,
                JsonOptions
            );
            if (existingInfo?.TaskId == taskId)
            {
                // Extend the lock
                info = info with
                {
                    ExpiresAt = now.Add(expiry),
                };
                json = JsonSerializer.Serialize(info, JsonOptions);
                await db.StringSetAsync(lockKey, json, expiry).ConfigureAwait(false);
                await db.HashSetAsync(_options.ExecutionIndexKey, lockKey, json)
                    .ConfigureAwait(false);
                return true;
            }
        }

        return false;
    }

    /// <inheritdoc />
    public async ValueTask StopAsync(
        string taskName,
        string taskId,
        string? key = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskName);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var lockKey = GetLockKey(taskName, key);

        // Use Lua script for atomic check-and-delete
        const string script = """
            local data = redis.call('get', KEYS[1])
            if data then
                local info = cjson.decode(data)
                if info.taskId == ARGV[1] then
                    redis.call('del', KEYS[1])
                    redis.call('hdel', KEYS[2], KEYS[1])
                    return 1
                end
            end
            return 0
            """;

        await db.ScriptEvaluateAsync(script, [lockKey, _options.ExecutionIndexKey], [taskId])
            .ConfigureAwait(false);

        _logger.LogDebug(
            "Stopped execution tracking for {TaskName} task {TaskId}",
            taskName,
            taskId
        );
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsExecutingAsync(
        string taskName,
        string? key = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskName);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var lockKey = GetLockKey(taskName, key);

        return await db.KeyExistsAsync(lockKey).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetExecutingTaskIdAsync(
        string taskName,
        string? key = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskName);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var lockKey = GetLockKey(taskName, key);

        var data = await db.StringGetAsync(lockKey).ConfigureAwait(false);
        if (data.IsNullOrEmpty)
        {
            return null;
        }

        var info = JsonSerializer.Deserialize<ExecutionData>((string)data!, JsonOptions);
        return info?.TaskId;
    }

    /// <inheritdoc />
    public async ValueTask<bool> ExtendAsync(
        string taskName,
        string taskId,
        string? key = null,
        TimeSpan? extension = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskName);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var lockKey = GetLockKey(taskName, key);
        var expiry = extension ?? _options.DefaultTimeout;

        // Get current data
        var data = await db.StringGetAsync(lockKey).ConfigureAwait(false);
        if (data.IsNullOrEmpty)
        {
            return false;
        }

        var info = JsonSerializer.Deserialize<ExecutionData>((string)data!, JsonOptions);
        if (info?.TaskId != taskId)
        {
            return false;
        }

        // Update expiry
        var now = DateTimeOffset.UtcNow;
        var newInfo = info with { ExpiresAt = now.Add(expiry) };
        var json = JsonSerializer.Serialize(newInfo, JsonOptions);

        await db.StringSetAsync(lockKey, json, expiry).ConfigureAwait(false);
        await db.HashSetAsync(_options.ExecutionIndexKey, lockKey, json).ConfigureAwait(false);

        return true;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, ExecutingTaskInfo>> GetAllExecutingAsync(
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var entries = await db.HashGetAllAsync(_options.ExecutionIndexKey).ConfigureAwait(false);

        var result = new Dictionary<string, ExecutingTaskInfo>();
        var now = DateTimeOffset.UtcNow;
        var keysToRemove = new List<RedisValue>();

        foreach (var entry in entries)
        {
            var lockKey = (string)entry.Name!;
            var data = (string)entry.Value!;

            var info = JsonSerializer.Deserialize<ExecutionData>(data, JsonOptions);
            if (info is null)
            {
                continue;
            }

            // Check if still valid
            if (info.ExpiresAt <= now)
            {
                keysToRemove.Add(lockKey);
                continue;
            }

            result[lockKey] = new ExecutingTaskInfo
            {
                TaskId = info.TaskId,
                Key = info.Key,
                StartedAt = info.StartedAt,
                ExpiresAt = info.ExpiresAt,
            };
        }

        // Clean up expired entries from index
        if (keysToRemove.Count > 0)
        {
            await db.HashDeleteAsync(_options.ExecutionIndexKey, [.. keysToRemove])
                .ConfigureAwait(false);
        }

        return result;
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
        _logger.LogInformation("Redis task execution tracker disposed");
    }

    private string GetLockKey(string taskName, string? key)
    {
        return key is null
            ? $"{_options.ExecutionKeyPrefix}{taskName}"
            : $"{_options.ExecutionKeyPrefix}{taskName}:{key}";
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
            _logger.LogInformation("Connected to Redis for task execution tracker");

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

    private sealed record ExecutionData
    {
        public required string TaskId { get; init; }
        public string? Key { get; init; }
        public required DateTimeOffset StartedAt { get; init; }
        public required DateTimeOffset ExpiresAt { get; init; }
    }
}
