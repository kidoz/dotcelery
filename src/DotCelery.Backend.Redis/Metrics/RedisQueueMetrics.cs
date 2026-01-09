using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.Metrics;

/// <summary>
/// Redis implementation of <see cref="IQueueMetrics"/>.
/// Uses Redis hashes for storing per-queue metrics counters.
/// </summary>
public sealed class RedisQueueMetrics : IQueueMetrics, IAsyncDisposable
{
    private readonly RedisQueueMetricsOptions _options;
    private readonly ILogger<RedisQueueMetrics> _logger;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    private ConnectionMultiplexer? _connection;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisQueueMetrics"/> class.
    /// </summary>
    public RedisQueueMetrics(
        IOptions<RedisQueueMetricsOptions> options,
        ILogger<RedisQueueMetrics> logger
    )
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async ValueTask<long> GetWaitingCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var value = await db.HashGetAsync(GetMetricsKey(queue), "waiting").ConfigureAwait(false);
        return value.IsNullOrEmpty ? 0 : (long)value;
    }

    /// <inheritdoc />
    public async ValueTask<long> GetRunningCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var value = await db.HashGetAsync(GetMetricsKey(queue), "running").ConfigureAwait(false);
        return value.IsNullOrEmpty ? 0 : (long)value;
    }

    /// <inheritdoc />
    public async ValueTask<long> GetProcessedCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var value = await db.HashGetAsync(GetMetricsKey(queue), "processed").ConfigureAwait(false);
        return value.IsNullOrEmpty ? 0 : (long)value;
    }

    /// <inheritdoc />
    public async ValueTask<int> GetConsumerCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var value = await db.HashGetAsync(GetMetricsKey(queue), "consumers").ConfigureAwait(false);
        return value.IsNullOrEmpty ? 0 : (int)value;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<string>> GetQueuesAsync(
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var members = await db.SetMembersAsync(_options.QueuesKey).ConfigureAwait(false);
        return members.Select(m => (string)m!).ToList();
    }

    /// <inheritdoc />
    public async ValueTask<QueueMetricsData> GetMetricsAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var entries = await db.HashGetAllAsync(GetMetricsKey(queue)).ConfigureAwait(false);

        var data = entries.ToDictionary(e => (string)e.Name!, e => e.Value);

        return new QueueMetricsData
        {
            Queue = queue,
            WaitingCount = GetLong(data, "waiting"),
            RunningCount = GetLong(data, "running"),
            ProcessedCount = GetLong(data, "processed"),
            SuccessCount = GetLong(data, "success"),
            FailureCount = GetLong(data, "failure"),
            ConsumerCount = (int)GetLong(data, "consumers"),
            AverageDuration = GetAverageDuration(data),
            LastEnqueuedAt = GetTimestamp(data, "lastEnqueued"),
            LastCompletedAt = GetTimestamp(data, "lastCompleted"),
        };
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, QueueMetricsData>> GetAllMetricsAsync(
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var queues = await GetQueuesAsync(cancellationToken).ConfigureAwait(false);

        var result = new Dictionary<string, QueueMetricsData>();
        foreach (var queue in queues)
        {
            result[queue] = await GetMetricsAsync(queue, cancellationToken).ConfigureAwait(false);
        }

        return result;
    }

    /// <inheritdoc />
    public async ValueTask RecordStartedAsync(
        string queue,
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var key = GetMetricsKey(queue);

        await db.SetAddAsync(_options.QueuesKey, queue).ConfigureAwait(false);
        await db.HashIncrementAsync(key, "running").ConfigureAwait(false);
        await db.HashDecrementAsync(key, "waiting").ConfigureAwait(false);
        await db.HashSetAsync(_options.RunningTasksKey, taskId, queue).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask RecordCompletedAsync(
        string queue,
        string taskId,
        bool success,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var key = GetMetricsKey(queue);
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        await db.HashDecrementAsync(key, "running").ConfigureAwait(false);
        await db.HashIncrementAsync(key, "processed").ConfigureAwait(false);
        await db.HashIncrementAsync(key, success ? "success" : "failure").ConfigureAwait(false);
        await db.HashSetAsync(key, "lastCompleted", now).ConfigureAwait(false);

        // Update average duration (using cumulative average)
        await db.HashIncrementAsync(key, "totalDurationMs", (long)duration.TotalMilliseconds)
            .ConfigureAwait(false);
        await db.HashIncrementAsync(key, "completedCount").ConfigureAwait(false);

        await db.HashDeleteAsync(_options.RunningTasksKey, taskId).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask RecordEnqueuedAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var key = GetMetricsKey(queue);
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        await db.SetAddAsync(_options.QueuesKey, queue).ConfigureAwait(false);
        await db.HashIncrementAsync(key, "waiting").ConfigureAwait(false);
        await db.HashSetAsync(key, "lastEnqueued", now).ConfigureAwait(false);
    }

    /// <summary>
    /// Registers a consumer for a queue.
    /// </summary>
    public async ValueTask RegisterConsumerAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        await db.SetAddAsync(_options.QueuesKey, queue).ConfigureAwait(false);
        await db.HashIncrementAsync(GetMetricsKey(queue), "consumers").ConfigureAwait(false);
    }

    /// <summary>
    /// Unregisters a consumer from a queue.
    /// </summary>
    public async ValueTask UnregisterConsumerAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        await db.HashDecrementAsync(GetMetricsKey(queue), "consumers").ConfigureAwait(false);
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
        _logger.LogInformation("Redis queue metrics disposed");
    }

    private string GetMetricsKey(string queue) => $"{_options.MetricsKeyPrefix}{queue}";

    private static long GetLong(Dictionary<string, RedisValue> data, string field)
    {
        return data.TryGetValue(field, out var value) && !value.IsNullOrEmpty ? (long)value : 0;
    }

    private static DateTimeOffset? GetTimestamp(Dictionary<string, RedisValue> data, string field)
    {
        if (!data.TryGetValue(field, out var value) || value.IsNullOrEmpty)
        {
            return null;
        }

        return DateTimeOffset.FromUnixTimeMilliseconds((long)value);
    }

    private static TimeSpan? GetAverageDuration(Dictionary<string, RedisValue> data)
    {
        var totalMs = GetLong(data, "totalDurationMs");
        var count = GetLong(data, "completedCount");

        if (count == 0)
        {
            return null;
        }

        return TimeSpan.FromMilliseconds(totalMs / count);
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
            _logger.LogInformation("Connected to Redis for queue metrics");

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
