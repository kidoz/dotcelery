using System.Runtime.CompilerServices;
using System.Text.Json;
using DotCelery.Backend.Redis.Serialization;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Dashboard;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.Historical;

/// <summary>
/// Redis implementation of <see cref="IHistoricalDataStore"/>.
/// Uses Redis sorted sets for time-indexed storage and hashes for aggregation.
/// </summary>
public sealed class RedisHistoricalDataStore : IHistoricalDataStore
{
    private readonly RedisHistoricalDataStoreOptions _options;
    private readonly ILogger<RedisHistoricalDataStore> _logger;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    // Fallback options for MetricsSnapshot type not in AOT context
    private static JsonSerializerOptions JsonOptions => RedisJsonHelper.FallbackOptions;

    private ConnectionMultiplexer? _connection;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisHistoricalDataStore"/> class.
    /// </summary>
    public RedisHistoricalDataStore(
        IOptions<RedisHistoricalDataStoreOptions> options,
        ILogger<RedisHistoricalDataStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async ValueTask RecordMetricsAsync(
        MetricsSnapshot snapshot,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(snapshot);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var id = Guid.NewGuid().ToString("N");
        var key = $"{_options.SnapshotKeyPrefix}{id}";
        var score = snapshot.Timestamp.ToUnixTimeMilliseconds();
        var json = JsonSerializer.Serialize(snapshot, JsonOptions);

        // Store the snapshot
        var expiry = _options.RetentionPeriod;
        await db.StringSetAsync(key, json, expiry).ConfigureAwait(false);

        // Index by time
        await db.SortedSetAddAsync(_options.TimeIndexKey, id, score).ConfigureAwait(false);

        // Update task-specific aggregates if task name is present
        if (snapshot.TaskName is not null)
        {
            var taskKey = $"{_options.TaskMetricsKeyPrefix}{snapshot.TaskName}";
            await db.HashIncrementAsync(taskKey, "totalSuccess", snapshot.SuccessCount)
                .ConfigureAwait(false);
            await db.HashIncrementAsync(taskKey, "totalFailure", snapshot.FailureCount)
                .ConfigureAwait(false);
            await db.HashIncrementAsync(taskKey, "totalRetry", snapshot.RetryCount)
                .ConfigureAwait(false);

            if (snapshot.AverageExecutionTime.HasValue)
            {
                await db.HashIncrementAsync(
                        taskKey,
                        "totalDurationMs",
                        (long)(
                            snapshot.AverageExecutionTime.Value.TotalMilliseconds
                            * snapshot.TotalProcessed
                        )
                    )
                    .ConfigureAwait(false);
                await db.HashIncrementAsync(taskKey, "durationSampleCount", snapshot.TotalProcessed)
                    .ConfigureAwait(false);
            }

            await db.KeyExpireAsync(taskKey, expiry).ConfigureAwait(false);
        }

        _logger.LogDebug("Recorded metrics snapshot at {Timestamp}", snapshot.Timestamp);
    }

    /// <inheritdoc />
    public async ValueTask<AggregatedMetrics> GetMetricsAsync(
        DateTimeOffset from,
        DateTimeOffset until,
        MetricsGranularity granularity = MetricsGranularity.Hour,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var snapshots = await GetSnapshotsInRangeAsync(from, until, cancellationToken)
            .ConfigureAwait(false);

        if (snapshots.Count == 0)
        {
            return new AggregatedMetrics
            {
                From = from,
                To = until,
                Granularity = granularity,
                TotalProcessed = 0,
                SuccessCount = 0,
                FailureCount = 0,
                RetryCount = 0,
                RevokedCount = 0,
                TasksPerSecond = 0,
            };
        }

        var totalSuccess = snapshots.Sum(s => s.SuccessCount);
        var totalFailure = snapshots.Sum(s => s.FailureCount);
        var totalRetry = snapshots.Sum(s => s.RetryCount);
        var totalRevoked = snapshots.Sum(s => s.RevokedCount);
        var totalProcessed = totalSuccess + totalFailure;

        var avgExecutionTime = snapshots
            .Where(s => s.AverageExecutionTime.HasValue)
            .Select(s => s.AverageExecutionTime!.Value)
            .DefaultIfEmpty()
            .Average(t => t.TotalMilliseconds);

        var durationSeconds = (until - from).TotalSeconds;
        var tasksPerSecond = durationSeconds > 0 ? totalProcessed / durationSeconds : 0;

        return new AggregatedMetrics
        {
            From = from,
            To = until,
            Granularity = granularity,
            TotalProcessed = totalProcessed,
            SuccessCount = totalSuccess,
            FailureCount = totalFailure,
            RetryCount = totalRetry,
            RevokedCount = totalRevoked,
            AverageExecutionTime =
                avgExecutionTime > 0 ? TimeSpan.FromMilliseconds(avgExecutionTime) : null,
            TasksPerSecond = tasksPerSecond,
        };
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<MetricsDataPoint> GetTimeSeriesAsync(
        DateTimeOffset from,
        DateTimeOffset until,
        MetricsGranularity granularity = MetricsGranularity.Hour,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var snapshots = await GetSnapshotsInRangeAsync(from, until, cancellationToken)
            .ConfigureAwait(false);
        var bucketSize = GetBucketSize(granularity);

        var buckets = snapshots
            .GroupBy(s => GetBucketStart(s.Timestamp, bucketSize))
            .OrderBy(g => g.Key)
            .Take(_options.MaxDataPoints);

        foreach (var bucket in buckets)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var items = bucket.ToList();
            var totalSuccess = items.Sum(s => s.SuccessCount);
            var totalFailure = items.Sum(s => s.FailureCount);
            var totalRetry = items.Sum(s => s.RetryCount);
            var totalProcessed = totalSuccess + totalFailure;

            var avgTime = items
                .Where(s => s.AverageExecutionTime.HasValue)
                .Select(s => s.AverageExecutionTime!.Value.TotalMilliseconds)
                .DefaultIfEmpty()
                .Average();

            var tasksPerSecond =
                bucketSize.TotalSeconds > 0 ? totalProcessed / bucketSize.TotalSeconds : 0;

            yield return new MetricsDataPoint
            {
                Timestamp = bucket.Key,
                SuccessCount = totalSuccess,
                FailureCount = totalFailure,
                RetryCount = totalRetry,
                TasksPerSecond = tasksPerSecond,
                AverageExecutionTime = avgTime > 0 ? TimeSpan.FromMilliseconds(avgTime) : null,
            };
        }
    }

    /// <inheritdoc />
    public async ValueTask<
        IReadOnlyDictionary<string, TaskMetricsSummary>
    > GetMetricsByTaskNameAsync(
        DateTimeOffset from,
        DateTimeOffset until,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var snapshots = await GetSnapshotsInRangeAsync(from, until, cancellationToken)
            .ConfigureAwait(false);

        return snapshots
            .Where(s => s.TaskName is not null)
            .GroupBy(s => s.TaskName!)
            .ToDictionary(
                g => g.Key,
                g =>
                {
                    var items = g.ToList();
                    var executionTimes = items
                        .Where(s => s.AverageExecutionTime.HasValue)
                        .Select(s => s.AverageExecutionTime!.Value)
                        .ToList();

                    return new TaskMetricsSummary
                    {
                        TaskName = g.Key,
                        TotalCount = items.Sum(s => s.TotalProcessed),
                        SuccessCount = items.Sum(s => s.SuccessCount),
                        FailureCount = items.Sum(s => s.FailureCount),
                        AverageExecutionTime =
                            executionTimes.Count > 0
                                ? TimeSpan.FromMilliseconds(
                                    executionTimes.Average(t => t.TotalMilliseconds)
                                )
                                : null,
                        MinExecutionTime = executionTimes.Count > 0 ? executionTimes.Min() : null,
                        MaxExecutionTime = executionTimes.Count > 0 ? executionTimes.Max() : null,
                    };
                }
            );
    }

    /// <inheritdoc />
    public async ValueTask<long> ApplyRetentionAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var cutoff = DateTimeOffset
            .UtcNow.Subtract(_options.RetentionPeriod)
            .ToUnixTimeMilliseconds();

        // Get IDs of expired snapshots
        var expiredIds = await db.SortedSetRangeByScoreAsync(
                _options.TimeIndexKey,
                double.NegativeInfinity,
                cutoff
            )
            .ConfigureAwait(false);

        if (expiredIds.Length == 0)
        {
            return 0;
        }

        // Delete snapshot data
        var keysToDelete = expiredIds
            .Select(id => (RedisKey)$"{_options.SnapshotKeyPrefix}{id}")
            .ToArray();
        await db.KeyDeleteAsync(keysToDelete).ConfigureAwait(false);

        // Remove from index
        await db.SortedSetRemoveRangeByScoreAsync(
                _options.TimeIndexKey,
                double.NegativeInfinity,
                cutoff
            )
            .ConfigureAwait(false);

        _logger.LogInformation("Removed {Count} expired snapshots", expiredIds.Length);
        return expiredIds.Length;
    }

    /// <inheritdoc />
    public async ValueTask<long> GetSnapshotCountAsync(
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        return await db.SortedSetLengthAsync(_options.TimeIndexKey).ConfigureAwait(false);
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
        _logger.LogInformation("Redis historical data store disposed");
    }

    private async Task<List<MetricsSnapshot>> GetSnapshotsInRangeAsync(
        DateTimeOffset from,
        DateTimeOffset until,
        CancellationToken cancellationToken
    )
    {
        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var fromScore = from.ToUnixTimeMilliseconds();
        var untilScore = until.ToUnixTimeMilliseconds();

        var ids = await db.SortedSetRangeByScoreAsync(_options.TimeIndexKey, fromScore, untilScore)
            .ConfigureAwait(false);

        var snapshots = new List<MetricsSnapshot>();
        foreach (var id in ids)
        {
            var key = $"{_options.SnapshotKeyPrefix}{id}";
            var json = await db.StringGetAsync(key).ConfigureAwait(false);

            if (!json.IsNullOrEmpty)
            {
                var snapshot = JsonSerializer.Deserialize<MetricsSnapshot>(
                    (string)json!,
                    JsonOptions
                );
                if (snapshot is not null)
                {
                    snapshots.Add(snapshot);
                }
            }
        }

        return snapshots;
    }

    private static TimeSpan GetBucketSize(MetricsGranularity granularity)
    {
        return granularity switch
        {
            MetricsGranularity.Minute => TimeSpan.FromMinutes(1),
            MetricsGranularity.Hour => TimeSpan.FromHours(1),
            MetricsGranularity.Day => TimeSpan.FromDays(1),
            MetricsGranularity.Week => TimeSpan.FromDays(7),
            _ => TimeSpan.FromHours(1),
        };
    }

    private static DateTimeOffset GetBucketStart(DateTimeOffset timestamp, TimeSpan bucketSize)
    {
        var ticks = timestamp.UtcTicks;
        var bucketTicks = bucketSize.Ticks;
        var bucketStart = ticks - (ticks % bucketTicks);
        return new DateTimeOffset(bucketStart, TimeSpan.Zero);
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
            _logger.LogInformation("Connected to Redis for historical data store");

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
