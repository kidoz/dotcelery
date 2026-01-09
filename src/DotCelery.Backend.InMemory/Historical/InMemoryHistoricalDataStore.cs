using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Dashboard;
using Microsoft.Extensions.Options;

namespace DotCelery.Backend.InMemory.Historical;

/// <summary>
/// In-memory implementation of <see cref="IHistoricalDataStore"/> for testing and development.
/// </summary>
public sealed class InMemoryHistoricalDataStore : IHistoricalDataStore
{
    private readonly ConcurrentDictionary<DateTimeOffset, MetricsSnapshot> _snapshots = new();
    private readonly HistoricalDataOptions _options;
    private readonly TimeProvider _timeProvider;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryHistoricalDataStore"/> class.
    /// </summary>
    public InMemoryHistoricalDataStore()
        : this(Options.Create(new HistoricalDataOptions()), TimeProvider.System) { }

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryHistoricalDataStore"/> class.
    /// </summary>
    /// <param name="options">The historical data options.</param>
    public InMemoryHistoricalDataStore(HistoricalDataOptions options)
        : this(Options.Create(options), TimeProvider.System) { }

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryHistoricalDataStore"/> class.
    /// </summary>
    /// <param name="options">The historical data options.</param>
    /// <param name="timeProvider">The time provider.</param>
    public InMemoryHistoricalDataStore(
        IOptions<HistoricalDataOptions> options,
        TimeProvider timeProvider
    )
    {
        _options = options.Value;
        _timeProvider = timeProvider;
    }

    /// <inheritdoc />
    public ValueTask RecordMetricsAsync(
        MetricsSnapshot snapshot,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _snapshots[snapshot.Timestamp] = snapshot;
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<AggregatedMetrics> GetMetricsAsync(
        DateTimeOffset from,
        DateTimeOffset until,
        MetricsGranularity granularity = MetricsGranularity.Hour,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var snapshots = _snapshots
            .Values.Where(s => s.Timestamp >= from && s.Timestamp <= until)
            .ToList();

        if (snapshots.Count == 0)
        {
            return ValueTask.FromResult(
                new AggregatedMetrics
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
                }
            );
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

        return ValueTask.FromResult(
            new AggregatedMetrics
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
            }
        );
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

        var bucketSize = GetBucketSize(granularity);
        var snapshots = _snapshots
            .Values.Where(s => s.Timestamp >= from && s.Timestamp <= until)
            .OrderBy(s => s.Timestamp)
            .ToList();

        // Group snapshots by time bucket
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

            await Task.Yield();
        }
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyDictionary<string, TaskMetricsSummary>> GetMetricsByTaskNameAsync(
        DateTimeOffset from,
        DateTimeOffset until,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var snapshots = _snapshots
            .Values.Where(s =>
                s.Timestamp >= from && s.Timestamp <= until && s.TaskName is not null
            )
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

        return ValueTask.FromResult<IReadOnlyDictionary<string, TaskMetricsSummary>>(snapshots);
    }

    /// <inheritdoc />
    public ValueTask<long> ApplyRetentionAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var cutoff = _timeProvider.GetUtcNow() - _options.RetentionPeriod;
        var keysToRemove = _snapshots.Keys.Where(k => k < cutoff).ToList();

        long removed = 0;
        foreach (var key in keysToRemove)
        {
            if (_snapshots.TryRemove(key, out _))
            {
                removed++;
            }
        }

        return ValueTask.FromResult(removed);
    }

    /// <inheritdoc />
    public ValueTask<long> GetSnapshotCountAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return ValueTask.FromResult((long)_snapshots.Count);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return ValueTask.CompletedTask;
        }

        _disposed = true;
        _snapshots.Clear();
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Clears all stored snapshots.
    /// </summary>
    public void Clear()
    {
        _snapshots.Clear();
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
}
