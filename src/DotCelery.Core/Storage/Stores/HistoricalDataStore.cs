using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Dashboard;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// <see cref="IHistoricalDataStore"/> on the storage primitives.
/// </summary>
/// <remarks>
/// Snapshots expire <see cref="HistoricalDataOptions.RetentionPeriod"/> after their timestamp.
/// Recording a snapshot again with the same timestamp and task name replaces it.
/// </remarks>
public sealed class HistoricalDataStore : IHistoricalDataStore
{
    private static readonly JsonTypeInfo<MetricsSnapshot> SnapshotTypeInfo =
        StoreJson.TypeInfo<MetricsSnapshot>();

    private readonly IDocumentStore _documents;
    private readonly HistoricalDataOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly string _collection;

    /// <summary>
    /// Initializes a new instance of the <see cref="HistoricalDataStore"/> class.
    /// </summary>
    /// <param name="storage">The storage provider.</param>
    /// <param name="options">The historical data options.</param>
    /// <param name="storeOptions">The store options.</param>
    /// <param name="timeProvider">The clock for retention.</param>
    public HistoricalDataStore(
        IStorageProvider storage,
        IOptions<HistoricalDataOptions>? options = null,
        IOptions<StorageStoreOptions>? storeOptions = null,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(storage);
        _documents = storage.Documents;
        _options = options?.Value ?? new HistoricalDataOptions();
        _timeProvider = timeProvider ?? TimeProvider.System;
        _collection = (storeOptions?.Value ?? new StorageStoreOptions()).Name("metrics-history");
    }

    /// <inheritdoc />
    public async ValueTask RecordMetricsAsync(
        MetricsSnapshot snapshot,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        var timeToLive = snapshot.Timestamp + _options.RetentionPeriod - _timeProvider.GetUtcNow();
        if (timeToLive <= TimeSpan.Zero)
        {
            return;
        }

        await _documents
            .UpsertAsync(
                _collection,
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"{snapshot.Timestamp.UtcTicks:D19}/{snapshot.TaskName}"
                ),
                snapshot,
                SnapshotTypeInfo,
                new DocumentWriteOptions { TimeToLive = timeToLive, SortKey = snapshot.Timestamp },
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<AggregatedMetrics> GetMetricsAsync(
        DateTimeOffset from,
        DateTimeOffset until,
        MetricsGranularity granularity = MetricsGranularity.Hour,
        CancellationToken cancellationToken = default
    )
    {
        var snapshots = await ReadAsync(from, until, cancellationToken).ConfigureAwait(false);
        var success = snapshots.Sum(s => s.SuccessCount);
        var failure = snapshots.Sum(s => s.FailureCount);
        var seconds = (until - from).TotalSeconds;

        return new AggregatedMetrics
        {
            From = from,
            To = until,
            Granularity = granularity,
            TotalProcessed = success + failure,
            SuccessCount = success,
            FailureCount = failure,
            RetryCount = snapshots.Sum(s => s.RetryCount),
            RevokedCount = snapshots.Sum(s => s.RevokedCount),
            AverageExecutionTime = AverageExecutionTime(snapshots),
            TasksPerSecond = seconds > 0 ? (success + failure) / seconds : 0,
        };
    }

    /// <inheritdoc />
    /// <remarks>At most <see cref="HistoricalDataOptions.MaxDataPoints"/> points are returned.</remarks>
    public async IAsyncEnumerable<MetricsDataPoint> GetTimeSeriesAsync(
        DateTimeOffset from,
        DateTimeOffset until,
        MetricsGranularity granularity = MetricsGranularity.Hour,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        var bucketSize = granularity switch
        {
            MetricsGranularity.Minute => TimeSpan.FromMinutes(1),
            MetricsGranularity.Day => TimeSpan.FromDays(1),
            MetricsGranularity.Week => TimeSpan.FromDays(7),
            _ => TimeSpan.FromHours(1),
        };
        var snapshots = await ReadAsync(from, until, cancellationToken).ConfigureAwait(false);

        var buckets = snapshots
            .GroupBy(s => new DateTimeOffset(
                s.Timestamp.UtcTicks - (s.Timestamp.UtcTicks % bucketSize.Ticks),
                TimeSpan.Zero
            ))
            .OrderBy(bucket => bucket.Key)
            .Take(_options.MaxDataPoints);

        foreach (var bucket in buckets)
        {
            var items = bucket.ToList();
            var success = items.Sum(s => s.SuccessCount);
            var failure = items.Sum(s => s.FailureCount);

            yield return new MetricsDataPoint
            {
                Timestamp = bucket.Key,
                SuccessCount = success,
                FailureCount = failure,
                RetryCount = items.Sum(s => s.RetryCount),
                TasksPerSecond = (success + failure) / bucketSize.TotalSeconds,
                AverageExecutionTime = AverageExecutionTime(items),
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
        var snapshots = await ReadAsync(from, until, cancellationToken).ConfigureAwait(false);

        return snapshots
            .Where(s => s.TaskName is not null)
            .GroupBy(s => s.TaskName!, StringComparer.Ordinal)
            .ToDictionary(
                group => group.Key,
                group =>
                {
                    var times = group
                        .Where(s => s.AverageExecutionTime.HasValue)
                        .Select(s => s.AverageExecutionTime!.Value)
                        .ToList();

                    return new TaskMetricsSummary
                    {
                        TaskName = group.Key,
                        TotalCount = group.Sum(s => s.TotalProcessed),
                        SuccessCount = group.Sum(s => s.SuccessCount),
                        FailureCount = group.Sum(s => s.FailureCount),
                        AverageExecutionTime = AverageExecutionTime(group),
                        MinExecutionTime = times.Count > 0 ? times.Min() : null,
                        MaxExecutionTime = times.Count > 0 ? times.Max() : null,
                    };
                },
                StringComparer.Ordinal
            );
    }

    /// <inheritdoc />
    public ValueTask<long> ApplyRetentionAsync(CancellationToken cancellationToken = default) =>
        _documents.DeleteManyAsync(
            _collection,
            new DocumentFilter
            {
                SortKeyBefore = _timeProvider.GetUtcNow() - _options.RetentionPeriod,
            },
            cancellationToken
        );

    /// <inheritdoc />
    public ValueTask<long> GetSnapshotCountAsync(CancellationToken cancellationToken = default) =>
        _documents.CountAsync(_collection, DocumentFilter.All, cancellationToken);

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static TimeSpan? AverageExecutionTime(IEnumerable<MetricsSnapshot> snapshots)
    {
        var times = snapshots
            .Where(s => s.AverageExecutionTime.HasValue)
            .Select(s => s.AverageExecutionTime!.Value.TotalMilliseconds)
            .ToList();

        return times.Count > 0 && times.Average() > 0
            ? TimeSpan.FromMilliseconds(times.Average())
            : null;
    }

    // Both ends of the range are included. Sort keys are kept to the microsecond, so the end is
    // widened by one microsecond rather than one tick.
    private async ValueTask<List<MetricsSnapshot>> ReadAsync(
        DateTimeOffset from,
        DateTimeOffset until,
        CancellationToken cancellationToken
    )
    {
        var snapshots = new List<MetricsSnapshot>();
        await foreach (
            var document in _documents
                .QueryAsync(
                    _collection,
                    new DocumentFilter
                    {
                        SortKeyFrom = from,
                        SortKeyBefore = until.AddTicks(TimeSpan.TicksPerMicrosecond),
                    },
                    SnapshotTypeInfo,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false)
        )
        {
            snapshots.Add(document.Value);
        }

        return snapshots;
    }
}
