using DotCelery.Core.Dashboard;

namespace DotCelery.Core.Abstractions;

/// <summary>
/// Storage interface for historical metrics data.
/// Provides time-series storage and retrieval for dashboard metrics.
/// </summary>
public interface IHistoricalDataStore : IAsyncDisposable
{
    /// <summary>
    /// Records a metrics snapshot at the current time.
    /// </summary>
    /// <param name="snapshot">The metrics snapshot to record.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask RecordMetricsAsync(
        MetricsSnapshot snapshot,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets aggregated metrics for a date range.
    /// </summary>
    /// <param name="from">Start of the date range.</param>
    /// <param name="until">End of the date range.</param>
    /// <param name="granularity">Aggregation granularity.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Aggregated metrics for the period.</returns>
    ValueTask<AggregatedMetrics> GetMetricsAsync(
        DateTimeOffset from,
        DateTimeOffset until,
        MetricsGranularity granularity = MetricsGranularity.Hour,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets time-series data points for charting.
    /// </summary>
    /// <param name="from">Start of the date range.</param>
    /// <param name="until">End of the date range.</param>
    /// <param name="granularity">Data point granularity.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async enumerable of data points.</returns>
    IAsyncEnumerable<MetricsDataPoint> GetTimeSeriesAsync(
        DateTimeOffset from,
        DateTimeOffset until,
        MetricsGranularity granularity = MetricsGranularity.Hour,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets metrics grouped by task name for a date range.
    /// </summary>
    /// <param name="from">Start of the date range.</param>
    /// <param name="until">End of the date range.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Dictionary of task names to their metrics summaries.</returns>
    ValueTask<IReadOnlyDictionary<string, TaskMetricsSummary>> GetMetricsByTaskNameAsync(
        DateTimeOffset from,
        DateTimeOffset until,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Applies retention policy, removing data older than the configured period.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of records removed.</returns>
    ValueTask<long> ApplyRetentionAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the total number of stored snapshots.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The count of stored snapshots.</returns>
    ValueTask<long> GetSnapshotCountAsync(CancellationToken cancellationToken = default);
}
