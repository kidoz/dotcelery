namespace DotCelery.Core.Dashboard;

/// <summary>
/// Configuration options for historical data storage.
/// </summary>
public sealed class HistoricalDataOptions
{
    /// <summary>
    /// Gets or sets how long to retain historical data. Default is 30 days.
    /// </summary>
    public TimeSpan RetentionPeriod { get; set; } = TimeSpan.FromDays(30);

    /// <summary>
    /// Gets or sets how often to record metrics snapshots. Default is every minute.
    /// </summary>
    public TimeSpan SnapshotInterval { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Gets or sets whether to enable automatic retention cleanup.
    /// </summary>
    public bool EnableAutoRetention { get; set; } = true;

    /// <summary>
    /// Gets or sets the maximum number of data points to return in time series queries.
    /// </summary>
    public int MaxDataPoints { get; set; } = 1000;

    /// <summary>
    /// Gets or sets the default granularity for queries when not specified.
    /// </summary>
    public MetricsGranularity DefaultGranularity { get; set; } = MetricsGranularity.Hour;
}
