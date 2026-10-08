using DotCelery.Core.Abstractions;
using DotCelery.Core.Dashboard;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Dashboard.Services;

/// <summary>
/// Records snapshots into the historical data store, so the dashboard's metrics survive the
/// task record retention and can be charted over longer windows. Each snapshot holds the tasks
/// each queue completed since the previous snapshot, computed from the queue metrics' totals.
/// </summary>
/// <remarks>
/// The collector does nothing without a historical data store and a queue metrics store. The
/// first poll of a queue only establishes its baseline, so restarting the collector does not
/// count anything twice.
/// </remarks>
public sealed class DashboardMetricsCollector : BackgroundService
{
    private static readonly TimeSpan RetentionInterval = TimeSpan.FromHours(1);

    private readonly IHistoricalDataStore? _history;
    private readonly IQueueMetrics? _metrics;
    private readonly DashboardOptions _options;
    private readonly ILogger<DashboardMetricsCollector> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly Dictionary<string, QueueTotals> _baseline = new(StringComparer.Ordinal);

    private DateTimeOffset _lastRetentionAt;

    /// <summary>
    /// Initializes a new instance of the <see cref="DashboardMetricsCollector"/> class.
    /// </summary>
    /// <param name="options">The dashboard options.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="history">The historical data store, when one is registered.</param>
    /// <param name="metrics">The queue metrics store, when one is registered.</param>
    /// <param name="timeProvider">The clock for the snapshot interval.</param>
    public DashboardMetricsCollector(
        IOptions<DashboardOptions> options,
        ILogger<DashboardMetricsCollector> logger,
        IHistoricalDataStore? history = null,
        IQueueMetrics? metrics = null,
        TimeProvider? timeProvider = null
    )
    {
        _options = options.Value;
        _logger = logger;
        _history = history;
        _metrics = metrics;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _lastRetentionAt = _timeProvider.GetUtcNow();
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_history is null || _metrics is null)
        {
            _logger.LogDebug(
                "No historical data store or queue metrics store is registered; metrics snapshots are disabled"
            );
            return;
        }

        _logger.LogInformation(
            "Recording metrics snapshots every {Interval}",
            _options.MetricsSnapshotInterval
        );

        try
        {
            using var timer = new PeriodicTimer(_options.MetricsSnapshotInterval, _timeProvider);

            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                await SnapshotAsync(stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Expected during shutdown
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "The metrics collector stopped");
        }
    }

    private async Task SnapshotAsync(CancellationToken cancellationToken)
    {
        var now = _timeProvider.GetUtcNow();
        var metrics = await _metrics!.GetAllMetricsAsync(cancellationToken).ConfigureAwait(false);

        foreach (var (queue, data) in metrics)
        {
            var current = new QueueTotals(data.SuccessCount, data.FailureCount);

            if (!_baseline.TryGetValue(queue, out var previous))
            {
                // The first poll only establishes the baseline
                _baseline[queue] = current;
                continue;
            }

            _baseline[queue] = current;

            var success = Math.Max(0, current.Success - previous.Success);
            var failure = Math.Max(0, current.Failure - previous.Failure);

            if (success == 0 && failure == 0)
            {
                continue;
            }

            await _history!
                .RecordMetricsAsync(
                    new MetricsSnapshot
                    {
                        Timestamp = now,
                        Queue = queue,
                        SuccessCount = success,
                        FailureCount = failure,
                        AverageExecutionTime = data.AverageDuration,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        if (now - _lastRetentionAt >= RetentionInterval)
        {
            await _history!.ApplyRetentionAsync(cancellationToken).ConfigureAwait(false);
            _lastRetentionAt = now;
        }
    }

    private readonly record struct QueueTotals(long Success, long Failure);
}
