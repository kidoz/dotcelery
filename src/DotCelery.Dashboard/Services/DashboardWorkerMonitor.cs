using DotCelery.Core.Dashboard;
using DotCelery.Dashboard.Hubs;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Dashboard.Services;

/// <summary>
/// Watches the worker registry: marks and removes workers that stopped sending heartbeats, and
/// notifies the dashboard's SignalR clients when a worker's status changes.
/// </summary>
public sealed class DashboardWorkerMonitor : BackgroundService
{
    private readonly IWorkerRegistry? _registry;
    private readonly DashboardNotificationService? _notifications;
    private readonly DashboardOptions _options;
    private readonly ILogger<DashboardWorkerMonitor> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly Dictionary<string, WorkerSnapshot> _lastSeen = new(StringComparer.Ordinal);

    /// <summary>
    /// Initializes a new instance of the <see cref="DashboardWorkerMonitor"/> class.
    /// </summary>
    /// <param name="options">The dashboard options.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="registry">The worker registry, when one is registered.</param>
    /// <param name="notifications">The SignalR notifications, when the hub is mapped.</param>
    /// <param name="timeProvider">The clock for the poll interval.</param>
    public DashboardWorkerMonitor(
        IOptions<DashboardOptions> options,
        ILogger<DashboardWorkerMonitor> logger,
        IWorkerRegistry? registry = null,
        DashboardNotificationService? notifications = null,
        TimeProvider? timeProvider = null
    )
    {
        _options = options.Value;
        _logger = logger;
        _registry = registry;
        _notifications = notifications;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_registry is null)
        {
            _logger.LogDebug("No worker registry is registered; the worker monitor is idle");
            return;
        }

        var interval =
            _options.RefreshIntervalSeconds > 0
                ? TimeSpan.FromSeconds(_options.RefreshIntervalSeconds)
                : TimeSpan.FromSeconds(5);

        try
        {
            using var timer = new PeriodicTimer(interval, _timeProvider);

            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                await PollAsync(stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Expected during shutdown
        }
    }

    private async Task PollAsync(CancellationToken cancellationToken)
    {
        // A worker that missed its heartbeat budget is marked unresponsive and, after another
        // budget, removed
        await _registry!
            .CleanupStaleWorkersAsync(_options.WorkerOfflineTimeout, cancellationToken)
            .ConfigureAwait(false);

        var seen = new HashSet<string>(StringComparer.Ordinal);

        await foreach (
            var worker in _registry.GetActiveWorkersAsync(cancellationToken).ConfigureAwait(false)
        )
        {
            seen.Add(worker.WorkerId);

            var snapshot = WorkerSnapshot.From(worker);
            if (_lastSeen.TryGetValue(worker.WorkerId, out var previous) && previous == snapshot)
            {
                continue;
            }

            _lastSeen[worker.WorkerId] = snapshot;
            await NotifyAsync(worker).ConfigureAwait(false);
        }

        // A removed worker is not announced again; its unresponsive update was sent before it
        // was removed
        foreach (var workerId in _lastSeen.Keys.Where(id => !seen.Contains(id)).ToList())
        {
            _lastSeen.Remove(workerId);
        }
    }

    private async Task NotifyAsync(WorkerInfo worker)
    {
        if (_notifications is null || !_options.EnableRealTimeUpdates)
        {
            return;
        }

        try
        {
            await _notifications.NotifyWorkerStatusChangedAsync(worker).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                ex,
                "Failed to notify dashboard clients about worker {WorkerId}",
                worker.WorkerId
            );
        }
    }

    private readonly record struct WorkerSnapshot(
        WorkerStatus Status,
        int ActiveTasks,
        long ProcessedCount,
        DateTimeOffset LastHeartbeat
    )
    {
        public static WorkerSnapshot From(WorkerInfo worker) =>
            new(worker.Status, worker.ActiveTasks, worker.ProcessedCount, worker.LastHeartbeat);
    }
}
