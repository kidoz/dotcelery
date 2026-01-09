using DotCelery.Core.Dashboard;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;

namespace DotCelery.Dashboard.Hubs;

/// <summary>
/// SignalR hub for real-time dashboard updates.
/// </summary>
public sealed class DashboardHub : Hub
{
    private readonly IDashboardDataProvider _dataProvider;
    private readonly DashboardOptions _options;

    /// <summary>
    /// Initializes a new instance of the <see cref="DashboardHub"/> class.
    /// </summary>
    public DashboardHub(IDashboardDataProvider dataProvider, IOptions<DashboardOptions> options)
    {
        _dataProvider = dataProvider;
        _options = options.Value;
    }

    /// <summary>
    /// Gets the current dashboard overview.
    /// </summary>
    public async Task GetOverview()
    {
        var stateCounts = await _dataProvider.GetStateCountsAsync();
        var workers = await _dataProvider.GetWorkersAsync();
        var metrics = await _dataProvider.GetMetricsAsync(_options.MetricsWindow);

        await Clients.Caller.SendAsync(
            "OverviewUpdated",
            new
            {
                StateCounts = stateCounts.ToDictionary(x => x.Key.ToString(), x => x.Value),
                WorkerCount = workers.Count,
                ActiveWorkers = workers.Count(w => w.Status == WorkerStatus.Online),
                Metrics = metrics,
            }
        );
    }

    /// <summary>
    /// Subscribes to task updates for a specific task.
    /// </summary>
    /// <param name="taskId">The task ID to watch.</param>
    public async Task WatchTask(string taskId)
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, $"task:{taskId}");
    }

    /// <summary>
    /// Unsubscribes from task updates.
    /// </summary>
    /// <param name="taskId">The task ID to stop watching.</param>
    public async Task UnwatchTask(string taskId)
    {
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, $"task:{taskId}");
    }

    /// <summary>
    /// Subscribes to all task state changes.
    /// </summary>
    public async Task SubscribeToTaskUpdates()
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, "task-updates");
    }

    /// <summary>
    /// Unsubscribes from all task state changes.
    /// </summary>
    public async Task UnsubscribeFromTaskUpdates()
    {
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, "task-updates");
    }
}

/// <summary>
/// Service for broadcasting dashboard updates via SignalR.
/// </summary>
public sealed class DashboardNotificationService
{
    private readonly IHubContext<DashboardHub> _hubContext;

    /// <summary>
    /// Initializes a new instance of the <see cref="DashboardNotificationService"/> class.
    /// </summary>
    public DashboardNotificationService(IHubContext<DashboardHub> hubContext)
    {
        _hubContext = hubContext;
    }

    /// <summary>
    /// Notifies clients that a task's state has changed.
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <param name="state">The new state.</param>
    /// <param name="summary">Optional task summary.</param>
    public async Task NotifyTaskStateChangedAsync(
        string taskId,
        string state,
        TaskSummary? summary = null
    )
    {
        var payload = new
        {
            TaskId = taskId,
            State = state,
            Summary = summary,
        };

        // Notify specific task watchers
        await _hubContext.Clients.Group($"task:{taskId}").SendAsync("TaskStateChanged", payload);

        // Notify general subscribers
        await _hubContext.Clients.Group("task-updates").SendAsync("TaskStateChanged", payload);
    }

    /// <summary>
    /// Notifies clients that a worker's status has changed.
    /// </summary>
    /// <param name="worker">The worker information.</param>
    public async Task NotifyWorkerStatusChangedAsync(WorkerInfo worker)
    {
        await _hubContext.Clients.All.SendAsync("WorkerStatusChanged", worker);
    }

    /// <summary>
    /// Notifies clients to refresh the overview.
    /// </summary>
    public async Task NotifyOverviewChangedAsync()
    {
        await _hubContext.Clients.All.SendAsync("OverviewChanged");
    }
}
