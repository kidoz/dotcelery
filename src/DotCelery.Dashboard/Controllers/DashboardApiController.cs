using Asp.Versioning;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Dashboard;
using DotCelery.Core.Models;
using DotCelery.Dashboard.Models;
using DotCelery.Dashboard.Security;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace DotCelery.Dashboard.Controllers;

/// <summary>
/// API controller for dashboard data.
/// </summary>
[ApiController]
[ApiVersion(1.0)]
[ReportApiVersions]
[DashboardResponseHeaders]
[Route("api/v{version:apiVersion}")]
public partial class DashboardApiController : ControllerBase
{
    private readonly IDashboardDataProvider _dataProvider;
    private readonly IRevocationStore? _revocationStore;
    private readonly IDelayedMessageStore? _delayedMessageStore;
    private readonly IHistoricalDataStore? _historicalDataStore;
    private readonly IDeadLetterStore? _deadLetterStore;
    private readonly DashboardOptions _options;

    /// <summary>
    /// Initializes a new instance of the <see cref="DashboardApiController"/> class.
    /// </summary>
    public DashboardApiController(
        IDashboardDataProvider dataProvider,
        IOptions<DashboardOptions> options,
        IRevocationStore? revocationStore = null,
        IDelayedMessageStore? delayedMessageStore = null,
        IHistoricalDataStore? historicalDataStore = null,
        IDeadLetterStore? deadLetterStore = null
    )
    {
        _dataProvider = dataProvider;
        _revocationStore = revocationStore;
        _delayedMessageStore = delayedMessageStore;
        _historicalDataStore = historicalDataStore;
        _deadLetterStore = deadLetterStore;
        _options = options.Value;
    }

    /// <summary>
    /// Gets dashboard overview data.
    /// </summary>
    [HttpGet("overview")]
    public async Task<ActionResult<DashboardOverviewResponse>> GetOverview(
        CancellationToken cancellationToken
    )
    {
        var stateCounts = await _dataProvider.GetStateCountsAsync(cancellationToken);
        var workers = await _dataProvider.GetWorkersAsync(cancellationToken);
        var queues = await _dataProvider.GetQueueStatsAsync(cancellationToken);
        var metrics = await _dataProvider.GetMetricsAsync(
            _options.MetricsWindow,
            cancellationToken
        );

        long? delayedCount = null;
        if (_delayedMessageStore is not null)
        {
            delayedCount = await _delayedMessageStore.GetPendingCountAsync(cancellationToken);
        }

        return Ok(
            new DashboardOverviewResponse
            {
                StateCounts = stateCounts.ToDictionary(x => x.Key.ToString(), x => x.Value),
                WorkerCount = workers.Count,
                ActiveWorkers = workers.Count(w => w.Status == WorkerStatus.Online),
                Queues = queues,
                Metrics = metrics,
                DelayedTaskCount = delayedCount,
            }
        );
    }

    /// <summary>
    /// Gets queue statistics.
    /// </summary>
    [HttpGet("queues")]
    public async Task<ActionResult<IReadOnlyList<QueueStats>>> GetQueues(
        CancellationToken cancellationToken
    )
    {
        var queues = await _dataProvider.GetQueueStatsAsync(cancellationToken);
        return Ok(queues);
    }

    /// <summary>
    /// Gets worker information.
    /// </summary>
    [HttpGet("workers")]
    public async Task<ActionResult<IReadOnlyList<WorkerInfo>>> GetWorkers(
        CancellationToken cancellationToken
    )
    {
        var workers = await _dataProvider.GetWorkersAsync(cancellationToken);
        return Ok(workers);
    }

    /// <summary>
    /// Gets tasks by state.
    /// </summary>
    [HttpGet("tasks")]
    public async Task<ActionResult<TaskListResponse>> GetTasks(
        [FromQuery] string? state,
        [FromQuery] string? taskName,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        CancellationToken cancellationToken = default
    )
    {
        (page, pageSize, var offset) = ClampPaging(page, pageSize);

        if (
            _taskRecords is not null
            || string.IsNullOrEmpty(state)
            || !string.IsNullOrEmpty(taskName)
        )
        {
            if (
                !string.IsNullOrEmpty(state)
                && (
                    !Enum.TryParse<TaskState>(state, true, out var parsed)
                    || !Enum.IsDefined(parsed)
                )
            )
            {
                return BadRequest("Unknown task state.");
            }
            if (offset >= FilterScanLimit)
                return BadRequest("The requested page starts beyond the task search scan limit.");
            var (matches, truncated) = await QueryTasksAsync(
                new TaskSearchRequest
                {
                    State = string.IsNullOrEmpty(state) ? null : Enum.Parse<TaskState>(state, true),
                    TaskName = taskName,
                },
                pageSize + 1,
                offset,
                cancellationToken
            );
            return Ok(
                new TaskListResponse
                {
                    Tasks = matches.Take(pageSize).ToList(),
                    Page = page,
                    PageSize = pageSize,
                    HasMore = truncated || matches.Count > pageSize,
                    Truncated = truncated,
                }
            );
        }

        IReadOnlyList<TaskSummary> tasks;

        if (!string.IsNullOrEmpty(taskName))
        {
            tasks = await _dataProvider.GetTasksByNameAsync(
                taskName,
                pageSize,
                offset,
                cancellationToken
            );
        }
        else if (
            !string.IsNullOrEmpty(state) && Enum.TryParse<TaskState>(state, true, out var taskState)
        )
        {
            tasks = await _dataProvider.GetTasksByStateAsync(
                taskState,
                pageSize,
                offset,
                cancellationToken
            );
        }
        else
        {
            // Default to pending tasks
            tasks = await _dataProvider.GetTasksByStateAsync(
                TaskState.Pending,
                pageSize,
                offset,
                cancellationToken
            );
        }

        return Ok(
            new TaskListResponse
            {
                Tasks = tasks,
                Page = page,
                PageSize = pageSize,
                HasMore = tasks.Count == pageSize,
            }
        );
    }

    /// <summary>
    /// Gets a specific task.
    /// </summary>
    [HttpGet("tasks/{taskId}")]
    public async Task<ActionResult<TaskSummary>> GetTask(
        string taskId,
        CancellationToken cancellationToken
    )
    {
        var task = await _dataProvider.GetTaskAsync(taskId, cancellationToken);

        if (task is null)
        {
            return NotFound();
        }

        return Ok(task);
    }

    /// <summary>
    /// Revokes a task.
    /// </summary>
    [HttpPost("tasks/{taskId}/revoke")]
    public async Task<ActionResult> RevokeTask(
        string taskId,
        [FromBody] RevokeTaskRequest? request,
        CancellationToken cancellationToken
    )
    {
        if (_options.ReadOnly || !_options.AllowTaskOperations)
        {
            return StatusCode(StatusCodes.Status403Forbidden);
        }

        if (_revocationStore is null)
        {
            return BadRequest("Revocation is not available");
        }

        var options = new RevokeOptions
        {
            Terminate = request?.Terminate ?? false,
            Signal =
                request?.Immediate == true
                    ? CancellationSignal.Immediate
                    : CancellationSignal.Graceful,
        };

        AuditTaskOperation("revoke", [taskId]);
        await _revocationStore.RevokeAsync(taskId, options, cancellationToken);

        return Ok(new { message = $"Cancellation requested for task {taskId}" });
    }

    /// <summary>
    /// Gets dashboard metrics.
    /// </summary>
    [HttpGet("metrics")]
    public async Task<ActionResult<DashboardMetrics>> GetMetrics(
        [FromQuery] int? windowMinutes,
        CancellationToken cancellationToken
    )
    {
        if (windowMinutes is <= 0 or > 10080)
        {
            return BadRequest("The metrics window must be between 1 minute and 7 days.");
        }
        var window = windowMinutes.HasValue
            ? TimeSpan.FromMinutes(windowMinutes.Value)
            : _options.MetricsWindow;

        var metrics = await _dataProvider.GetMetricsAsync(window, cancellationToken);
        return Ok(metrics);
    }

    /// <summary>
    /// Gets available task names.
    /// </summary>
    [HttpGet("task-names")]
    public async Task<ActionResult<IReadOnlyList<string>>> GetTaskNames(
        CancellationToken cancellationToken
    )
    {
        var names = await _dataProvider.GetTaskNamesAsync(cancellationToken);
        return Ok(names);
    }

    #region Historical Metrics

    /// <summary>
    /// Gets historical aggregated metrics for a date range.
    /// </summary>
    [HttpGet("metrics/historical")]
    public async Task<ActionResult<AggregatedMetrics>> GetHistoricalMetrics(
        [FromQuery] DateTimeOffset? from,
        [FromQuery] DateTimeOffset? until,
        [FromQuery] MetricsGranularity granularity = MetricsGranularity.Hour,
        CancellationToken cancellationToken = default
    )
    {
        if (_historicalDataStore is null)
        {
            return BadRequest("Historical data store is not configured");
        }

        var fromDate = from ?? DateTimeOffset.UtcNow.AddDays(-1);
        var untilDate = until ?? DateTimeOffset.UtcNow;
        var historyOptions =
            HttpContext?.RequestServices?.GetService<IOptions<HistoricalDataOptions>>()?.Value
            ?? new HistoricalDataOptions();
        if (untilDate < fromDate || untilDate - fromDate > historyOptions.MaxQueryRange)
            return BadRequest(
                "Historical range is reversed or exceeds the configured maximum span."
            );

        try
        {
            var metrics = await _historicalDataStore.GetMetricsAsync(
                fromDate,
                untilDate,
                granularity,
                cancellationToken
            );

            return Ok(metrics);
        }
        catch (ArgumentOutOfRangeException ex)
        {
            return BadRequest(ex.Message);
        }
    }

    /// <summary>
    /// Gets time series data points for charting.
    /// </summary>
    [HttpGet("metrics/timeseries")]
    public async Task<ActionResult<IReadOnlyList<MetricsDataPoint>>> GetTimeSeries(
        [FromQuery] DateTimeOffset? from,
        [FromQuery] DateTimeOffset? until,
        [FromQuery] MetricsGranularity granularity = MetricsGranularity.Hour,
        CancellationToken cancellationToken = default
    )
    {
        if (_historicalDataStore is null)
        {
            return BadRequest("Historical data store is not configured");
        }

        var fromDate = from ?? DateTimeOffset.UtcNow.AddDays(-1);
        var untilDate = until ?? DateTimeOffset.UtcNow;
        var historyOptions =
            HttpContext?.RequestServices?.GetService<IOptions<HistoricalDataOptions>>()?.Value
            ?? new HistoricalDataOptions();
        if (untilDate < fromDate || untilDate - fromDate > historyOptions.MaxQueryRange)
            return BadRequest(
                "Historical range is reversed or exceeds the configured maximum span."
            );

        try
        {
            var dataPoints = new List<MetricsDataPoint>();
            await foreach (
                var point in _historicalDataStore.GetTimeSeriesAsync(
                    fromDate,
                    untilDate,
                    granularity,
                    cancellationToken
                )
            )
            {
                dataPoints.Add(point);
            }

            return Ok(dataPoints);
        }
        catch (ArgumentOutOfRangeException ex)
        {
            return BadRequest(ex.Message);
        }
    }

    /// <summary>
    /// Gets metrics breakdown by task name.
    /// </summary>
    [HttpGet("metrics/by-task")]
    public async Task<
        ActionResult<IReadOnlyDictionary<string, TaskMetricsSummary>>
    > GetMetricsByTask(
        [FromQuery] DateTimeOffset? from,
        [FromQuery] DateTimeOffset? until,
        CancellationToken cancellationToken = default
    )
    {
        if (_historicalDataStore is null)
        {
            return BadRequest("Historical data store is not configured");
        }

        var fromDate = from ?? DateTimeOffset.UtcNow.AddDays(-1);
        var untilDate = until ?? DateTimeOffset.UtcNow;
        var historyOptions =
            HttpContext?.RequestServices?.GetService<IOptions<HistoricalDataOptions>>()?.Value
            ?? new HistoricalDataOptions();
        if (untilDate < fromDate || untilDate - fromDate > historyOptions.MaxQueryRange)
            return BadRequest(
                "Historical range is reversed or exceeds the configured maximum span."
            );

        try
        {
            var metrics = await _historicalDataStore.GetMetricsByTaskNameAsync(
                fromDate,
                untilDate,
                cancellationToken
            );

            return Ok(metrics);
        }
        catch (ArgumentOutOfRangeException ex)
        {
            return BadRequest(ex.Message);
        }
    }

    #endregion
}
