using Asp.Versioning;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Dashboard;
using DotCelery.Core.Models;
using DotCelery.Dashboard.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace DotCelery.Dashboard.Controllers;

/// <summary>
/// API controller for dashboard data.
/// </summary>
[ApiController]
[ApiVersion(1.0)]
[ReportApiVersions]
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
            var matches = await QueryTasksAsync(
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
                    HasMore = matches.Count > pageSize,
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

    #region Bulk Operations

    /// <summary>
    /// Bulk revoke tasks by IDs or filter.
    /// </summary>
    [HttpPost("tasks/bulk/revoke")]
    public async Task<ActionResult<BulkOperationResponse>> BulkRevoke(
        [FromBody] BulkRevokeRequest request,
        CancellationToken cancellationToken = default
    )
    {
        if (_options.ReadOnly || !_options.AllowTaskOperations)
        {
            return StatusCode(StatusCodes.Status403Forbidden);
        }

        if (_revocationStore is null)
        {
            return BadRequest("Revocation store is not configured");
        }

        if (ExceedsBulkLimit(request.TaskIds) is { } bulkLimitError)
        {
            return bulkLimitError;
        }

        var taskIds = await ResolveTaskIdsAsync(request.TaskIds, request.Filter, cancellationToken);
        if (taskIds.Count == 0)
        {
            return Ok(
                new BulkOperationResponse
                {
                    ProcessedCount = 0,
                    SuccessCount = 0,
                    FailureCount = 0,
                    Message = "No tasks matched the criteria",
                }
            );
        }

        var options = new RevokeOptions
        {
            Terminate = request.Terminate,
            Signal = request.Immediate ? CancellationSignal.Immediate : CancellationSignal.Graceful,
        };

        var successCount = 0;
        var failedIds = new List<string>();

        foreach (var taskId in taskIds)
        {
            try
            {
                await _revocationStore.RevokeAsync(taskId, options, cancellationToken);
                successCount++;
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                failedIds.Add(taskId);
            }
        }

        return Ok(
            new BulkOperationResponse
            {
                ProcessedCount = taskIds.Count,
                SuccessCount = successCount,
                FailureCount = failedIds.Count,
                FailedTaskIds = failedIds.Count > 0 ? failedIds : null,
            }
        );
    }

    /// <summary>
    /// Bulk retry failed tasks by IDs or filter.
    /// </summary>
    [HttpPost("tasks/bulk/retry")]
    public async Task<ActionResult<BulkOperationResponse>> BulkRetry(
        [FromBody] BulkRetryRequest request,
        CancellationToken cancellationToken = default
    )
    {
        if (_options.ReadOnly || !_options.AllowTaskOperations)
        {
            return StatusCode(StatusCodes.Status403Forbidden);
        }

        if (_deadLetterStore is null)
        {
            return BadRequest("Dead letter store is not configured");
        }

        if (ExceedsBulkLimit(request.TaskIds) is { } bulkLimitError)
        {
            return bulkLimitError;
        }

        // For retry, we need task IDs from the dead letter store
        var taskIds = request.TaskIds?.ToList() ?? [];

        if (taskIds.Count == 0 && request.Filter is not null)
        {
            // Get failed tasks from dead letter store matching the filter
            var failedTasks = await GetFailedTasksAsync(request.Filter, cancellationToken);
            taskIds = failedTasks.Select(t => t.TaskId).ToList();
        }

        if (taskIds.Count == 0)
        {
            return Ok(
                new BulkOperationResponse
                {
                    ProcessedCount = 0,
                    SuccessCount = 0,
                    FailureCount = 0,
                    Message = "No tasks matched the criteria",
                }
            );
        }

        var successCount = 0;
        var failedIds = new List<string>();
        var retryMessages = await ResolveRetryMessagesAsync(taskIds, cancellationToken);

        foreach (var taskId in taskIds.Distinct(StringComparer.Ordinal))
        {
            try
            {
                var requeued =
                    retryMessages.TryGetValue(taskId, out var messageId)
                    && await _deadLetterStore.RequeueAsync(messageId, cancellationToken);
                if (requeued)
                {
                    successCount++;
                }
                else
                {
                    failedIds.Add(taskId);
                }
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                failedIds.Add(taskId);
            }
        }

        return Ok(
            new BulkOperationResponse
            {
                ProcessedCount = taskIds.Count,
                SuccessCount = successCount,
                FailureCount = failedIds.Count,
                FailedTaskIds = failedIds.Count > 0 ? failedIds : null,
            }
        );
    }

    /// <summary>
    /// Preview tasks matching a filter without modifying them.
    /// </summary>
    [HttpPost("tasks/filter")]
    public async Task<ActionResult<FilterPreviewResponse>> PreviewFilter(
        [FromBody] TaskFilter filter,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        CancellationToken cancellationToken = default
    )
    {
        (page, pageSize, var offset) = ClampPaging(page, pageSize);

        var tasks = await GetFilteredTasksAsync(filter, pageSize, offset, cancellationToken);

        return Ok(
            new FilterPreviewResponse
            {
                Tasks = tasks,
                Page = page,
                PageSize = pageSize,
                HasMore = tasks.Count == pageSize,
            }
        );
    }

    // Paging is clamped so a request cannot ask for a page outside the configured bounds or
    // overflow the offset
    private (int Page, int PageSize, int Offset) ClampPaging(int page, int pageSize)
    {
        page = Math.Max(1, page);
        pageSize = Math.Clamp(pageSize, 1, Math.Clamp(_options.PageSize, 1, 1000));

        var offset = (long)(page - 1) * pageSize;
        return (page, pageSize, offset > int.MaxValue ? int.MaxValue : (int)offset);
    }

    // A bulk operation may not affect more tasks than the configured limit
    private BadRequestObjectResult? ExceedsBulkLimit(IReadOnlyList<string>? taskIds) =>
        taskIds is { Count: > 0 } ids && ids.Count > _options.MaxBulkOperationSize
            ? BadRequest(
                $"A bulk operation can affect at most {_options.MaxBulkOperationSize} tasks."
            )
            : null;

    private async Task<IReadOnlyList<string>> ResolveTaskIdsAsync(
        IReadOnlyList<string>? explicitIds,
        TaskFilter? filter,
        CancellationToken cancellationToken
    )
    {
        if (explicitIds is { Count: > 0 })
        {
            return explicitIds;
        }

        if (filter is null)
        {
            return [];
        }

        var tasks = await GetFilteredTasksAsync(
            filter,
            _options.MaxBulkOperationSize,
            0,
            cancellationToken
        );
        return tasks.Select(t => t.TaskId).ToList();
    }

    private async Task<IReadOnlyList<TaskSummary>> GetFilteredTasksAsync(
        TaskFilter filter,
        int limit,
        int offset,
        CancellationToken cancellationToken
    )
    {
        var matches = new List<TaskSummary>();
        var position = 0;
        var skipped = 0;
        while (matches.Count < limit)
        {
            var batch = await QueryTasksAsync(
                new TaskSearchRequest { State = filter.State, Queue = filter.Queue },
                100,
                position,
                cancellationToken
            );
            foreach (var task in batch)
            {
                if (
                    !string.IsNullOrEmpty(filter.TaskNamePattern)
                    && !MatchesPattern(task.TaskName, filter.TaskNamePattern)
                )
                    continue;
                if (filter.CompletedAfter.HasValue && !(task.CompletedAt >= filter.CompletedAfter))
                    continue;
                if (
                    filter.CompletedBefore.HasValue && !(task.CompletedAt <= filter.CompletedBefore)
                )
                    continue;
                if (skipped < offset)
                {
                    skipped++;
                    continue;
                }
                matches.Add(task);
                if (matches.Count >= limit)
                    break;
            }
            if (batch.Count < 100)
                break;
            position = checked(position + batch.Count);
        }
        return matches;
    }

    private async Task<IReadOnlyList<TaskSummary>> GetFailedTasksAsync(
        TaskFilter filter,
        CancellationToken cancellationToken
    )
    {
        // Get tasks from dead letter store
        if (_deadLetterStore is null)
        {
            return [];
        }

        var failedTasks = new List<TaskSummary>();
        await foreach (
            var dlm in _deadLetterStore.GetAllAsync(
                _options.MaxBulkOperationSize,
                0,
                cancellationToken
            )
        )
        {
            // Apply filter criteria
            if (
                filter.TaskNamePattern is not null
                && !MatchesPattern(dlm.TaskName, filter.TaskNamePattern)
            )
            {
                continue;
            }

            if (filter.Queue is not null && dlm.Queue != filter.Queue)
            {
                continue;
            }

            if (filter.CompletedAfter.HasValue && dlm.Timestamp < filter.CompletedAfter)
            {
                continue;
            }

            if (filter.CompletedBefore.HasValue && dlm.Timestamp > filter.CompletedBefore)
            {
                continue;
            }

            failedTasks.Add(
                new TaskSummary
                {
                    TaskId = dlm.TaskId,
                    TaskName = dlm.TaskName,
                    State = "Failure",
                    Queue = dlm.Queue,
                    CompletedAt = dlm.Timestamp,
                }
            );
        }

        return failedTasks;
    }

    private static bool MatchesPattern(string value, string pattern)
    {
        // Simple glob pattern matching
        if (pattern == "*")
        {
            return true;
        }

        if (pattern.StartsWith('*') && pattern.EndsWith('*'))
        {
            return value.Contains(pattern[1..^1], StringComparison.OrdinalIgnoreCase);
        }

        if (pattern.StartsWith('*'))
        {
            return value.EndsWith(pattern[1..], StringComparison.OrdinalIgnoreCase);
        }

        if (pattern.EndsWith('*'))
        {
            return value.StartsWith(pattern[..^1], StringComparison.OrdinalIgnoreCase);
        }

        return value.Equals(pattern, StringComparison.OrdinalIgnoreCase);
    }

    #endregion
}

/// <summary>
/// Response for dashboard overview.
/// </summary>
public sealed record DashboardOverviewResponse
{
    /// <summary>
    /// Gets task counts by state.
    /// </summary>
    public required IReadOnlyDictionary<string, long> StateCounts { get; init; }

    /// <summary>
    /// Gets the total worker count.
    /// </summary>
    public int WorkerCount { get; init; }

    /// <summary>
    /// Gets the active worker count.
    /// </summary>
    public int ActiveWorkers { get; init; }

    /// <summary>
    /// Gets queue statistics.
    /// </summary>
    public required IReadOnlyList<QueueStats> Queues { get; init; }

    /// <summary>
    /// Gets dashboard metrics.
    /// </summary>
    public required DashboardMetrics Metrics { get; init; }

    /// <summary>
    /// Gets the count of delayed tasks.
    /// </summary>
    public long? DelayedTaskCount { get; init; }
}

/// <summary>
/// Response for task list.
/// </summary>
public sealed record TaskListResponse
{
    /// <summary>
    /// Gets the tasks.
    /// </summary>
    public required IReadOnlyList<TaskSummary> Tasks { get; init; }

    /// <summary>
    /// Gets the current page.
    /// </summary>
    public int Page { get; init; }

    /// <summary>
    /// Gets the page size.
    /// </summary>
    public int PageSize { get; init; }

    /// <summary>
    /// Gets whether there are more pages.
    /// </summary>
    public bool HasMore { get; init; }
}

/// <summary>
/// Request to revoke a task.
/// </summary>
public sealed record RevokeTaskRequest
{
    /// <summary>
    /// Gets whether to terminate a running task.
    /// </summary>
    public bool Terminate { get; init; }

    /// <summary>
    /// Gets whether to use immediate cancellation.
    /// </summary>
    public bool Immediate { get; init; }
}

/// <summary>
/// Response for filter preview.
/// </summary>
public sealed record FilterPreviewResponse
{
    /// <summary>
    /// Gets the matching tasks.
    /// </summary>
    public required IReadOnlyList<TaskSummary> Tasks { get; init; }

    /// <summary>
    /// Gets the current page.
    /// </summary>
    public int Page { get; init; }

    /// <summary>
    /// Gets the page size.
    /// </summary>
    public int PageSize { get; init; }

    /// <summary>
    /// Gets whether there are more pages.
    /// </summary>
    public bool HasMore { get; init; }
}
