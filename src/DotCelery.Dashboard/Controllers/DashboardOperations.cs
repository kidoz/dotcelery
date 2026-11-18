using DotCelery.Core.Abstractions;
using DotCelery.Core.Dashboard;
using DotCelery.Core.Models;
using DotCelery.Core.MultiTenancy;
using DotCelery.Dashboard.Models;
using Microsoft.AspNetCore.Mvc;

namespace DotCelery.Dashboard.Controllers;

public partial class DashboardApiController
{
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

        var taskIds = request.TaskIds ?? [];
        if (taskIds.Count == 0 && request.Filter is { } filter && _options.MaxBulkOperationSize > 0)
        {
            var (tasks, truncated) = await GetFilteredTasksAsync(
                filter,
                _options.MaxBulkOperationSize,
                0,
                cancellationToken
            );
            if (truncated)
                return BadRequest(
                    "The task filter exceeded the scan limit. Narrow the filter and try again."
                );
            taskIds = tasks.Select(task => task.TaskId).ToList();
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

        AuditTaskOperation("bulk-revoke", taskIds);
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

        AuditTaskOperation("bulk-retry", taskIds);
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

        if (offset >= FilterScanLimit)
            return BadRequest("The requested page starts beyond the task filter scan limit.");

        var (tasks, truncated) = await GetFilteredTasksAsync(
            filter,
            pageSize + 1,
            offset,
            cancellationToken
        );

        return Ok(
            new FilterPreviewResponse
            {
                Tasks = tasks.Take(pageSize).ToList(),
                Page = page,
                PageSize = pageSize,
                HasMore = truncated || tasks.Count > pageSize,
                Truncated = truncated,
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

    private int FilterScanLimit => Math.Max(1, _options.MaxFilterScan);

    private void AuditTaskOperation(string operation, IReadOnlyList<string> taskIds)
    {
        if (HttpContext?.RequestServices?.GetService<ILoggerFactory>() is not { } factory)
            return;
        LogTaskOperation(
            factory.CreateLogger("DotCelery.Dashboard.Audit"),
            operation,
            HttpContext.User.Identity?.Name,
            TenantContext.Current.TenantId,
            HttpContext.Connection.RemoteIpAddress?.ToString(),
            taskIds.Count,
            string.Join(
                ",",
                taskIds.Take(10).Select(id => id is { Length: > 128 } ? id[..128] : id)
            )
        );
    }

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Dashboard operation {Operation} by {User} in tenant {Tenant} from {RemoteIp} targeted {TaskCount} tasks: {TaskIds}"
    )]
    private static partial void LogTaskOperation(
        ILogger logger,
        string operation,
        string? user,
        string? tenant,
        string? remoteIp,
        int taskCount,
        string taskIds
    );

    private async Task<(IReadOnlyList<TaskSummary> Tasks, bool Truncated)> GetFilteredTasksAsync(
        TaskFilter filter,
        int limit,
        int offset,
        CancellationToken cancellationToken
    )
    {
        // The storage index uses an exclusive bound and may round timestamps. Widen it
        // slightly and apply the exact inclusive comparison to the returned records.
        var until =
            filter.CompletedBefore is { } before
            && before.UtcTicks <= DateTimeOffset.MaxValue.UtcTicks - TimeSpan.TicksPerMillisecond
                ? before.ToUniversalTime().AddMilliseconds(1)
                : (DateTimeOffset?)null;
        var query = new TaskSearchRequest
        {
            State = filter.State,
            Queue = filter.Queue,
            From = filter.CompletedAfter,
            Until = until,
        };
        var budget = FilterScanLimit;
        if (_taskRecords is { IsAvailable: true } records)
            return await records.SearchAsync(
                query,
                limit,
                offset,
                budget,
                task => MatchesFilter(task, filter),
                cancellationToken
            );

        var (candidates, truncated) = await ReadTaskCandidatesAsync(
            query,
            budget,
            cancellationToken
        );
        var page = OrderTasks(
                candidates.Where(task => MatchesFilter(task, filter)),
                oldestFirst: false
            )
            .DistinctBy(task => task.TaskId)
            .Skip(offset)
            .Take(limit)
            .ToList();
        return (page, truncated);
    }

    private static bool MatchesFilter(TaskSummary task, TaskFilter filter) =>
        (
            string.IsNullOrEmpty(filter.TaskNamePattern)
            || MatchesPattern(task.TaskName, filter.TaskNamePattern)
        )
        && (!filter.CompletedAfter.HasValue || task.CompletedAt >= filter.CompletedAfter)
        && (!filter.CompletedBefore.HasValue || task.CompletedAt <= filter.CompletedBefore);

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
        var limit = Math.Min(_options.MaxBulkOperationSize, FilterScanLimit);
        if (limit <= 0)
            return failedTasks;
        await foreach (var dlm in _deadLetterStore.GetAllAsync(limit, 0, cancellationToken))
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
}
