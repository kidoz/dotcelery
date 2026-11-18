using DotCelery.Core.Abstractions;
using DotCelery.Core.Dashboard;
using DotCelery.Core.Models;
using DotCelery.Dashboard.Models;
using DotCelery.Dashboard.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace DotCelery.Dashboard.Controllers;

public partial class DashboardApiController
{
    private readonly TaskRecordStore? _taskRecords;

    /// <summary>Creates the dashboard API with its indexed task history.</summary>
    [ActivatorUtilitiesConstructor]
    public DashboardApiController(
        TaskRecordStore taskRecords,
        IDashboardDataProvider dataProvider,
        IOptions<DashboardOptions> options,
        IRevocationStore? revocationStore = null,
        IDelayedMessageStore? delayedMessageStore = null,
        IHistoricalDataStore? historicalDataStore = null,
        IDeadLetterStore? deadLetterStore = null
    )
        : this(
            dataProvider,
            options,
            revocationStore,
            delayedMessageStore,
            historicalDataStore,
            deadLetterStore
        )
    {
        _taskRecords = taskRecords;
    }

    /// <summary>Gets the features available to this dashboard and its data retention.</summary>
    [HttpGet("capabilities")]
    public ActionResult GetCapabilities() =>
        Ok(
            new
            {
                ReadOnly = _options.ReadOnly || !_options.AllowTaskOperations,
                CanCancel = !_options.ReadOnly
                    && _options.AllowTaskOperations
                    && _revocationStore is not null,
                CanRetry = !_options.ReadOnly
                    && _options.AllowTaskOperations
                    && _deadLetterStore is not null,
                HasHistory = _historicalDataStore is not null,
                HasTaskRecords = _taskRecords?.IsAvailable ?? true,
                RetentionSeconds = _options.TaskRecordRetention.TotalSeconds,
                SnapshotSeconds = _options.MetricsSnapshotInterval.TotalSeconds,
                RefreshSeconds = Math.Max(1, _options.RefreshIntervalSeconds),
                MetricsWindowMinutes = _options.MetricsWindow.TotalMinutes,
                PageSize = Math.Clamp(_options.PageSize, 1, 1000),
            }
        );

    /// <summary>Searches all retained task states, combining filters before pagination.</summary>
    [HttpGet("tasks/search")]
    public async Task<ActionResult<TaskListResponse>> SearchTasks(
        [FromQuery] TaskSearchRequest query,
        CancellationToken cancellationToken = default
    )
    {
        if (query.State.HasValue && !Enum.IsDefined(query.State.Value))
            return BadRequest("Unknown task state.");
        if (query.From.HasValue && query.Until.HasValue && query.From >= query.Until)
            return BadRequest("The start of the range must be before its end.");
        var (page, pageSize, offset) = ClampPaging(query.Page, query.PageSize);
        if (offset >= FilterScanLimit)
            return BadRequest("The requested page starts beyond the task search scan limit.");
        var (tasks, truncated) = await QueryTasksAsync(
            query,
            pageSize + 1,
            offset,
            cancellationToken
        );
        return Ok(
            new TaskListResponse
            {
                Tasks = tasks.Take(pageSize).ToList(),
                Page = page,
                PageSize = pageSize,
                HasMore = truncated || tasks.Count > pageSize,
                Truncated = truncated,
            }
        );
    }

    /// <summary>Previews which explicitly selected tasks can be cancelled or retried.</summary>
    [HttpPost("tasks/actions/preview")]
    public async Task<ActionResult> PreviewActions(
        [FromBody] BulkRetryRequest request,
        CancellationToken cancellationToken = default
    )
    {
        if (ExceedsBulkLimit(request.TaskIds) is { } error)
            return error;
        var ids = request.TaskIds?.Distinct(StringComparer.Ordinal).ToList() ?? [];
        var cancel = new List<string>();
        var retry = new List<string>();
        if (!_options.ReadOnly && _options.AllowTaskOperations)
        {
            if (_revocationStore is not null)
            {
                var taskRecords = new TaskSummary?[ids.Count];
                if (
                    _taskRecords is { IsAvailable: true } records
                    && _dataProvider is DashboardDataService
                )
                    await Parallel.ForEachAsync(
                        Enumerable.Range(0, ids.Count),
                        new ParallelOptions
                        {
                            MaxDegreeOfParallelism = 8,
                            CancellationToken = cancellationToken,
                        },
                        async (index, ct) =>
                            taskRecords[index] = await records.GetAsync(ids[index], ct)
                    );

                for (var index = 0; index < ids.Count; index++)
                {
                    var id = ids[index];
                    var task =
                        taskRecords[index]
                        ?? await _dataProvider.GetTaskAsync(id, cancellationToken);
                    if (
                        task?.State
                        is "Pending"
                            or "Received"
                            or "Started"
                            or "Progress"
                            or "Retry"
                            or "Requeued"
                    )
                        cancel.Add(id);
                }
            }
            if (_deadLetterStore is not null)
                retry.AddRange((await ResolveRetryMessagesAsync(ids, cancellationToken)).Keys);
        }
        return Ok(new { CancelTaskIds = cancel, RetryTaskIds = retry });
    }

    private async ValueTask<(IReadOnlyList<TaskSummary> Tasks, bool Truncated)> QueryTasksAsync(
        TaskSearchRequest query,
        int limit,
        int offset,
        CancellationToken cancellationToken
    )
    {
        if (_taskRecords is { IsAvailable: true } records)
            return await records.SearchAsync(
                query,
                limit,
                offset,
                FilterScanLimit,
                null,
                cancellationToken
            );

        var (candidates, truncated) = await ReadTaskCandidatesAsync(
            query,
            FilterScanLimit,
            cancellationToken
        );
        return (
            OrderTasks(candidates, query.OldestFirst)
                .DistinctBy(t => t.TaskId)
                .Skip(offset)
                .Take(limit)
                .ToList(),
            truncated
        );
    }

    private async Task<(List<TaskSummary> Candidates, bool Truncated)> ReadTaskCandidatesAsync(
        TaskSearchRequest query,
        int budget,
        CancellationToken cancellationToken
    )
    {
        var candidates = new List<TaskSummary>();
        var states = query.State.HasValue ? [query.State.Value] : Enum.GetValues<TaskState>();
        var remaining = new Queue<(TaskState State, int Offset)>(
            states.Select(state => (state, 0))
        );
        while (budget > 0 && remaining.TryDequeue(out var next))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var count = Math.Min(100, budget);
            var batch = await _dataProvider.GetTasksByStateAsync(
                next.State,
                count,
                next.Offset,
                cancellationToken
            );
            candidates.AddRange(batch.Take(count).Where(query.Matches));
            budget -= Math.Min(count, batch.Count);
            if (batch.Count >= count)
                remaining.Enqueue((next.State, next.Offset + count));
        }
        return (candidates, remaining.Count > 0);
    }

    private static IOrderedEnumerable<TaskSummary> OrderTasks(
        IEnumerable<TaskSummary> tasks,
        bool oldestFirst
    )
    {
        static DateTimeOffset Updated(TaskSummary t) =>
            t.CompletedAt ?? t.StartedAt ?? t.SentAt ?? DateTimeOffset.MinValue;
        return oldestFirst
            ? tasks.OrderBy(Updated).ThenBy(t => t.TaskId, StringComparer.Ordinal)
            : tasks
                .OrderByDescending(Updated)
                .ThenByDescending(t => t.TaskId, StringComparer.Ordinal);
    }

    private async Task<Dictionary<string, string>> ResolveRetryMessagesAsync(
        IEnumerable<string> taskIds,
        CancellationToken cancellationToken
    )
    {
        var remaining = taskIds.ToHashSet(StringComparer.Ordinal);
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        if (_deadLetterStore is null || remaining.Count == 0)
            return result;
        var offset = 0;
        while (remaining.Count > 0)
        {
            var count = 0;
            await foreach (
                var message in _deadLetterStore.GetAllAsync(100, offset, cancellationToken)
            )
            {
                count++;
                if (remaining.Remove(message.TaskId))
                    result[message.TaskId] = message.Id;
                // Also accept a dead-letter entry ID for compatibility with existing clients.
                if (remaining.Remove(message.Id))
                    result[message.Id] = message.Id;
            }
            if (count < 100)
                break;
            offset = checked(offset + count);
        }
        return result;
    }
}
