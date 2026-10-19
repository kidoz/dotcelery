using DotCelery.Core.Dashboard;
using DotCelery.Core.Models;
using DotCelery.Core.Signals;
using DotCelery.Dashboard.Hubs;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Dashboard.Services;

/// <summary>
/// Records the tasks the host runs into the <see cref="TaskRecordStore"/> and, when real-time
/// updates are enabled, notifies the dashboard's SignalR clients about them.
/// </summary>
/// <remarks>
/// The executor dispatches a <see cref="TaskPreRunSignal"/> when a task starts and a
/// <see cref="TaskPostRunSignal"/> with the outcome when it settles; the failure and rejection
/// signals arrive before the post-run signal and enrich the record with the error.
/// </remarks>
public sealed class DashboardTaskSignalHandler
    : ITaskSignalHandler<TaskPreRunSignal>,
        ITaskSignalHandler<TaskPostRunSignal>,
        ITaskSignalHandler<TaskFailureSignal>,
        ITaskSignalHandler<TaskRejectedSignal>
{
    private readonly TaskRecordStore _records;
    private readonly TaskMetricsStore? _taskMetrics;
    private readonly DashboardNotificationService? _notifications;
    private readonly DashboardOptions _options;
    private readonly ILogger<DashboardTaskSignalHandler> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="DashboardTaskSignalHandler"/> class.
    /// </summary>
    /// <param name="records">The task records.</param>
    /// <param name="options">The dashboard options.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="notifications">The SignalR notifications, when the hub is mapped.</param>
    /// <param name="taskMetrics">The task metrics the settled tasks are counted in, when one is registered.</param>
    public DashboardTaskSignalHandler(
        TaskRecordStore records,
        IOptions<DashboardOptions> options,
        ILogger<DashboardTaskSignalHandler> logger,
        DashboardNotificationService? notifications = null,
        TaskMetricsStore? taskMetrics = null
    )
    {
        _records = records;
        _options = options.Value;
        _logger = logger;
        _notifications = notifications;
        _taskMetrics = taskMetrics;
    }

    /// <inheritdoc />
    public async ValueTask HandleAsync(
        TaskPreRunSignal signal,
        CancellationToken cancellationToken = default
    )
    {
        await UpdateAsync(
                signal.TaskId,
                existing =>
                    Merge(
                        existing,
                        signal.TaskName,
                        new TaskSummary
                        {
                            TaskId = signal.TaskId,
                            TaskName = signal.TaskName,
                            State = TaskState.Started.ToString(),
                            Worker = signal.Worker,
                            StartedAt = signal.Timestamp,
                            Retries = signal.RetryCount,
                        }
                    ),
                cancellationToken
            )
            .ConfigureAwait(false);

        await NotifyAsync(signal.TaskId, TaskState.Started.ToString(), cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask HandleAsync(
        TaskPostRunSignal signal,
        CancellationToken cancellationToken = default
    )
    {
        var completedAt = TaskStateValidator.IsTerminal(signal.State)
            ? signal.Timestamp
            : (DateTimeOffset?)null;

        await UpdateAsync(
                signal.TaskId,
                existing =>
                    Merge(
                        existing,
                        signal.TaskName,
                        new TaskSummary
                        {
                            TaskId = signal.TaskId,
                            TaskName = signal.TaskName,
                            State = signal.State.ToString(),
                            Worker = signal.Worker,
                            CompletedAt = completedAt,
                            Duration = signal.Duration,
                        }
                    ),
                cancellationToken
            )
            .ConfigureAwait(false);

        // Only a terminal state settled the task; a retry runs it again and is counted then
        if (completedAt is not null && _taskMetrics is not null)
        {
            await _taskMetrics
                .RecordCompletedAsync(
                    signal.TaskName,
                    success: signal.State == TaskState.Success,
                    signal.Duration,
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        await NotifyAsync(signal.TaskId, signal.State.ToString(), cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public ValueTask HandleAsync(
        TaskFailureSignal signal,
        CancellationToken cancellationToken = default
    ) =>
        UpdateAsync(
            signal.TaskId,
            existing =>
                Merge(
                    existing,
                    signal.TaskName,
                    new TaskSummary
                    {
                        TaskId = signal.TaskId,
                        TaskName = signal.TaskName,
                        State = TaskState.Failure.ToString(),
                        Worker = signal.Worker,
                        Retries = signal.RetryCount,
                        ExceptionMessage = TaskExceptionSanitizer.Sanitize(
                            signal.Exception?.GetType().FullName,
                            signal.Exception?.Message,
                            _options
                        ),
                    }
                ),
            cancellationToken
        );

    /// <inheritdoc />
    public ValueTask HandleAsync(
        TaskRejectedSignal signal,
        CancellationToken cancellationToken = default
    ) =>
        UpdateAsync(
            signal.TaskId,
            existing =>
                Merge(
                    existing,
                    signal.TaskName,
                    new TaskSummary
                    {
                        TaskId = signal.TaskId,
                        TaskName = signal.TaskName,
                        State = TaskState.Rejected.ToString(),
                        Worker = signal.Worker,
                        ExceptionMessage = $"Rejected: {signal.Reason}",
                    }
                ),
            cancellationToken
        );

    private async ValueTask NotifyAsync(
        string taskId,
        string state,
        CancellationToken cancellationToken
    )
    {
        if (_notifications is null || !_options.EnableRealTimeUpdates)
        {
            return;
        }

        try
        {
            await _notifications.NotifyTaskStateChangedAsync(taskId, state).ConfigureAwait(false);
            await _notifications.NotifyOverviewChangedAsync().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A dashboard that cannot reach its clients must not fail the task
            _logger.LogWarning(
                ex,
                "Failed to notify dashboard clients about task {TaskId}",
                taskId
            );
        }
    }

    private async ValueTask UpdateAsync(
        string taskId,
        Func<TaskSummary?, TaskSummary> update,
        CancellationToken cancellationToken
    )
    {
        if (!_records.IsAvailable)
        {
            return;
        }

        var existing = await _records.GetAsync(taskId, cancellationToken).ConfigureAwait(false);

        await _records.RecordAsync(update(existing), cancellationToken).ConfigureAwait(false);
    }

    // The updates of one task arrive as separate signals, so each one keeps what the previous
    // ones stored and replaces only the fields it carries
    private static TaskSummary Merge(TaskSummary? existing, string taskName, TaskSummary update) =>
        new()
        {
            TaskId = update.TaskId,
            TaskName = taskName,
            State = update.State,
            Queue = existing?.Queue,
            Worker = update.Worker ?? existing?.Worker,
            SentAt = existing?.SentAt,
            StartedAt = update.StartedAt ?? existing?.StartedAt,
            CompletedAt = update.CompletedAt ?? existing?.CompletedAt,
            Duration = update.Duration ?? existing?.Duration,
            Retries = update.Retries != 0 ? update.Retries : existing?.Retries ?? 0,
            ExceptionMessage = update.ExceptionMessage ?? existing?.ExceptionMessage,
            Eta = existing?.Eta,
        };
}
