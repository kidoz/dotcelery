using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Progress;
using DotCelery.Core.Signals;

namespace DotCelery.Worker.Progress;

/// <summary>
/// Default implementation of <see cref="IProgressReporter"/> that updates
/// result backend state and dispatches progress signals.
/// </summary>
public sealed class ProgressReporter : IProgressReporter
{
    private readonly string _taskId;
    private readonly string _taskName;
    private readonly IResultBackend _resultBackend;
    private readonly ITaskSignalDispatcher? _signalDispatcher;
    private readonly string? _workerName;

    /// <summary>
    /// Initializes a new instance of the <see cref="ProgressReporter"/> class.
    /// </summary>
    public ProgressReporter(
        string taskId,
        string taskName,
        IResultBackend resultBackend,
        ITaskSignalDispatcher? signalDispatcher = null,
        string? workerName = null
    )
    {
        _taskId = taskId;
        _taskName = taskName;
        _resultBackend = resultBackend;
        _signalDispatcher = signalDispatcher;
        _workerName = workerName;
    }

    /// <inheritdoc />
    public async ValueTask ReportAsync(
        double percentage,
        string? message = null,
        IReadOnlyDictionary<string, object>? data = null,
        CancellationToken cancellationToken = default
    )
    {
        var progress = new ProgressInfo
        {
            Percentage = Math.Clamp(percentage, 0, 100),
            Message = message,
            Data = data,
            Timestamp = DateTimeOffset.UtcNow,
        };

        await ReportAsync(progress, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask ReportAsync(
        ProgressInfo progress,
        CancellationToken cancellationToken = default
    )
    {
        // Ensure percentage is clamped
        var clampedProgress = progress with
        {
            Percentage = Math.Clamp(progress.Percentage, 0, 100),
        };

        // Update state in result backend with progress metadata
        await _resultBackend
            .UpdateStateAsync(_taskId, TaskState.Progress, clampedProgress, cancellationToken)
            .ConfigureAwait(false);

        // Dispatch signal for real-time updates (dashboard, SignalR, etc.)
        if (_signalDispatcher is not null)
        {
            await _signalDispatcher
                .DispatchAsync(
                    new ProgressUpdatedSignal
                    {
                        TaskId = _taskId,
                        TaskName = _taskName,
                        Timestamp = clampedProgress.Timestamp,
                        Progress = clampedProgress,
                        Worker = _workerName,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public ValueTask ReportItemsAsync(
        long itemsProcessed,
        long totalItems,
        string? message = null,
        CancellationToken cancellationToken = default
    )
    {
        var percentage = totalItems > 0 ? itemsProcessed * 100.0 / totalItems : 0;

        var progress = new ProgressInfo
        {
            Percentage = Math.Clamp(percentage, 0, 100),
            Message = message,
            ItemsProcessed = itemsProcessed,
            TotalItems = totalItems,
            Timestamp = DateTimeOffset.UtcNow,
        };

        return ReportAsync(progress, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask ReportStepAsync(
        int currentStep,
        int totalSteps,
        string? stepName = null,
        CancellationToken cancellationToken = default
    )
    {
        var percentage = totalSteps > 0 ? currentStep * 100.0 / totalSteps : 0;

        var progress = new ProgressInfo
        {
            Percentage = Math.Clamp(percentage, 0, 100),
            Message = stepName,
            CurrentStep = $"{currentStep} of {totalSteps}",
            Timestamp = DateTimeOffset.UtcNow,
        };

        return ReportAsync(progress, cancellationToken);
    }
}
