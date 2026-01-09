namespace DotCelery.Core.Progress;

/// <summary>
/// Allows tasks to report incremental progress during execution.
/// </summary>
public interface IProgressReporter
{
    /// <summary>
    /// Reports progress for the current task.
    /// </summary>
    /// <param name="percentage">Progress percentage (0-100). Values outside this range are clamped.</param>
    /// <param name="message">Optional status message.</param>
    /// <param name="data">Optional custom data.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask ReportAsync(
        double percentage,
        string? message = null,
        IReadOnlyDictionary<string, object>? data = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Reports progress with a ProgressInfo object.
    /// </summary>
    /// <param name="progress">The progress information.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask ReportAsync(ProgressInfo progress, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reports progress with item counts.
    /// </summary>
    /// <param name="itemsProcessed">Number of items processed.</param>
    /// <param name="totalItems">Total number of items.</param>
    /// <param name="message">Optional status message.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask ReportItemsAsync(
        long itemsProcessed,
        long totalItems,
        string? message = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Reports step progress.
    /// </summary>
    /// <param name="currentStep">Current step number (1-based).</param>
    /// <param name="totalSteps">Total number of steps.</param>
    /// <param name="stepName">Name of the current step.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask ReportStepAsync(
        int currentStep,
        int totalSteps,
        string? stepName = null,
        CancellationToken cancellationToken = default
    );
}
