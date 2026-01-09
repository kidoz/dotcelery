using DotCelery.Core.Resilience;

namespace DotCelery.Core.Abstractions;

/// <summary>
/// Kill switch that automatically stops consuming new messages when a failure threshold is reached.
/// Inspired by MassTransit's kill switch for endpoint protection.
/// </summary>
public interface IKillSwitch
{
    /// <summary>
    /// Gets the current state of the kill switch.
    /// </summary>
    KillSwitchState State { get; }

    /// <summary>
    /// Gets whether consuming is currently paused.
    /// </summary>
    bool IsPaused { get; }

    /// <summary>
    /// Gets the time when the kill switch was last tripped.
    /// </summary>
    DateTimeOffset? LastTrippedAt { get; }

    /// <summary>
    /// Gets the current failure rate (0.0 to 1.0).
    /// </summary>
    double CurrentFailureRate { get; }

    /// <summary>
    /// Gets the total number of messages tracked in the current window.
    /// </summary>
    int TrackedMessageCount { get; }

    /// <summary>
    /// Records a successful task execution.
    /// </summary>
    void RecordSuccess();

    /// <summary>
    /// Records a failed task execution.
    /// </summary>
    /// <param name="exception">The exception that caused the failure.</param>
    void RecordFailure(Exception? exception = null);

    /// <summary>
    /// Manually resets the kill switch to allow consuming to resume.
    /// </summary>
    void Reset();

    /// <summary>
    /// Waits until consuming is allowed (kill switch not tripped).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when consuming is allowed.</returns>
    ValueTask WaitUntilReadyAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Event raised when the kill switch state changes.
    /// </summary>
    event EventHandler<KillSwitchStateChangedEventArgs>? StateChanged;
}
