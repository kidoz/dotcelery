namespace DotCelery.Core.Resilience;

/// <summary>
/// Event args for kill switch state changes.
/// </summary>
public sealed class KillSwitchStateChangedEventArgs : EventArgs
{
    /// <summary>
    /// Gets the previous state.
    /// </summary>
    public required KillSwitchState OldState { get; init; }

    /// <summary>
    /// Gets the new state.
    /// </summary>
    public required KillSwitchState NewState { get; init; }

    /// <summary>
    /// Gets when the state change occurred.
    /// </summary>
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the exception that triggered the state change (if applicable).
    /// </summary>
    public Exception? TriggeringException { get; init; }

    /// <summary>
    /// Gets the current failure count when the state changed.
    /// </summary>
    public int FailureCount { get; init; }

    /// <summary>
    /// Gets the current failure rate when the state changed.
    /// </summary>
    public double FailureRate { get; init; }
}
