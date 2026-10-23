namespace DotCelery.Core.Resilience;

/// <summary>
/// Event args for circuit breaker state changes.
/// </summary>
public sealed class CircuitStateChangedEventArgs : EventArgs
{
    /// <summary>
    /// Gets the name of the circuit breaker.
    /// </summary>
    public required string CircuitName { get; init; }

    /// <summary>
    /// Gets the previous state.
    /// </summary>
    public required CircuitState OldState { get; init; }

    /// <summary>
    /// Gets the new state.
    /// </summary>
    public required CircuitState NewState { get; init; }

    /// <summary>
    /// Gets when the state change occurred.
    /// </summary>
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the exception that triggered the state change (if applicable).
    /// </summary>
    public Exception? TriggeringException { get; init; }

    /// <summary>
    /// Gets the current consecutive failure count.
    /// </summary>
    public int FailureCount { get; init; }
}
