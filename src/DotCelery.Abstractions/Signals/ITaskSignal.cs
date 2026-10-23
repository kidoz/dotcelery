namespace DotCelery.Core.Signals;

/// <summary>
/// Marker interface for task lifecycle signals.
/// </summary>
public interface ITaskSignal
{
    /// <summary>
    /// Gets the task ID.
    /// </summary>
    string TaskId { get; }

    /// <summary>
    /// Gets the task name.
    /// </summary>
    string TaskName { get; }

    /// <summary>
    /// Gets the timestamp when the signal was created.
    /// </summary>
    DateTimeOffset Timestamp { get; }
}

/// <summary>
/// Handler for task lifecycle signals.
/// </summary>
/// <typeparam name="TSignal">The signal type.</typeparam>
public interface ITaskSignalHandler<in TSignal>
    where TSignal : ITaskSignal
{
    /// <summary>
    /// Handles the signal.
    /// </summary>
    /// <param name="signal">The signal.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask HandleAsync(TSignal signal, CancellationToken cancellationToken = default);
}

/// <summary>
/// Dispatches task signals to registered handlers.
/// </summary>
public interface ITaskSignalDispatcher
{
    /// <summary>
    /// Dispatches a signal to all registered handlers.
    /// </summary>
    /// <typeparam name="TSignal">The signal type.</typeparam>
    /// <param name="signal">The signal.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask DispatchAsync<TSignal>(TSignal signal, CancellationToken cancellationToken = default)
        where TSignal : ITaskSignal;
}
