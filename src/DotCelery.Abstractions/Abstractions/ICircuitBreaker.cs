using DotCelery.Core.Resilience;

namespace DotCelery.Core.Abstractions;

/// <summary>
/// Circuit breaker for protecting endpoints during failures.
/// Prevents cascading failures by temporarily disabling consumption.
/// </summary>
public interface ICircuitBreaker
{
    /// <summary>
    /// Gets the name of this circuit breaker.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Gets the current circuit state.
    /// </summary>
    CircuitState State { get; }

    /// <summary>
    /// Gets whether the circuit is currently allowing requests.
    /// </summary>
    bool IsAllowed { get; }

    /// <summary>
    /// Gets the time when the circuit was last opened.
    /// </summary>
    DateTimeOffset? LastOpenedAt { get; }

    /// <summary>
    /// Gets the number of consecutive failures.
    /// </summary>
    int FailureCount { get; }

    /// <summary>
    /// Gets the number of consecutive successes (used in half-open state).
    /// </summary>
    int SuccessCount { get; }

    /// <summary>
    /// Records a successful operation.
    /// </summary>
    void RecordSuccess();

    /// <summary>
    /// Records a failed operation.
    /// </summary>
    /// <param name="exception">The exception that caused the failure.</param>
    void RecordFailure(Exception? exception = null);

    /// <summary>
    /// Attempts to execute an operation through the circuit breaker.
    /// </summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="operation">The operation to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The operation result.</returns>
    /// <exception cref="CircuitBreakerOpenException">When the circuit is open.</exception>
    ValueTask<T> ExecuteAsync<T>(
        Func<CancellationToken, ValueTask<T>> operation,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Manually opens the circuit.
    /// </summary>
    void Trip();

    /// <summary>
    /// Manually resets the circuit to closed state.
    /// </summary>
    void Reset();

    /// <summary>
    /// Event raised when the circuit state changes.
    /// </summary>
    event EventHandler<CircuitStateChangedEventArgs>? StateChanged;
}

/// <summary>
/// Factory for creating queue-specific circuit breakers.
/// </summary>
public interface ICircuitBreakerFactory
{
    /// <summary>
    /// Gets or creates a circuit breaker for the specified queue.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <returns>The circuit breaker instance.</returns>
    ICircuitBreaker GetOrCreate(string queueName);

    /// <summary>
    /// Gets the global circuit breaker (for infrastructure failures).
    /// </summary>
    ICircuitBreaker GlobalCircuitBreaker { get; }

    /// <summary>
    /// Gets all circuit breakers that have been created.
    /// </summary>
    IReadOnlyCollection<ICircuitBreaker> All { get; }
}
