namespace DotCelery.Core.Resilience;

/// <summary>
/// Exception thrown when an operation is attempted on an open circuit breaker.
/// </summary>
public sealed class CircuitBreakerOpenException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CircuitBreakerOpenException"/> class.
    /// </summary>
    public CircuitBreakerOpenException()
        : base("The circuit breaker is open and not allowing operations.") { }

    /// <summary>
    /// Initializes a new instance of the <see cref="CircuitBreakerOpenException"/> class.
    /// </summary>
    /// <param name="circuitName">The name of the circuit breaker.</param>
    public CircuitBreakerOpenException(string circuitName)
        : base($"The circuit breaker '{circuitName}' is open and not allowing operations.")
    {
        CircuitName = circuitName;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="CircuitBreakerOpenException"/> class.
    /// </summary>
    /// <param name="circuitName">The name of the circuit breaker.</param>
    /// <param name="retryAfter">When the circuit may allow operations again.</param>
    public CircuitBreakerOpenException(string circuitName, TimeSpan retryAfter)
        : base($"The circuit breaker '{circuitName}' is open. Retry after {retryAfter}.")
    {
        CircuitName = circuitName;
        RetryAfter = retryAfter;
    }

    /// <summary>
    /// Gets the name of the circuit breaker that is open.
    /// </summary>
    public string? CircuitName { get; }

    /// <summary>
    /// Gets the duration to wait before retrying.
    /// </summary>
    public TimeSpan? RetryAfter { get; }
}
