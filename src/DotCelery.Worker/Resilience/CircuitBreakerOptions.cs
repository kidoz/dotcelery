namespace DotCelery.Worker.Resilience;

/// <summary>
/// Configuration options for circuit breakers.
/// </summary>
public sealed class CircuitBreakerOptions
{
    /// <summary>
    /// Gets or sets the number of consecutive failures to open the circuit.
    /// Default: 5.
    /// </summary>
    public int FailureThreshold
    {
        get;
        set =>
            field =
                value > 0
                    ? value
                    : throw new ArgumentOutOfRangeException(
                        nameof(value),
                        "FailureThreshold must be positive."
                    );
    } = 5;

    /// <summary>
    /// Gets or sets the duration the circuit stays open before transitioning to half-open.
    /// Default: 30 seconds.
    /// </summary>
    public TimeSpan OpenDuration
    {
        get;
        set =>
            field =
                value > TimeSpan.Zero
                    ? value
                    : throw new ArgumentOutOfRangeException(
                        nameof(value),
                        "OpenDuration must be positive."
                    );
    } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the number of successful operations in half-open state
    /// required to close the circuit. Default: 3.
    /// </summary>
    public int SuccessThreshold
    {
        get;
        set =>
            field =
                value > 0
                    ? value
                    : throw new ArgumentOutOfRangeException(
                        nameof(value),
                        "SuccessThreshold must be positive."
                    );
    } = 3;

    /// <summary>
    /// Gets or sets the time window for counting failures.
    /// Failures outside this window are forgotten. Default: 1 minute.
    /// </summary>
    public TimeSpan FailureWindow
    {
        get;
        set =>
            field =
                value > TimeSpan.Zero
                    ? value
                    : throw new ArgumentOutOfRangeException(
                        nameof(value),
                        "FailureWindow must be positive."
                    );
    } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Gets or sets whether to use per-queue circuit breakers.
    /// If false, a single global circuit breaker is used. Default: true.
    /// </summary>
    public bool UsePerQueueCircuitBreakers { get; set; } = true;

    /// <summary>
    /// Gets or sets exception types that should trip the circuit.
    /// If empty, all exceptions trip the circuit.
    /// </summary>
    public IReadOnlyList<Type> TripOnExceptions
    {
        get;
        set => field = value ?? throw new ArgumentNullException(nameof(value));
    } = [];

    /// <summary>
    /// Gets or sets exception types to ignore.
    /// These exceptions will not affect the circuit state.
    /// </summary>
    public IReadOnlyList<Type> IgnoreExceptions
    {
        get;
        set => field = value ?? throw new ArgumentNullException(nameof(value));
    } = [];
}
