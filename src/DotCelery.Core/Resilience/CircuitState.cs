namespace DotCelery.Core.Resilience;

/// <summary>
/// Circuit breaker states following the standard pattern.
/// </summary>
public enum CircuitState
{
    /// <summary>
    /// Circuit is closed - operations allowed.
    /// </summary>
    Closed,

    /// <summary>
    /// Circuit is open - operations blocked.
    /// </summary>
    Open,

    /// <summary>
    /// Circuit is half-open - testing if operations succeed.
    /// </summary>
    HalfOpen,
}
