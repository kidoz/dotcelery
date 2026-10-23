namespace DotCelery.Core.Sagas;

/// <summary>
/// Represents the state of a saga.
/// </summary>
public enum SagaState
{
    /// <summary>
    /// Saga is being created.
    /// </summary>
    Created,

    /// <summary>
    /// Saga steps are being executed.
    /// </summary>
    Executing,

    /// <summary>
    /// Saga is compensating after a failure.
    /// </summary>
    Compensating,

    /// <summary>
    /// All saga steps completed successfully.
    /// </summary>
    Completed,

    /// <summary>
    /// Saga failed without compensation (no compensable steps).
    /// </summary>
    Failed,

    /// <summary>
    /// Saga was compensated successfully.
    /// </summary>
    Compensated,

    /// <summary>
    /// Compensation failed (requires manual intervention).
    /// </summary>
    CompensationFailed,

    /// <summary>
    /// Saga was cancelled.
    /// </summary>
    Cancelled,
}
