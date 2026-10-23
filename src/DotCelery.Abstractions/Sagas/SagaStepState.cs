namespace DotCelery.Core.Sagas;

/// <summary>
/// Represents the state of an individual saga step.
/// </summary>
public enum SagaStepState
{
    /// <summary>
    /// Step is pending execution.
    /// </summary>
    Pending,

    /// <summary>
    /// Step is currently executing.
    /// </summary>
    Executing,

    /// <summary>
    /// Step completed successfully.
    /// </summary>
    Completed,

    /// <summary>
    /// Step failed.
    /// </summary>
    Failed,

    /// <summary>
    /// Step is being compensated.
    /// </summary>
    Compensating,

    /// <summary>
    /// Step was compensated successfully.
    /// </summary>
    Compensated,

    /// <summary>
    /// Step compensation failed.
    /// </summary>
    CompensationFailed,

    /// <summary>
    /// Step was skipped.
    /// </summary>
    Skipped,
}
