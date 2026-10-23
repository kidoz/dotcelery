using DotCelery.Core.Canvas;

namespace DotCelery.Core.Sagas;

/// <summary>
/// Represents a single step in a saga.
/// </summary>
public sealed record SagaStep
{
    /// <summary>
    /// Gets the unique step identifier.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Gets the step name/description.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Gets the step order (0-based).
    /// </summary>
    public required int Order { get; init; }

    /// <summary>
    /// Gets the execution task signature.
    /// </summary>
    public required Signature ExecuteTask { get; init; }

    /// <summary>
    /// Gets the compensation task signature (optional).
    /// If null, this step cannot be compensated.
    /// </summary>
    public Signature? CompensateTask { get; init; }

    /// <summary>
    /// Gets the current step state.
    /// </summary>
    public SagaStepState State { get; init; } = SagaStepState.Pending;

    /// <summary>
    /// Gets the execution task ID (once started).
    /// </summary>
    public string? ExecuteTaskId { get; init; }

    /// <summary>
    /// Gets the compensation task ID (if compensating).
    /// </summary>
    public string? CompensateTaskId { get; init; }

    /// <summary>
    /// Gets the step result (if completed).
    /// </summary>
    public object? Result { get; init; }

    /// <summary>
    /// Gets the error message (if failed).
    /// </summary>
    public string? Error { get; init; }

    /// <summary>
    /// Gets when the step started.
    /// </summary>
    public DateTimeOffset? StartedAt { get; init; }

    /// <summary>
    /// Gets when the step completed.
    /// </summary>
    public DateTimeOffset? CompletedAt { get; init; }

    /// <summary>
    /// Gets whether this step requires compensation if subsequent steps fail.
    /// </summary>
    public bool RequiresCompensation => CompensateTask is not null;

    /// <summary>
    /// Gets whether this step has been compensated.
    /// </summary>
    public bool IsCompensated =>
        State is SagaStepState.Compensated or SagaStepState.CompensationFailed;

    /// <summary>
    /// Gets whether this step is in a terminal state.
    /// </summary>
    public bool IsTerminal =>
        State
            is SagaStepState.Completed
                or SagaStepState.Failed
                or SagaStepState.Compensated
                or SagaStepState.CompensationFailed
                or SagaStepState.Skipped;
}
