namespace DotCelery.Core.Sagas;

/// <summary>
/// Represents a saga - a long-running business process with compensating actions.
/// </summary>
public sealed record Saga
{
    /// <summary>
    /// Gets the unique saga identifier.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Gets the saga name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Gets the current saga state.
    /// </summary>
    public required SagaState State { get; init; }

    /// <summary>
    /// Gets the saga steps in order.
    /// </summary>
    public required IReadOnlyList<SagaStep> Steps { get; init; }

    /// <summary>
    /// Gets when the saga was created.
    /// </summary>
    public required DateTimeOffset CreatedAt { get; init; }

    /// <summary>
    /// Gets when the saga started executing.
    /// </summary>
    public DateTimeOffset? StartedAt { get; init; }

    /// <summary>
    /// Gets when the saga completed (success, failed, or compensated).
    /// </summary>
    public DateTimeOffset? CompletedAt { get; init; }

    /// <summary>
    /// Gets the correlation ID for tracing.
    /// </summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// Gets custom saga metadata.
    /// </summary>
    public IReadOnlyDictionary<string, object>? Metadata { get; init; }

    /// <summary>
    /// Gets the index of the current step being executed.
    /// </summary>
    public int CurrentStepIndex { get; init; }

    /// <summary>
    /// Gets the failure reason (if failed).
    /// </summary>
    public string? FailureReason { get; init; }

    // Computed properties (following Batch pattern)

    /// <summary>
    /// Gets the total number of steps.
    /// </summary>
    public int TotalSteps => Steps.Count;

    /// <summary>
    /// Gets the number of completed steps.
    /// </summary>
    public int CompletedSteps => Steps.Count(s => s.State == SagaStepState.Completed);

    /// <summary>
    /// Gets the number of failed steps.
    /// </summary>
    public int FailedSteps => Steps.Count(s => s.State == SagaStepState.Failed);

    /// <summary>
    /// Gets the number of compensated steps.
    /// </summary>
    public int CompensatedSteps => Steps.Count(s => s.State == SagaStepState.Compensated);

    /// <summary>
    /// Gets the number of pending steps.
    /// </summary>
    public int PendingSteps => Steps.Count(s => s.State == SagaStepState.Pending);

    /// <summary>
    /// Gets the progress percentage (0-100).
    /// </summary>
    public double Progress => TotalSteps > 0 ? CompletedSteps * 100.0 / TotalSteps : 0;

    /// <summary>
    /// Gets whether the saga has finished (any terminal state).
    /// </summary>
    public bool IsFinished =>
        State
            is SagaState.Completed
                or SagaState.Failed
                or SagaState.Compensated
                or SagaState.CompensationFailed
                or SagaState.Cancelled;

    /// <summary>
    /// Gets the current executing step (if any).
    /// </summary>
    public SagaStep? CurrentStep =>
        CurrentStepIndex >= 0 && CurrentStepIndex < Steps.Count ? Steps[CurrentStepIndex] : null;

    /// <summary>
    /// Gets the steps that need compensation (completed steps with compensation tasks, in reverse order).
    /// </summary>
    public IReadOnlyList<SagaStep> StepsToCompensate =>
        Steps
            .Where(s => s.State == SagaStepState.Completed && s.RequiresCompensation)
            .OrderByDescending(s => s.Order)
            .ToList();
}
