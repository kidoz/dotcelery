using DotCelery.Core.Signals;

namespace DotCelery.Core.Sagas;

/// <summary>
/// Signal emitted when a saga's state changes.
/// </summary>
public sealed record SagaStateChangedSignal : ITaskSignal
{
    /// <inheritdoc />
    public required string TaskId { get; init; } // Saga ID

    /// <inheritdoc />
    public required string TaskName { get; init; } // Saga name

    /// <inheritdoc />
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the previous state.
    /// </summary>
    public required SagaState OldState { get; init; }

    /// <summary>
    /// Gets the new state.
    /// </summary>
    public required SagaState NewState { get; init; }

    /// <summary>
    /// Gets the reason for the state change (if applicable).
    /// </summary>
    public string? Reason { get; init; }

    /// <summary>
    /// Gets the current step index.
    /// </summary>
    public int CurrentStepIndex { get; init; }

    /// <summary>
    /// Gets the saga progress percentage.
    /// </summary>
    public double Progress { get; init; }
}

/// <summary>
/// Signal emitted when a saga step completes.
/// </summary>
public sealed record SagaStepCompletedSignal : ITaskSignal
{
    /// <inheritdoc />
    public required string TaskId { get; init; } // Step task ID

    /// <inheritdoc />
    public required string TaskName { get; init; } // Step name

    /// <inheritdoc />
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the saga ID.
    /// </summary>
    public required string SagaId { get; init; }

    /// <summary>
    /// Gets the step ID.
    /// </summary>
    public required string StepId { get; init; }

    /// <summary>
    /// Gets the step order.
    /// </summary>
    public int StepOrder { get; init; }

    /// <summary>
    /// Gets the step state.
    /// </summary>
    public required SagaStepState State { get; init; }

    /// <summary>
    /// Gets the step result (if successful).
    /// </summary>
    public object? Result { get; init; }

    /// <summary>
    /// Gets the error message (if failed).
    /// </summary>
    public string? Error { get; init; }

    /// <summary>
    /// Gets the execution duration.
    /// </summary>
    public TimeSpan Duration { get; init; }
}

/// <summary>
/// Signal emitted when a saga step is compensated.
/// </summary>
public sealed record SagaStepCompensatedSignal : ITaskSignal
{
    /// <inheritdoc />
    public required string TaskId { get; init; } // Compensation task ID

    /// <inheritdoc />
    public required string TaskName { get; init; } // Step name

    /// <inheritdoc />
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the saga ID.
    /// </summary>
    public required string SagaId { get; init; }

    /// <summary>
    /// Gets the step ID.
    /// </summary>
    public required string StepId { get; init; }

    /// <summary>
    /// Gets the step order.
    /// </summary>
    public int StepOrder { get; init; }

    /// <summary>
    /// Gets whether compensation succeeded.
    /// </summary>
    public required bool Success { get; init; }

    /// <summary>
    /// Gets the error message (if failed).
    /// </summary>
    public string? Error { get; init; }
}

/// <summary>
/// Signal emitted when compensation begins for a saga.
/// </summary>
public sealed record SagaCompensationStartedSignal : ITaskSignal
{
    /// <inheritdoc />
    public required string TaskId { get; init; } // Saga ID

    /// <inheritdoc />
    public required string TaskName { get; init; } // Saga name

    /// <inheritdoc />
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the reason for compensation.
    /// </summary>
    public string? Reason { get; init; }

    /// <summary>
    /// Gets the number of steps to compensate.
    /// </summary>
    public int StepsToCompensate { get; init; }
}
