namespace DotCelery.Core.Sagas;

/// <summary>
/// Storage interface for saga tracking.
/// Follows the pattern established by IBatchStore.
/// </summary>
public interface ISagaStore : IAsyncDisposable
{
    /// <summary>
    /// Creates a new saga.
    /// </summary>
    /// <param name="saga">The saga to create.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask CreateAsync(Saga saga, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a saga by ID.
    /// </summary>
    /// <param name="sagaId">The saga ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The saga or null if not found.</returns>
    ValueTask<Saga?> GetAsync(string sagaId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Updates the saga state.
    /// </summary>
    /// <param name="sagaId">The saga ID.</param>
    /// <param name="state">The new state.</param>
    /// <param name="failureReason">Optional failure reason.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The updated saga or null if not found.</returns>
    ValueTask<Saga?> UpdateStateAsync(
        string sagaId,
        SagaState state,
        string? failureReason = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Updates a specific step's state.
    /// </summary>
    /// <param name="sagaId">The saga ID.</param>
    /// <param name="stepId">The step ID.</param>
    /// <param name="state">The new step state.</param>
    /// <param name="taskId">The task ID for this step execution.</param>
    /// <param name="compensateTaskId">The task ID for this step compensation.</param>
    /// <param name="result">Optional step result.</param>
    /// <param name="errorMessage">Optional error message.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The updated saga or null if not found.</returns>
    ValueTask<Saga?> UpdateStepStateAsync(
        string sagaId,
        string stepId,
        SagaStepState state,
        string? taskId = null,
        string? compensateTaskId = null,
        object? result = null,
        string? errorMessage = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Records step compensation status.
    /// </summary>
    /// <param name="sagaId">The saga ID.</param>
    /// <param name="stepId">The step ID.</param>
    /// <param name="success">Whether compensation succeeded.</param>
    /// <param name="compensateTaskId">The compensation task ID.</param>
    /// <param name="errorMessage">Optional error message.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The updated saga or null if not found.</returns>
    ValueTask<Saga?> MarkStepCompensatedAsync(
        string sagaId,
        string stepId,
        bool success,
        string? compensateTaskId = null,
        string? errorMessage = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Advances to the next step.
    /// </summary>
    /// <param name="sagaId">The saga ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The updated saga or null if not found.</returns>
    ValueTask<Saga?> AdvanceStepAsync(string sagaId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a saga.
    /// </summary>
    /// <param name="sagaId">The saga ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if deleted, false if not found.</returns>
    ValueTask<bool> DeleteAsync(string sagaId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the saga ID for a task (if the task is a saga step).
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The saga ID or null if not found.</returns>
    ValueTask<string?> GetSagaIdForTaskAsync(
        string taskId,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets sagas in a specific state (for monitoring/recovery).
    /// </summary>
    /// <param name="state">The state to filter by.</param>
    /// <param name="limit">Maximum number of sagas to return.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Stream of sagas in the specified state.</returns>
    IAsyncEnumerable<Saga> GetByStateAsync(
        SagaState state,
        int limit = 100,
        CancellationToken cancellationToken = default
    );
}
