namespace DotCelery.Core.Sagas;

/// <summary>
/// Orchestrates saga execution with step coordination and compensation.
/// </summary>
public interface ISagaOrchestrator
{
    /// <summary>
    /// Starts a new saga execution.
    /// </summary>
    /// <param name="saga">The saga definition.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The created saga with ID.</returns>
    ValueTask<Saga> StartAsync(Saga saga, CancellationToken cancellationToken = default);

    /// <summary>
    /// Continues saga execution after a step completes.
    /// </summary>
    /// <param name="sagaId">The saga ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The updated saga.</returns>
    ValueTask<Saga?> ContinueAsync(string sagaId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Triggers compensation for a saga manually.
    /// </summary>
    /// <param name="sagaId">The saga ID.</param>
    /// <param name="reason">Reason for compensation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask CompensateAsync(
        string sagaId,
        string? reason = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets the current state of a saga.
    /// </summary>
    /// <param name="sagaId">The saga ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The saga or null if not found.</returns>
    ValueTask<Saga?> GetAsync(string sagaId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retries a failed saga from the last failed step.
    /// </summary>
    /// <param name="sagaId">The saga ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask RetryAsync(string sagaId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Cancels a saga that is currently executing.
    /// </summary>
    /// <param name="sagaId">The saga ID.</param>
    /// <param name="reason">Cancellation reason.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask CancelAsync(
        string sagaId,
        string? reason = null,
        CancellationToken cancellationToken = default
    );
}
