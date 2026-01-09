namespace DotCelery.Core.Batches;

/// <summary>
/// Storage interface for batch tracking.
/// </summary>
public interface IBatchStore : IAsyncDisposable
{
    /// <summary>
    /// Creates a new batch.
    /// </summary>
    /// <param name="batch">The batch to create.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask CreateAsync(Batch batch, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a batch by ID.
    /// </summary>
    /// <param name="batchId">The batch ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The batch, or null if not found.</returns>
    ValueTask<Batch?> GetAsync(string batchId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Updates the batch state.
    /// </summary>
    /// <param name="batchId">The batch ID.</param>
    /// <param name="state">The new state.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask UpdateStateAsync(
        string batchId,
        BatchState state,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Marks a task as completed within a batch.
    /// </summary>
    /// <param name="batchId">The batch ID.</param>
    /// <param name="taskId">The task ID that completed.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The updated batch.</returns>
    ValueTask<Batch?> MarkTaskCompletedAsync(
        string batchId,
        string taskId,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Marks a task as failed within a batch.
    /// </summary>
    /// <param name="batchId">The batch ID.</param>
    /// <param name="taskId">The task ID that failed.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The updated batch.</returns>
    ValueTask<Batch?> MarkTaskFailedAsync(
        string batchId,
        string taskId,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Deletes a batch.
    /// </summary>
    /// <param name="batchId">The batch ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if deleted, false if not found.</returns>
    ValueTask<bool> DeleteAsync(string batchId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the batch ID for a task (if the task belongs to a batch).
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The batch ID, or null if task is not part of a batch.</returns>
    ValueTask<string?> GetBatchIdForTaskAsync(
        string taskId,
        CancellationToken cancellationToken = default
    );
}
