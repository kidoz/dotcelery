using DotCelery.Core.Batches;

namespace DotCelery.Client.Batches;

/// <summary>
/// Client interface for batch operations.
/// </summary>
public interface IBatchClient
{
    /// <summary>
    /// Creates and publishes a new batch of tasks.
    /// </summary>
    /// <param name="configure">Configuration action for the batch.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The created batch ID.</returns>
    ValueTask<string> CreateBatchAsync(
        Action<BatchBuilder> configure,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets the current state of a batch.
    /// </summary>
    /// <param name="batchId">The batch ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The batch state, or null if not found.</returns>
    ValueTask<Batch?> GetBatchAsync(string batchId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Waits for a batch to complete.
    /// </summary>
    /// <param name="batchId">The batch ID.</param>
    /// <param name="timeout">Optional timeout.</param>
    /// <param name="pollInterval">Interval between status checks.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The completed batch.</returns>
    Task<Batch> WaitForBatchAsync(
        string batchId,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Cancels a batch and revokes all pending tasks.
    /// </summary>
    /// <param name="batchId">The batch ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask CancelBatchAsync(string batchId, CancellationToken cancellationToken = default);
}
