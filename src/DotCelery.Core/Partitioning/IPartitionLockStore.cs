namespace DotCelery.Core.Partitioning;

/// <summary>
/// Store for managing partition locks to ensure sequential processing.
/// Messages with the same partition key are processed one at a time.
/// </summary>
public interface IPartitionLockStore : IAsyncDisposable
{
    /// <summary>
    /// Attempts to acquire a lock for the specified partition.
    /// </summary>
    /// <param name="partitionKey">The partition key to lock.</param>
    /// <param name="taskId">The task ID requesting the lock.</param>
    /// <param name="timeout">How long the lock should be held before auto-release.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if lock acquired, false if partition is already locked.</returns>
    ValueTask<bool> TryAcquireAsync(
        string partitionKey,
        string taskId,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Releases the lock for the specified partition.
    /// </summary>
    /// <param name="partitionKey">The partition key to unlock.</param>
    /// <param name="taskId">The task ID that holds the lock.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if lock was released, false if not held by this task.</returns>
    ValueTask<bool> ReleaseAsync(
        string partitionKey,
        string taskId,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Checks if a partition is currently locked.
    /// </summary>
    /// <param name="partitionKey">The partition key to check.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if locked, false otherwise.</returns>
    ValueTask<bool> IsLockedAsync(
        string partitionKey,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets the task ID that currently holds the lock for a partition.
    /// </summary>
    /// <param name="partitionKey">The partition key to query.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The task ID holding the lock, or null if not locked.</returns>
    ValueTask<string?> GetLockHolderAsync(
        string partitionKey,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Extends the timeout for an existing lock.
    /// </summary>
    /// <param name="partitionKey">The partition key.</param>
    /// <param name="taskId">The task ID that holds the lock.</param>
    /// <param name="extension">Additional time to add to the lock.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if extended, false if not held by this task.</returns>
    ValueTask<bool> ExtendAsync(
        string partitionKey,
        string taskId,
        TimeSpan extension,
        CancellationToken cancellationToken = default
    );
}
