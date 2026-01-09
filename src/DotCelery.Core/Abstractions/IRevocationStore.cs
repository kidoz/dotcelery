using DotCelery.Core.Models;

namespace DotCelery.Core.Abstractions;

/// <summary>
/// Store for revoked task IDs. Used to prevent execution of cancelled tasks
/// and to signal running tasks to stop.
/// </summary>
public interface IRevocationStore : IAsyncDisposable
{
    /// <summary>
    /// Marks a task as revoked.
    /// </summary>
    /// <param name="taskId">The task ID to revoke.</param>
    /// <param name="options">Revocation options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask RevokeAsync(
        string taskId,
        RevokeOptions? options = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Marks multiple tasks as revoked.
    /// </summary>
    /// <param name="taskIds">The task IDs to revoke.</param>
    /// <param name="options">Revocation options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask RevokeAsync(
        IEnumerable<string> taskIds,
        RevokeOptions? options = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Checks if a task has been revoked.
    /// </summary>
    /// <param name="taskId">The task ID to check.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the task has been revoked; otherwise false.</returns>
    ValueTask<bool> IsRevokedAsync(string taskId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets all revoked task IDs. Useful for worker startup synchronization.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Async enumerable of revoked task IDs.</returns>
    IAsyncEnumerable<string> GetRevokedTaskIdsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes old revocations to free up storage.
    /// </summary>
    /// <param name="maxAge">Maximum age of revocations to keep.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of revocations removed.</returns>
    ValueTask<long> CleanupAsync(TimeSpan maxAge, CancellationToken cancellationToken = default);

    /// <summary>
    /// Subscribes to revocation events for real-time notification.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Async enumerable of revocation events.</returns>
    IAsyncEnumerable<RevocationEvent> SubscribeAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Event raised when a task is revoked.
/// </summary>
public sealed record RevocationEvent
{
    /// <summary>
    /// Gets the revoked task ID.
    /// </summary>
    public required string TaskId { get; init; }

    /// <summary>
    /// Gets the revocation options.
    /// </summary>
    public required RevokeOptions Options { get; init; }

    /// <summary>
    /// Gets the timestamp when the revocation occurred.
    /// </summary>
    public required DateTimeOffset Timestamp { get; init; }
}
