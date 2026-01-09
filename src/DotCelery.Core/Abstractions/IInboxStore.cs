namespace DotCelery.Core.Abstractions;

/// <summary>
/// Inbox store for message deduplication (exactly-once processing).
/// Tracks processed message IDs to prevent duplicate handling.
/// </summary>
public interface IInboxStore : IAsyncDisposable
{
    /// <summary>
    /// Checks if a message has already been processed.
    /// </summary>
    /// <param name="messageId">The message ID to check.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the message was already processed.</returns>
    ValueTask<bool> IsProcessedAsync(
        string messageId,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Marks a message as processed (within a transaction if supported).
    /// </summary>
    /// <param name="messageId">The message ID.</param>
    /// <param name="transaction">Optional transaction to participate in.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask MarkProcessedAsync(
        string messageId,
        object? transaction = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets the count of processed messages currently tracked.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Number of processed message records.</returns>
    ValueTask<long> GetCountAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes old processed message records.
    /// </summary>
    /// <param name="olderThan">Remove records older than this duration.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Number of records removed.</returns>
    ValueTask<long> CleanupAsync(TimeSpan olderThan, CancellationToken cancellationToken = default);
}
