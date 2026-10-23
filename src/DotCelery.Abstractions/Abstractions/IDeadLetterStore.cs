using DotCelery.Core.DeadLetter;

namespace DotCelery.Core.Abstractions;

/// <summary>
/// Storage interface for dead letter queue messages.
/// </summary>
public interface IDeadLetterStore : IAsyncDisposable
{
    /// <summary>
    /// Stores a message in the dead letter queue.
    /// </summary>
    /// <param name="message">The dead letter message.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask StoreAsync(DeadLetterMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets all messages from the dead letter queue.
    /// </summary>
    /// <param name="limit">Maximum number of messages to return.</param>
    /// <param name="offset">Number of messages to skip.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Stream of dead letter messages.</returns>
    IAsyncEnumerable<DeadLetterMessage> GetAllAsync(
        int limit = 100,
        int offset = 0,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets a specific message by ID.
    /// </summary>
    /// <param name="messageId">The message ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The message, or null if not found.</returns>
    ValueTask<DeadLetterMessage?> GetAsync(
        string messageId,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Requeues a message from the dead letter queue for reprocessing.
    /// </summary>
    /// <param name="messageId">The message ID to requeue.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the message was requeued, false if not found.</returns>
    ValueTask<bool> RequeueAsync(string messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a message from the dead letter queue.
    /// </summary>
    /// <param name="messageId">The message ID to delete.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the message was deleted, false if not found.</returns>
    ValueTask<bool> DeleteAsync(string messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the count of messages in the dead letter queue.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The message count.</returns>
    ValueTask<long> GetCountAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Purges all messages from the dead letter queue.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Number of messages purged.</returns>
    ValueTask<long> PurgeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes expired messages from the dead letter queue.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Number of messages removed.</returns>
    ValueTask<long> CleanupExpiredAsync(CancellationToken cancellationToken = default);
}
