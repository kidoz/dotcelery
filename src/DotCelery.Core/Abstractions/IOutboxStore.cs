using DotCelery.Core.Outbox;

namespace DotCelery.Core.Abstractions;

/// <summary>
/// Transactional outbox store for exactly-once message delivery.
/// Messages are stored in the same transaction as the business operation,
/// then dispatched asynchronously by a background processor.
/// </summary>
public interface IOutboxStore : IAsyncDisposable
{
    /// <summary>
    /// Stores a message in the outbox within the current transaction.
    /// </summary>
    /// <param name="message">The message to store.</param>
    /// <param name="transaction">The database transaction to participate in (optional).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask StoreAsync(
        OutboxMessage message,
        object? transaction = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets pending messages that need to be dispatched.
    /// </summary>
    /// <param name="limit">Maximum number of messages to return.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Stream of pending messages.</returns>
    IAsyncEnumerable<OutboxMessage> GetPendingAsync(
        int limit = 100,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Marks a message as dispatched after successful publishing.
    /// </summary>
    /// <param name="messageId">The message ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask MarkDispatchedAsync(string messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks a message as failed with error details.
    /// </summary>
    /// <param name="messageId">The message ID.</param>
    /// <param name="errorMessage">Error information.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask MarkFailedAsync(
        string messageId,
        string errorMessage,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets the count of pending messages.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Number of pending messages.</returns>
    ValueTask<long> GetPendingCountAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes old dispatched messages.
    /// </summary>
    /// <param name="olderThan">Remove messages dispatched before this duration.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Number of messages removed.</returns>
    ValueTask<long> CleanupAsync(TimeSpan olderThan, CancellationToken cancellationToken = default);
}
