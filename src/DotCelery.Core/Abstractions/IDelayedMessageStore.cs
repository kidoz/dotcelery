using DotCelery.Core.Models;

namespace DotCelery.Core.Abstractions;

/// <summary>
/// Store for delayed messages awaiting their ETA (Estimated Time of Arrival).
/// Enables efficient handling of scheduled tasks without busy-loop polling.
/// </summary>
public interface IDelayedMessageStore : IAsyncDisposable
{
    /// <summary>
    /// Adds a message to be delivered at the specified time.
    /// </summary>
    /// <param name="message">The task message to delay.</param>
    /// <param name="deliveryTime">The time when the message should be delivered.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask AddAsync(
        TaskMessage message,
        DateTimeOffset deliveryTime,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets and removes all messages that are due for delivery.
    /// Messages with delivery time &lt;= now are returned.
    /// </summary>
    /// <param name="now">The current time to check against.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Async enumerable of due messages.</returns>
    IAsyncEnumerable<TaskMessage> GetDueMessagesAsync(
        DateTimeOffset now,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Removes a specific message by task ID (for cancellation/revocation).
    /// </summary>
    /// <param name="taskId">The task ID to remove.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the message was found and removed; otherwise false.</returns>
    ValueTask<bool> RemoveAsync(string taskId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the count of pending delayed messages.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of messages waiting for delivery.</returns>
    ValueTask<long> GetPendingCountAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the next delivery time, if any messages are pending.
    /// Useful for optimizing poll intervals.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The earliest delivery time, or null if no messages are pending.</returns>
    ValueTask<DateTimeOffset?> GetNextDeliveryTimeAsync(
        CancellationToken cancellationToken = default
    );
}
