namespace DotCelery.Core.Storage;

/// <summary>
/// Named queues of items that become due at a given time and are claimed by one consumer at a time.
/// </summary>
/// <remarks>
/// A claim lasts for a visibility timeout. If the consumer neither completes nor abandons the
/// item in time, the item can be claimed again, so a consumer that crashes does not lose it.
/// Consumers must therefore tolerate an item being delivered more than once.
/// </remarks>
public interface IQueueStore
{
    /// <summary>
    /// Adds an item, or replaces the item with the same id. Replacing an item releases its
    /// claim and resets its delivery count.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="id">The item id.</param>
    /// <param name="payload">The item payload.</param>
    /// <param name="dueAt">When the item can first be claimed.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the item is stored.</returns>
    ValueTask EnqueueAsync(
        string queue,
        string id,
        ReadOnlyMemory<byte> payload,
        DateTimeOffset dueAt,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Claims due items that are not claimed, or whose claim has expired, in order of due
    /// time and then id. Each claim increments the item's delivery count.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="maxItems">The maximum number of items to claim.</param>
    /// <param name="visibilityTimeout">How long the claim lasts.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The claimed items; none of them is claimed by another consumer.</returns>
    ValueTask<IReadOnlyList<QueueItem>> ClaimAsync(
        string queue,
        int maxItems,
        TimeSpan visibilityTimeout,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Removes a claimed item after it was processed.
    /// </summary>
    /// <param name="item">The claimed item.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// <c>true</c> if the item was removed; <c>false</c> if the claim expired and the item
    /// was claimed again, replaced, or removed.
    /// </returns>
    ValueTask<bool> CompleteAsync(QueueItem item, CancellationToken cancellationToken = default);

    /// <summary>
    /// Releases the claim on an item so it can be claimed again.
    /// </summary>
    /// <param name="item">The claimed item.</param>
    /// <param name="dueAt">When the item becomes due again; it stays due now when not set.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><c>true</c> if the claim was released; <c>false</c> if it was no longer held.</returns>
    ValueTask<bool> AbandonAsync(
        QueueItem item,
        DateTimeOffset? dueAt = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Removes an item whether or not it is claimed.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="id">The item id.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><c>true</c> if the item was removed.</returns>
    ValueTask<bool> RemoveAsync(
        string queue,
        string id,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Counts all items in a queue, claimed or not.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of items.</returns>
    ValueTask<long> CountAsync(string queue, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the earliest due time of the items that are not claimed.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The earliest due time, or <c>null</c> if no item is waiting.</returns>
    ValueTask<DateTimeOffset?> GetNextDueAsync(
        string queue,
        CancellationToken cancellationToken = default
    );
}

/// <summary>
/// A claimed queue item.
/// </summary>
/// <param name="Queue">The queue name.</param>
/// <param name="Id">The item id.</param>
/// <param name="Payload">The item payload.</param>
/// <param name="DueAt">When the item became due.</param>
/// <param name="DeliveryCount">How many times the item has been claimed, including this claim.</param>
/// <param name="ClaimToken">Identifies this claim.</param>
/// <param name="ClaimedUntil">When this claim expires.</param>
public sealed record QueueItem(
    string Queue,
    string Id,
    ReadOnlyMemory<byte> Payload,
    DateTimeOffset DueAt,
    int DeliveryCount,
    Guid ClaimToken,
    DateTimeOffset ClaimedUntil
);
