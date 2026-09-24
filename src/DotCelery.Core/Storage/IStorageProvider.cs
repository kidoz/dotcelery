namespace DotCelery.Core.Storage;

/// <summary>
/// The storage primitives of one backend. DotCelery's stores are built on these primitives,
/// so a backend implements only this interface to support every store.
/// </summary>
/// <remarks>
/// Providers use the <see cref="TimeProvider"/> they are given for expiry, due times, and
/// lease deadlines, rather than the database clock. Processes that share a backend therefore
/// need synchronized clocks, as they already do for task ETAs and expiry.
/// </remarks>
public interface IStorageProvider
{
    /// <summary>
    /// Gets the provider name, such as <c>InMemory</c> or <c>PostgreSQL</c>.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Gets the document store.
    /// </summary>
    IDocumentStore Documents { get; }

    /// <summary>
    /// Gets the lease store.
    /// </summary>
    ILeaseStore Leases { get; }

    /// <summary>
    /// Gets the queue store.
    /// </summary>
    IQueueStore Queues { get; }

    /// <summary>
    /// Gets the counter store.
    /// </summary>
    ICounterStore Counters { get; }

    /// <summary>
    /// Gets the notification channel, or <c>null</c> if the backend cannot push notifications.
    /// </summary>
    INotificationChannel? Notifications { get; }

    /// <summary>
    /// Physically removes expired documents, leases, and counters, and events that have left
    /// every window. Expired data is already invisible; this only reclaims space.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of records removed.</returns>
    ValueTask<long> PurgeExpiredAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks whether the backend is reachable.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><c>true</c> if the backend is healthy.</returns>
    ValueTask<bool> IsHealthyAsync(CancellationToken cancellationToken = default);
}
