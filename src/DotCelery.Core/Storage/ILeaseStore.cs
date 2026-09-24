namespace DotCelery.Core.Storage;

/// <summary>
/// Grants time-limited exclusive leases on keys, such as partition locks.
/// </summary>
/// <remarks>
/// Each acquisition gets a fencing token that is greater than any token issued before for
/// the same key. A holder that may have lost its lease can pass the token to the resource it
/// protects, so the resource can reject writes from an older holder.
/// </remarks>
public interface ILeaseStore
{
    /// <summary>
    /// Acquires a lease if the key is free or its lease has expired. If
    /// <paramref name="owner"/> already holds the lease, it is extended and keeps its token.
    /// </summary>
    /// <param name="key">The key to lease.</param>
    /// <param name="owner">Identifies the holder.</param>
    /// <param name="duration">How long the lease lasts.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The lease, or <c>null</c> if another owner holds it.</returns>
    ValueTask<Lease?> TryAcquireAsync(
        string key,
        string owner,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Extends a lease that is still held.
    /// </summary>
    /// <param name="lease">The lease to extend.</param>
    /// <param name="duration">How long the lease lasts from now.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The extended lease, or <c>null</c> if the lease expired or was taken over.</returns>
    ValueTask<Lease?> RenewAsync(
        Lease lease,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Releases a lease. A lease that was taken over by another holder is not affected.
    /// </summary>
    /// <param name="lease">The lease to release.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><c>true</c> if the lease was released.</returns>
    ValueTask<bool> ReleaseAsync(Lease lease, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the current lease on a key.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The lease, or <c>null</c> if the key is free.</returns>
    ValueTask<Lease?> GetAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists current leases whose keys start with <paramref name="keyPrefix"/>, ordered by key.
    /// </summary>
    /// <param name="keyPrefix">The key prefix; empty lists all leases.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The leases.</returns>
    IAsyncEnumerable<Lease> ListAsync(
        string keyPrefix,
        CancellationToken cancellationToken = default
    );
}

/// <summary>
/// A lease on a key.
/// </summary>
/// <param name="Key">The leased key.</param>
/// <param name="Owner">The holder.</param>
/// <param name="Token">The fencing token of this acquisition.</param>
/// <param name="ExpiresAt">When the lease expires.</param>
public sealed record Lease(string Key, string Owner, long Token, DateTimeOffset ExpiresAt);
