using DotCelery.Core.Storage;

namespace DotCelery.Backend.InMemory.Storage;

/// <summary>
/// In-memory <see cref="ILeaseStore"/>.
/// </summary>
internal sealed class InMemoryLeaseStore : ILeaseStore
{
    private readonly Dictionary<string, Lease> _leases = new(StringComparer.Ordinal);
    private readonly Lock _lock = new();
    private readonly TimeProvider _timeProvider;
    private long _lastToken;

    public InMemoryLeaseStore(TimeProvider timeProvider)
    {
        _timeProvider = timeProvider;
    }

    public ValueTask<Lease?> TryAcquireAsync(
        string key,
        string owner,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidKey(key);
        StorageGuard.ThrowIfInvalidKey(owner);
        StorageGuard.ThrowIfNotPositive(duration);

        lock (_lock)
        {
            var now = _timeProvider.GetUtcNow();

            if (TryGetLive(key, now, out var current))
            {
                if (current.Owner != owner)
                {
                    return ValueTask.FromResult<Lease?>(null);
                }

                var extended = current with { ExpiresAt = now + duration };
                _leases[key] = extended;
                return ValueTask.FromResult<Lease?>(extended);
            }

            var lease = new Lease(key, owner, ++_lastToken, now, now + duration);
            _leases[key] = lease;
            return ValueTask.FromResult<Lease?>(lease);
        }
    }

    public ValueTask<Lease?> RenewAsync(
        Lease lease,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(lease);
        StorageGuard.ThrowIfNotPositive(duration);

        lock (_lock)
        {
            var now = _timeProvider.GetUtcNow();

            if (!TryGetLive(lease.Key, now, out var current) || current.Token != lease.Token)
            {
                return ValueTask.FromResult<Lease?>(null);
            }

            var renewed = current with { ExpiresAt = now + duration };
            _leases[lease.Key] = renewed;
            return ValueTask.FromResult<Lease?>(renewed);
        }
    }

    public ValueTask<bool> ReleaseAsync(Lease lease, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);

        lock (_lock)
        {
            if (!_leases.TryGetValue(lease.Key, out var current) || current.Token != lease.Token)
            {
                return ValueTask.FromResult(false);
            }

            _leases.Remove(lease.Key);
            return ValueTask.FromResult(true);
        }
    }

    public ValueTask<Lease?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        StorageGuard.ThrowIfInvalidKey(key);

        lock (_lock)
        {
            return ValueTask.FromResult(
                TryGetLive(key, _timeProvider.GetUtcNow(), out var lease) ? lease : null
            );
        }
    }

    public IAsyncEnumerable<Lease> ListAsync(
        string keyPrefix,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidPrefix(keyPrefix);

        List<Lease> leases;
        lock (_lock)
        {
            var now = _timeProvider.GetUtcNow();
            leases =
            [
                .. _leases
                    .Values.Where(l =>
                        l.ExpiresAt > now && l.Key.StartsWith(keyPrefix, StringComparison.Ordinal)
                    )
                    .OrderBy(l => l.Key, StringComparer.Ordinal),
            ];
        }

        return leases.ToAsyncEnumerable();
    }

    internal long PurgeExpired()
    {
        lock (_lock)
        {
            var now = _timeProvider.GetUtcNow();
            var expired = _leases.Values.Where(l => l.ExpiresAt <= now).Select(l => l.Key).ToList();
            foreach (var key in expired)
            {
                _leases.Remove(key);
            }

            return expired.Count;
        }
    }

    private bool TryGetLive(string key, DateTimeOffset now, out Lease lease)
    {
        if (_leases.TryGetValue(key, out lease!) && lease.ExpiresAt > now)
        {
            return true;
        }

        return false;
    }
}
