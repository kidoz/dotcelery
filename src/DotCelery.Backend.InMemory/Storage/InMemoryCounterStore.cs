using DotCelery.Core.Storage;

namespace DotCelery.Backend.InMemory.Storage;

/// <summary>
/// In-memory <see cref="ICounterStore"/>.
/// </summary>
internal sealed class InMemoryCounterStore : ICounterStore
{
    private readonly Dictionary<string, Counter> _counters = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Window> _windows = new(StringComparer.Ordinal);
    private readonly Lock _lock = new();
    private readonly TimeProvider _timeProvider;

    public InMemoryCounterStore(TimeProvider timeProvider)
    {
        _timeProvider = timeProvider;
    }

    public ValueTask<long> IncrementAsync(
        string key,
        long delta = 1,
        TimeSpan? timeToLive = null,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidKey(key);
        if (timeToLive is { } ttl)
        {
            StorageGuard.ThrowIfNotPositive(ttl, nameof(timeToLive));
        }

        lock (_lock)
        {
            var now = _timeProvider.GetUtcNow();
            var counter = TryGetLive(key, now, out var current)
                ? current with
                {
                    Value = current.Value + delta,
                }
                : new Counter(delta, now + timeToLive);

            _counters[key] = counter;
            return ValueTask.FromResult(counter.Value);
        }
    }

    public ValueTask<long> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        StorageGuard.ThrowIfInvalidKey(key);

        lock (_lock)
        {
            return ValueTask.FromResult(
                TryGetLive(key, _timeProvider.GetUtcNow(), out var counter) ? counter.Value : 0
            );
        }
    }

    public ValueTask<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        StorageGuard.ThrowIfInvalidKey(key);

        lock (_lock)
        {
            var live = TryGetLive(key, _timeProvider.GetUtcNow(), out _);
            _counters.Remove(key);
            return ValueTask.FromResult(live);
        }
    }

    public ValueTask<WindowResult> TryAddToWindowAsync(
        string key,
        int limit,
        TimeSpan window,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidKey(key);
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        StorageGuard.ThrowIfNotPositive(window);

        lock (_lock)
        {
            var now = _timeProvider.GetUtcNow();

            if (!_windows.TryGetValue(key, out var events))
            {
                events = new Window();
                _windows[key] = events;
            }

            events.Length = window;
            events.Times.RemoveAll(t => t <= now - window);

            if (events.Times.Count < limit)
            {
                events.Times.Add(now);
                return ValueTask.FromResult(new WindowResult(true, events.Times.Count, null));
            }

            var retryAfter = events.Times[0] + window - now;
            return ValueTask.FromResult(new WindowResult(false, events.Times.Count, retryAfter));
        }
    }

    public ValueTask<long> GetWindowCountAsync(
        string key,
        TimeSpan window,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidKey(key);
        StorageGuard.ThrowIfNotPositive(window);

        lock (_lock)
        {
            var now = _timeProvider.GetUtcNow();
            return ValueTask.FromResult(
                _windows.TryGetValue(key, out var events)
                    ? events.Times.LongCount(t => t > now - window)
                    : 0L
            );
        }
    }

    internal long PurgeExpired()
    {
        lock (_lock)
        {
            var now = _timeProvider.GetUtcNow();
            var expired = _counters
                .Where(c => c.Value.ExpiresAt <= now)
                .Select(c => c.Key)
                .ToList();
            foreach (var key in expired)
            {
                _counters.Remove(key);
            }

            long removed = expired.Count;
            foreach (var (key, events) in _windows.ToList())
            {
                removed += events.Times.RemoveAll(t => t <= now - events.Length);
                if (events.Times.Count == 0)
                {
                    _windows.Remove(key);
                }
            }

            return removed;
        }
    }

    private bool TryGetLive(string key, DateTimeOffset now, out Counter counter)
    {
        if (_counters.TryGetValue(key, out counter!) && !(counter.ExpiresAt <= now))
        {
            return true;
        }

        return false;
    }

    private sealed record Counter(long Value, DateTimeOffset? ExpiresAt);

    private sealed class Window
    {
        public List<DateTimeOffset> Times { get; } = [];

        public TimeSpan Length { get; set; }
    }
}
