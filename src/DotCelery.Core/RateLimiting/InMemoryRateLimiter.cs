using System.Collections.Concurrent;
using DotCelery.Core.Abstractions;

namespace DotCelery.Core.RateLimiting;

/// <summary>
/// In-memory rate limiter for single-worker scenarios and testing.
/// Implements sliding window algorithm for accurate rate limiting.
/// </summary>
public sealed class InMemoryRateLimiter : IRateLimiter
{
    private readonly ConcurrentDictionary<string, RateLimitBucket> _buckets = new();
    private readonly TimeProvider _timeProvider;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryRateLimiter"/> class.
    /// </summary>
    /// <param name="timeProvider">Optional time provider for testing.</param>
    public InMemoryRateLimiter(TimeProvider? timeProvider = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public ValueTask<RateLimitLease> TryAcquireAsync(
        string resourceKey,
        RateLimitPolicy policy,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(resourceKey);
        ArgumentNullException.ThrowIfNull(policy);

        var key = GetKey(resourceKey, policy);
        var bucket = _buckets.GetOrAdd(key, _ => new RateLimitBucket(policy, _timeProvider));

        var lease = bucket.TryAcquire();
        return ValueTask.FromResult(lease);
    }

    /// <inheritdoc />
    public ValueTask<TimeSpan?> GetRetryAfterAsync(
        string resourceKey,
        RateLimitPolicy policy,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(resourceKey);
        ArgumentNullException.ThrowIfNull(policy);

        var key = GetKey(resourceKey, policy);

        if (!_buckets.TryGetValue(key, out var bucket))
        {
            return ValueTask.FromResult<TimeSpan?>(null);
        }

        var retryAfter = bucket.GetRetryAfter();
        return ValueTask.FromResult(retryAfter);
    }

    /// <inheritdoc />
    public ValueTask<RateLimitUsage> GetUsageAsync(
        string resourceKey,
        RateLimitPolicy policy,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(resourceKey);
        ArgumentNullException.ThrowIfNull(policy);

        var key = GetKey(resourceKey, policy);

        if (!_buckets.TryGetValue(key, out var bucket))
        {
            var now = _timeProvider.GetUtcNow();
            return ValueTask.FromResult(
                new RateLimitUsage
                {
                    Used = 0,
                    Limit = policy.Limit,
                    ResetAt = now.Add(policy.Window),
                }
            );
        }

        return ValueTask.FromResult(bucket.GetUsage());
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return ValueTask.CompletedTask;
        }

        _disposed = true;
        _buckets.Clear();

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Clears all rate limit buckets. For testing purposes.
    /// </summary>
    public void Clear()
    {
        _buckets.Clear();
    }

    private static string GetKey(string resourceKey, RateLimitPolicy policy) =>
        $"{resourceKey}:{policy.Window.TotalSeconds}:{policy.Limit}";

    private sealed class RateLimitBucket
    {
        private readonly RateLimitPolicy _policy;
        private readonly TimeProvider _timeProvider;
        private readonly Lock _lock = new();
        private readonly Queue<DateTimeOffset> _timestamps = new();

        public RateLimitBucket(RateLimitPolicy policy, TimeProvider timeProvider)
        {
            _policy = policy;
            _timeProvider = timeProvider;
        }

        public RateLimitLease TryAcquire()
        {
            lock (_lock)
            {
                var now = _timeProvider.GetUtcNow();
                CleanupExpired(now);

                if (_timestamps.Count >= _policy.Limit)
                {
                    var oldest = _timestamps.Peek();
                    var retryAfter = oldest.Add(_policy.Window) - now;
                    var resetAt = oldest.Add(_policy.Window);

                    return RateLimitLease.RateLimited(
                        retryAfter > TimeSpan.Zero ? retryAfter : TimeSpan.FromMilliseconds(1),
                        resetAt
                    );
                }

                _timestamps.Enqueue(now);

                var remaining = _policy.Limit - _timestamps.Count;
                var nextReset =
                    _timestamps.Count > 0
                        ? _timestamps.Peek().Add(_policy.Window)
                        : now.Add(_policy.Window);

                return RateLimitLease.Acquired(remaining, nextReset);
            }
        }

        public TimeSpan? GetRetryAfter()
        {
            lock (_lock)
            {
                var now = _timeProvider.GetUtcNow();
                CleanupExpired(now);

                if (_timestamps.Count < _policy.Limit)
                {
                    return null;
                }

                var oldest = _timestamps.Peek();
                var retryAfter = oldest.Add(_policy.Window) - now;

                return retryAfter > TimeSpan.Zero ? retryAfter : null;
            }
        }

        public RateLimitUsage GetUsage()
        {
            lock (_lock)
            {
                var now = _timeProvider.GetUtcNow();
                CleanupExpired(now);

                var resetAt =
                    _timestamps.Count > 0
                        ? _timestamps.Peek().Add(_policy.Window)
                        : now.Add(_policy.Window);

                return new RateLimitUsage
                {
                    Used = _timestamps.Count,
                    Limit = _policy.Limit,
                    ResetAt = resetAt,
                };
            }
        }

        private void CleanupExpired(DateTimeOffset now)
        {
            var cutoff = now - _policy.Window;

            while (_timestamps.Count > 0 && _timestamps.Peek() < cutoff)
            {
                _timestamps.Dequeue();
            }
        }
    }
}
