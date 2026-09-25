using DotCelery.Core.Abstractions;
using DotCelery.Core.RateLimiting;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// <see cref="IRateLimiter"/> on the storage primitives, using a sliding window shared by
/// every process that uses the same storage.
/// </summary>
/// <remarks>
/// Every policy is applied as a sliding window; <see cref="RateLimitPolicy.Algorithm"/> is not used.
/// </remarks>
public sealed class WindowRateLimiter : IRateLimiter
{
    private readonly ICounterStore _counters;
    private readonly string _keyPrefix;
    private readonly TimeProvider _timeProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="WindowRateLimiter"/> class.
    /// </summary>
    /// <param name="storage">The storage provider.</param>
    /// <param name="options">The store options.</param>
    /// <param name="timeProvider">The clock for reset times.</param>
    public WindowRateLimiter(
        IStorageProvider storage,
        IOptions<StorageStoreOptions>? options = null,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(storage);
        _counters = storage.Counters;
        _keyPrefix = (options?.Value ?? new StorageStoreOptions()).Name("ratelimit/");
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public async ValueTask<RateLimitLease> TryAcquireAsync(
        string resourceKey,
        RateLimitPolicy policy,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(policy);

        var result = await _counters
            .TryAddToWindowAsync(
                _keyPrefix + resourceKey,
                policy.Limit,
                policy.Window,
                cancellationToken
            )
            .ConfigureAwait(false);
        var now = _timeProvider.GetUtcNow();

        return result.Added
            ? RateLimitLease.Acquired(policy.Limit - (int)result.Count, now + policy.Window)
            : RateLimitLease.RateLimited(result.RetryAfter!.Value, now + result.RetryAfter.Value);
    }

    /// <inheritdoc />
    public async ValueTask<TimeSpan?> GetRetryAfterAsync(
        string resourceKey,
        RateLimitPolicy policy,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(policy);

        var window = await _counters
            .GetWindowAsync(_keyPrefix + resourceKey, policy.Window, cancellationToken)
            .ConfigureAwait(false);

        return window.Count < policy.Limit || window.OldestEvent is null
            ? null
            : window.OldestEvent.Value + policy.Window - _timeProvider.GetUtcNow();
    }

    /// <inheritdoc />
    public async ValueTask<RateLimitUsage> GetUsageAsync(
        string resourceKey,
        RateLimitPolicy policy,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(policy);

        var window = await _counters
            .GetWindowAsync(_keyPrefix + resourceKey, policy.Window, cancellationToken)
            .ConfigureAwait(false);
        var now = _timeProvider.GetUtcNow();

        return new RateLimitUsage
        {
            Used = (int)window.Count,
            Limit = policy.Limit,
            ResetAt = (window.OldestEvent ?? now) + policy.Window,
        };
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
