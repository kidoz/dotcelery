using DotCelery.Core.RateLimiting;

namespace DotCelery.Core.Abstractions;

/// <summary>
/// Rate limiter for controlling task execution frequency.
/// </summary>
public interface IRateLimiter : IAsyncDisposable
{
    /// <summary>
    /// Attempts to acquire a permit for executing the specified task.
    /// </summary>
    /// <param name="resourceKey">The resource key to rate limit (typically task name).</param>
    /// <param name="policy">The rate limit policy to apply.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A lease if the permit was acquired; otherwise null if rate limited.</returns>
    ValueTask<RateLimitLease> TryAcquireAsync(
        string resourceKey,
        RateLimitPolicy policy,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets the time until the next permit is available.
    /// </summary>
    /// <param name="resourceKey">The resource key to check.</param>
    /// <param name="policy">The rate limit policy.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The time to wait, or null if a permit is available now.</returns>
    ValueTask<TimeSpan?> GetRetryAfterAsync(
        string resourceKey,
        RateLimitPolicy policy,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets the current usage for a resource.
    /// </summary>
    /// <param name="resourceKey">The resource key to check.</param>
    /// <param name="policy">The rate limit policy.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The current usage information.</returns>
    ValueTask<RateLimitUsage> GetUsageAsync(
        string resourceKey,
        RateLimitPolicy policy,
        CancellationToken cancellationToken = default
    );
}

/// <summary>
/// Represents the result of a rate limit acquisition attempt.
/// </summary>
public sealed class RateLimitLease : IAsyncDisposable
{
    /// <summary>
    /// Gets whether the permit was acquired successfully.
    /// </summary>
    public bool IsAcquired { get; init; }

    /// <summary>
    /// Gets the time to wait before retrying, if the permit was not acquired.
    /// </summary>
    public TimeSpan? RetryAfter { get; init; }

    /// <summary>
    /// Gets the remaining permits in the current window.
    /// </summary>
    public int? Remaining { get; init; }

    /// <summary>
    /// Gets the time when the current window resets.
    /// </summary>
    public DateTimeOffset? ResetAt { get; init; }

    /// <summary>
    /// Gets a successful lease indicating the permit was acquired.
    /// </summary>
    public static RateLimitLease Success { get; } = new() { IsAcquired = true };

    /// <summary>
    /// Creates a successful lease with metadata.
    /// </summary>
    public static RateLimitLease Acquired(int remaining, DateTimeOffset resetAt) =>
        new()
        {
            IsAcquired = true,
            Remaining = remaining,
            ResetAt = resetAt,
        };

    /// <summary>
    /// Creates a failed lease indicating the rate limit was exceeded.
    /// </summary>
    /// <param name="retryAfter">The time to wait before retrying.</param>
    /// <returns>A rate-limited lease.</returns>
    public static RateLimitLease RateLimited(TimeSpan retryAfter) =>
        new() { IsAcquired = false, RetryAfter = retryAfter };

    /// <summary>
    /// Creates a failed lease with full metadata.
    /// </summary>
    public static RateLimitLease RateLimited(TimeSpan retryAfter, DateTimeOffset resetAt) =>
        new()
        {
            IsAcquired = false,
            RetryAfter = retryAfter,
            Remaining = 0,
            ResetAt = resetAt,
        };

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        // No-op for basic implementation
        // Sliding window implementations may use this to release permits
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// Current rate limit usage information.
/// </summary>
public sealed record RateLimitUsage
{
    /// <summary>
    /// Gets the number of requests used in the current window.
    /// </summary>
    public required int Used { get; init; }

    /// <summary>
    /// Gets the total limit for the window.
    /// </summary>
    public required int Limit { get; init; }

    /// <summary>
    /// Gets the remaining requests in the current window.
    /// </summary>
    public int Remaining => Math.Max(0, Limit - Used);

    /// <summary>
    /// Gets the time when the current window resets.
    /// </summary>
    public required DateTimeOffset ResetAt { get; init; }

    /// <summary>
    /// Gets whether the rate limit has been exceeded.
    /// </summary>
    public bool IsLimited => Used >= Limit;
}
