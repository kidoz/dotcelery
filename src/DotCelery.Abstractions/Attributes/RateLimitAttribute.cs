using DotCelery.Core.RateLimiting;

namespace DotCelery.Core.Attributes;

/// <summary>
/// Specifies rate limiting for a task. Apply to task classes to automatically
/// enforce rate limits during execution.
/// </summary>
/// <remarks>
/// Rate limiting prevents a task from being executed more than the specified
/// number of times within a given time window. When rate limited, tasks will
/// be requeued with an appropriate delay.
/// </remarks>
/// <example>
/// <code>
/// [RateLimit(10, 60)] // 10 executions per 60 seconds
/// public class MyTask : ITask&lt;Input, Output&gt; { ... }
///
/// [RateLimit("100/h")] // 100 executions per hour
/// public class AnotherTask : ITask&lt;Input, Output&gt; { ... }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class RateLimitAttribute : Attribute
{
    /// <summary>
    /// Gets the maximum number of requests allowed within the window.
    /// </summary>
    public int Limit { get; }

    /// <summary>
    /// Gets the time window in seconds for the rate limit.
    /// </summary>
    public double WindowSeconds { get; }

    /// <summary>
    /// Gets the rate limiting algorithm to use.
    /// </summary>
    public RateLimitAlgorithm Algorithm { get; init; } = RateLimitAlgorithm.SlidingWindow;

    /// <summary>
    /// Gets or sets the resource key for rate limiting.
    /// If null, the task name is used. This allows multiple tasks to share a rate limit.
    /// </summary>
    public string? ResourceKey { get; init; }

    /// <summary>
    /// Initializes a new instance of the <see cref="RateLimitAttribute"/> class
    /// with explicit limit and window values.
    /// </summary>
    /// <param name="limit">Maximum number of executions allowed within the window.</param>
    /// <param name="windowSeconds">Time window in seconds (default: 60 seconds).</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown when limit is less than 1 or windowSeconds is less than or equal to 0.
    /// </exception>
    public RateLimitAttribute(int limit, double windowSeconds = 60)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(windowSeconds, 0);

        Limit = limit;
        WindowSeconds = windowSeconds;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RateLimitAttribute"/> class
    /// from a specification string.
    /// </summary>
    /// <param name="spec">
    /// Rate limit specification in format "limit/unit".
    /// Supported units: s (second), m (minute), h (hour), d (day).
    /// Examples: "10/m" (10 per minute), "100/h" (100 per hour), "5/s" (5 per second).
    /// </param>
    /// <exception cref="FormatException">
    /// Thrown when the specification string is invalid.
    /// </exception>
    public RateLimitAttribute(string spec)
    {
        var policy = RateLimitPolicy.Parse(spec);
        Limit = policy.Limit;
        WindowSeconds = policy.Window.TotalSeconds;
    }

    /// <summary>
    /// Creates a <see cref="RateLimitPolicy"/> from this attribute's configuration.
    /// </summary>
    /// <returns>A new rate limit policy.</returns>
    public RateLimitPolicy ToPolicy() =>
        new()
        {
            Limit = Limit,
            Window = TimeSpan.FromSeconds(WindowSeconds),
            Algorithm = Algorithm,
            ResourceKey = ResourceKey,
        };
}
