namespace DotCelery.Core.RateLimiting;

/// <summary>
/// Configuration for rate limiting task execution.
/// </summary>
public sealed record RateLimitPolicy
{
    /// <summary>
    /// Gets the maximum number of requests allowed within the window.
    /// </summary>
    public required int Limit { get; init; }

    /// <summary>
    /// Gets the time window for the rate limit.
    /// </summary>
    public required TimeSpan Window { get; init; }

    /// <summary>
    /// Gets the algorithm used for rate limiting.
    /// </summary>
    public RateLimitAlgorithm Algorithm { get; init; } = RateLimitAlgorithm.SlidingWindow;

    /// <summary>
    /// Gets the resource key for rate limiting. If null, the task name is used.
    /// This allows multiple tasks to share a rate limit.
    /// </summary>
    public string? ResourceKey { get; init; }

    /// <summary>
    /// Creates a rate limit policy allowing the specified number of requests per second.
    /// </summary>
    /// <param name="limit">Maximum requests per second.</param>
    /// <returns>A new rate limit policy.</returns>
    public static RateLimitPolicy PerSecond(int limit) =>
        new() { Limit = limit, Window = TimeSpan.FromSeconds(1) };

    /// <summary>
    /// Creates a rate limit policy allowing the specified number of requests per minute.
    /// </summary>
    /// <param name="limit">Maximum requests per minute.</param>
    /// <returns>A new rate limit policy.</returns>
    public static RateLimitPolicy PerMinute(int limit) =>
        new() { Limit = limit, Window = TimeSpan.FromMinutes(1) };

    /// <summary>
    /// Creates a rate limit policy allowing the specified number of requests per hour.
    /// </summary>
    /// <param name="limit">Maximum requests per hour.</param>
    /// <returns>A new rate limit policy.</returns>
    public static RateLimitPolicy PerHour(int limit) =>
        new() { Limit = limit, Window = TimeSpan.FromHours(1) };

    /// <summary>
    /// Creates a rate limit policy allowing the specified number of requests per day.
    /// </summary>
    /// <param name="limit">Maximum requests per day.</param>
    /// <returns>A new rate limit policy.</returns>
    public static RateLimitPolicy PerDay(int limit) =>
        new() { Limit = limit, Window = TimeSpan.FromDays(1) };

    /// <summary>
    /// Parses a rate limit specification string like "10/m", "100/h", "5/s".
    /// </summary>
    /// <param name="spec">The specification string.</param>
    /// <returns>A new rate limit policy.</returns>
    /// <exception cref="FormatException">If the specification is invalid.</exception>
    public static RateLimitPolicy Parse(string spec)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(spec);

        var parts = spec.Split('/');
        if (parts.Length != 2)
        {
            throw new FormatException(
                $"Invalid rate limit specification: '{spec}'. Expected format: 'limit/unit' (e.g., '10/m')"
            );
        }

        if (!int.TryParse(parts[0], out var limit) || limit <= 0)
        {
            throw new FormatException(
                $"Invalid rate limit value: '{parts[0]}'. Must be a positive integer."
            );
        }

        var window = parts[1].ToLowerInvariant() switch
        {
            "s" or "sec" or "second" => TimeSpan.FromSeconds(1),
            "m" or "min" or "minute" => TimeSpan.FromMinutes(1),
            "h" or "hr" or "hour" => TimeSpan.FromHours(1),
            "d" or "day" => TimeSpan.FromDays(1),
            _ => throw new FormatException(
                $"Invalid rate limit unit: '{parts[1]}'. Expected: s, m, h, or d."
            ),
        };

        return new RateLimitPolicy { Limit = limit, Window = window };
    }

    /// <summary>
    /// Tries to parse a rate limit specification string.
    /// </summary>
    /// <param name="spec">The specification string.</param>
    /// <param name="policy">The parsed policy, if successful.</param>
    /// <returns>True if parsing succeeded; otherwise false.</returns>
    public static bool TryParse(string? spec, out RateLimitPolicy? policy)
    {
        policy = null;

        if (string.IsNullOrWhiteSpace(spec))
        {
            return false;
        }

        try
        {
            policy = Parse(spec);
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    /// <inheritdoc />
    public override string ToString() => $"{Limit}/{FormatWindow(Window)}";

    private static string FormatWindow(TimeSpan window) =>
        window.TotalSeconds switch
        {
            1 => "s",
            60 => "m",
            3600 => "h",
            86400 => "d",
            _ => $"{window.TotalSeconds}s",
        };
}

/// <summary>
/// Algorithm used for rate limiting.
/// </summary>
public enum RateLimitAlgorithm
{
    /// <summary>
    /// Fixed window algorithm. Resets the counter at the start of each window.
    /// Simple but can allow bursts at window boundaries.
    /// </summary>
    FixedWindow,

    /// <summary>
    /// Sliding window algorithm. Provides smoother rate limiting by considering
    /// a rolling time window. More accurate but slightly more complex.
    /// </summary>
    SlidingWindow,

    /// <summary>
    /// Token bucket algorithm. Allows controlled bursting while maintaining
    /// an average rate. Good for APIs with occasional bursts.
    /// </summary>
    TokenBucket,
}
