namespace DotCelery.Client;

/// <summary>
/// Options for sending a task.
/// </summary>
public sealed record SendOptions
{
    /// <summary>
    /// Gets the scheduled execution time.
    /// </summary>
    public DateTimeOffset? Eta { get; init; }

    /// <summary>
    /// Gets the delay before execution.
    /// </summary>
    public TimeSpan? Countdown { get; init; }

    /// <summary>
    /// Gets the task expiration time.
    /// </summary>
    public DateTimeOffset? Expires { get; init; }

    /// <summary>
    /// Gets the target queue (overrides default).
    /// </summary>
    public string? Queue { get; init; }

    /// <summary>
    /// Gets the task priority (0-9, higher = more urgent).
    /// </summary>
    public int? Priority { get; init; }

    /// <summary>
    /// Gets the maximum retry attempts.
    /// </summary>
    public int? MaxRetries { get; init; }

    /// <summary>
    /// Gets the custom task ID (auto-generated if null).
    /// </summary>
    public string? TaskId { get; init; }

    /// <summary>
    /// Gets the correlation ID for tracing.
    /// </summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// Gets the custom headers.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Headers { get; init; }

    /// <summary>
    /// Validates the send options and throws if invalid.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if Priority or MaxRetries are out of range.</exception>
    /// <exception cref="ArgumentException">Thrown if Expires is in the past or before Eta.</exception>
    public void Validate()
    {
        if (Priority.HasValue && (Priority.Value < 0 || Priority.Value > 9))
        {
            throw new ArgumentOutOfRangeException(
                nameof(Priority),
                Priority.Value,
                "Priority must be between 0 and 9"
            );
        }

        if (MaxRetries.HasValue && MaxRetries.Value < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxRetries),
                MaxRetries.Value,
                "MaxRetries cannot be negative"
            );
        }

        if (Countdown.HasValue && Countdown.Value < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(Countdown),
                Countdown.Value,
                "Countdown cannot be negative"
            );
        }

        // Calculate effective ETA for validation
        var effectiveEta = Eta;
        if (Countdown.HasValue)
        {
            effectiveEta = DateTimeOffset.UtcNow.Add(Countdown.Value);
        }

        // Check if Expires is before the effective ETA
        if (Expires.HasValue && effectiveEta.HasValue && Expires.Value < effectiveEta.Value)
        {
            throw new ArgumentException(
                "Expires cannot be before the scheduled execution time (Eta/Countdown)",
                nameof(Expires)
            );
        }
    }
}
