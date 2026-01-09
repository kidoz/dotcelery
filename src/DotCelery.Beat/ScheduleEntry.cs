using DotCelery.Core.Canvas;
using DotCelery.Cron;

namespace DotCelery.Beat;

/// <summary>
/// Represents a scheduled task entry.
/// </summary>
public sealed class ScheduleEntry
{
    private CronExpression? _cronExpression;

    /// <summary>
    /// Gets or sets the unique name for this schedule entry.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Gets or sets the task signature to execute.
    /// </summary>
    public required Signature Task { get; init; }

    /// <summary>
    /// Gets or sets the cron expression for scheduling.
    /// Format: "minute hour day month weekday"
    /// Examples: "*/5 * * * *" (every 5 minutes), "0 0 * * *" (daily at midnight)
    /// </summary>
    public string? Cron { get; init; }

    /// <summary>
    /// Gets or sets the interval for scheduling (alternative to cron).
    /// </summary>
    public TimeSpan? Interval { get; init; }

    /// <summary>
    /// Gets or sets whether this schedule is enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Gets or sets the last run time.
    /// </summary>
    public DateTimeOffset? LastRunTime { get; set; }

    /// <summary>
    /// Gets or sets additional options for task execution.
    /// </summary>
    public ScheduleOptions? Options { get; init; }

    /// <summary>
    /// Gets the next scheduled run time.
    /// </summary>
    /// <param name="from">The time to calculate from.</param>
    /// <returns>The next run time, or null if schedule is invalid.</returns>
    public DateTimeOffset? GetNextRunTime(DateTimeOffset from)
    {
        if (!Enabled)
        {
            return null;
        }

        if (Interval.HasValue)
        {
            if (LastRunTime.HasValue)
            {
                return LastRunTime.Value + Interval.Value;
            }

            return from + Interval.Value;
        }

        if (!string.IsNullOrEmpty(Cron))
        {
            _cronExpression ??= CronExpression.Parse(Cron);
            return _cronExpression.GetNextOccurrence(from, TimeZoneInfo.Utc);
        }

        return null;
    }

    /// <summary>
    /// Checks if the entry should run now.
    /// </summary>
    /// <param name="now">The current time.</param>
    /// <returns>True if the entry should run.</returns>
    public bool ShouldRun(DateTimeOffset now)
    {
        if (!Enabled)
        {
            return false;
        }

        var nextRun = GetNextRunTime(LastRunTime ?? now.AddDays(-1));
        return nextRun.HasValue && nextRun.Value <= now;
    }
}

/// <summary>
/// Additional options for scheduled task execution.
/// </summary>
public sealed class ScheduleOptions
{
    /// <summary>
    /// Gets or sets the target queue override.
    /// </summary>
    public string? Queue { get; init; }

    /// <summary>
    /// Gets or sets the task priority.
    /// </summary>
    public int? Priority { get; init; }

    /// <summary>
    /// Gets or sets the task expiration time relative to scheduled time.
    /// </summary>
    public TimeSpan? ExpiresIn { get; init; }

    /// <summary>
    /// Gets or sets whether to use relative ETA (time from now) instead of absolute.
    /// </summary>
    public bool RelativeEta { get; init; }
}
