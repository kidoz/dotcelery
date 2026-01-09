namespace DotCelery.Cron;

/// <summary>
/// Specifies the format of a cron expression.
/// </summary>
public enum CronFormat
{
    /// <summary>
    /// Standard 5-field format: minute hour day-of-month month day-of-week.
    /// Example: "*/5 * * * *" (every 5 minutes)
    /// </summary>
    Standard = 0,

    /// <summary>
    /// Extended 6-field format with seconds: second minute hour day-of-month month day-of-week.
    /// Example: "0 */5 * * * *" (every 5 minutes at second 0)
    /// </summary>
    IncludeSeconds = 1,

    /// <summary>
    /// Extended 6-field format with year: minute hour day-of-month month day-of-week year.
    /// Example: "0 0 1 1 * 2025" (midnight on Jan 1, 2025)
    /// </summary>
    IncludeYear = 2,

    /// <summary>
    /// Full 7-field format with seconds and year: second minute hour day-of-month month day-of-week year.
    /// Example: "0 0 0 1 1 * 2025" (midnight on Jan 1, 2025 at second 0)
    /// </summary>
    IncludeSecondsAndYear = 3,
}
