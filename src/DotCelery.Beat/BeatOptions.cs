namespace DotCelery.Beat;

/// <summary>
/// Configuration options for the Beat scheduler.
/// </summary>
public sealed class BeatOptions
{
    /// <summary>
    /// Gets or sets the schedule check interval.
    /// </summary>
    public TimeSpan CheckInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets or sets the maximum jitter to add to task execution times.
    /// Jitter helps prevent thundering herd problems.
    /// </summary>
    public TimeSpan MaxJitter { get; set; } = TimeSpan.Zero;

    /// <summary>
    /// Gets or sets whether to remember when each entry last ran, so a restart resumes the
    /// schedule instead of starting it over.
    /// </summary>
    public bool PersistState { get; set; }

    /// <summary>
    /// Gets or sets the state persistence path. Defaults to
    /// <c>{SchedulerName}.state.json</c> in the current directory.
    /// </summary>
    public string? StatePath { get; set; }

    /// <summary>
    /// Gets or sets the scheduler name for identification. Schedulers that share the
    /// configured storage elect one of themselves per name.
    /// </summary>
    public string SchedulerName { get; set; } = "celery-beat";

    /// <summary>
    /// Gets or sets whether to run a schedule entry once at startup when it was missed while
    /// the scheduler was stopped. When off, missed runs are skipped and the entry continues
    /// from the next interval or cron occurrence.
    /// </summary>
    public bool RunMissedOnStartup { get; set; }
}
