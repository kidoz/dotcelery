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
    /// Gets or sets whether to persist schedule state.
    /// </summary>
    public bool PersistState { get; set; }

    /// <summary>
    /// Gets or sets the state persistence path.
    /// </summary>
    public string? StatePath { get; set; }

    /// <summary>
    /// Gets or sets the scheduler name for identification.
    /// </summary>
    public string SchedulerName { get; set; } = "celery-beat";

    /// <summary>
    /// Gets or sets whether to run missed tasks on startup.
    /// </summary>
    public bool RunMissedOnStartup { get; set; }
}
