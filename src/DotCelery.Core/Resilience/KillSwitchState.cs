namespace DotCelery.Core.Resilience;

/// <summary>
/// Kill switch states.
/// </summary>
public enum KillSwitchState
{
    /// <summary>
    /// Normal operation - consuming allowed.
    /// </summary>
    Ready,

    /// <summary>
    /// Monitoring failures - tracking failure rate.
    /// </summary>
    Tracking,

    /// <summary>
    /// Kill switch tripped - consuming paused.
    /// </summary>
    Tripped,

    /// <summary>
    /// Restarting - waiting for restart timeout to elapse.
    /// </summary>
    Restarting,
}
