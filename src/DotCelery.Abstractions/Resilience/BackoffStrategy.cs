namespace DotCelery.Core.Resilience;

/// <summary>
/// The backoff strategy a <see cref="RetryPolicy"/> uses to compute the delay before a retry.
/// </summary>
public enum BackoffStrategy
{
    /// <summary>
    /// Every retry waits the same delay.
    /// </summary>
    Constant,

    /// <summary>
    /// Each retry waits longer than the previous one, scaled by the backoff multiplier.
    /// </summary>
    Exponential,
}
