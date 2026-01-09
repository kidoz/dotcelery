using DotCelery.Core.TimeLimits;

namespace DotCelery.Core.Attributes;

/// <summary>
/// Specifies time limits for task execution.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class TimeLimitAttribute : Attribute
{
    /// <summary>
    /// Gets or sets the soft time limit in seconds.
    /// When exceeded, a SoftTimeLimitExceededException is thrown but the task can catch it and perform cleanup.
    /// </summary>
    public int SoftLimitSeconds { get; set; }

    /// <summary>
    /// Gets or sets the hard time limit in seconds.
    /// When exceeded, the task's CancellationToken is cancelled forcing immediate termination.
    /// </summary>
    public int HardLimitSeconds { get; set; }

    /// <summary>
    /// Converts this attribute to a <see cref="TimeLimitPolicy"/>.
    /// </summary>
    /// <returns>A new TimeLimitPolicy based on this attribute's settings.</returns>
    public TimeLimitPolicy ToPolicy()
    {
        TimeSpan? softLimit = SoftLimitSeconds > 0 ? TimeSpan.FromSeconds(SoftLimitSeconds) : null;
        TimeSpan? hardLimit = HardLimitSeconds > 0 ? TimeSpan.FromSeconds(HardLimitSeconds) : null;

        return new TimeLimitPolicy(SoftLimit: softLimit, HardLimit: hardLimit);
    }
}
