using DotCelery.Core.RateLimiting;
using DotCelery.Core.TimeLimits;

namespace DotCelery.Worker.Registry;

/// <summary>
/// Registration info for a task type.
/// </summary>
/// <param name="TaskName">The task name.</param>
/// <param name="TaskType">The task implementation type.</param>
/// <param name="InputType">The input type.</param>
/// <param name="OutputType">The output type (null for void tasks).</param>
/// <param name="RateLimitPolicy">The rate limit policy for this task (null if not rate-limited).</param>
/// <param name="FilterTypes">The filter types to apply to this task (null if none).</param>
/// <param name="Queue">The default queue for this task (null to use default).</param>
/// <param name="TimeLimitPolicy">The time limit policy for this task (null if no time limits).</param>
public sealed record TaskRegistration(
    string TaskName,
    Type TaskType,
    Type? InputType,
    Type? OutputType,
    RateLimitPolicy? RateLimitPolicy = null,
    IReadOnlyList<Type>? FilterTypes = null,
    string? Queue = null,
    TimeLimitPolicy? TimeLimitPolicy = null
);
