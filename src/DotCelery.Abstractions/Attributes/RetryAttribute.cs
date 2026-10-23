using DotCelery.Core.Resilience;

namespace DotCelery.Core.Attributes;

/// <summary>
/// Specifies retry semantics for a task. Apply to task classes to override the
/// worker's default <see cref="RetryPolicy"/> for that task.
/// </summary>
/// <example>
/// <code>
/// [Retry(MaxRetries = 5, Strategy = BackoffStrategy.Exponential, InitialDelaySeconds = 2)]
/// public class MyTask : ITask&lt;Input, Output&gt; { ... }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class RetryAttribute : Attribute
{
    /// <summary>
    /// Gets the maximum number of retries. Default: 3.
    /// </summary>
    public int MaxRetries { get; init; } = 3;

    /// <summary>
    /// Gets the backoff strategy used to compute the delay before a retry.
    /// Default: <see cref="BackoffStrategy.Exponential"/>.
    /// </summary>
    public BackoffStrategy Strategy { get; init; } = BackoffStrategy.Exponential;

    /// <summary>
    /// Gets the delay before the first retry, in seconds. Default: 1.
    /// </summary>
    public double InitialDelaySeconds { get; init; } = 1;

    /// <summary>
    /// Gets the longest delay between retries, in seconds. Default: 60.
    /// </summary>
    public double MaxDelaySeconds { get; init; } = 60;

    /// <summary>
    /// Gets the multiplier applied to the delay after each retry when
    /// <see cref="Strategy"/> is <see cref="BackoffStrategy.Exponential"/>. Default: 2.0.
    /// </summary>
    public double BackoffMultiplier { get; init; } = 2.0;

    /// <summary>
    /// Gets whether the computed delay is randomized into [50%, 100%] of its value.
    /// Default: true.
    /// </summary>
    public bool Jitter { get; init; } = true;

    /// <summary>
    /// Gets the exception types that may be retried. When null, every exception is
    /// retryable unless it matches <see cref="DontRetryFor"/>.
    /// </summary>
    public Type[]? RetryFor { get; init; }

    /// <summary>
    /// Gets the exception types that must never be retried. This list always wins
    /// over <see cref="RetryFor"/>.
    /// </summary>
    public Type[]? DontRetryFor { get; init; }

    /// <summary>
    /// Converts this attribute to a validated <see cref="RetryPolicy"/>.
    /// </summary>
    /// <returns>A new RetryPolicy based on this attribute's settings.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown when the attribute's settings form an invalid policy.
    /// </exception>
    public RetryPolicy ToPolicy()
    {
        var policy = new RetryPolicy
        {
            MaxRetries = MaxRetries,
            Strategy = Strategy,
            InitialDelay = TimeSpan.FromSeconds(InitialDelaySeconds),
            MaxDelay = TimeSpan.FromSeconds(MaxDelaySeconds),
            BackoffMultiplier = BackoffMultiplier,
            Jitter = Jitter,
            RetryableExceptions = RetryFor,
            NonRetryableExceptions = DontRetryFor,
        };

        policy.Validate();

        return policy;
    }
}
