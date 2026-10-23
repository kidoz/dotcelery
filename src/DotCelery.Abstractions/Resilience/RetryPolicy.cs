namespace DotCelery.Core.Resilience;

/// <summary>
/// Defines retry semantics for task execution: how long to wait before a retry
/// and which exceptions are worth retrying.
/// </summary>
/// <remarks>
/// The policy is a pure value object; the worker applies it when a task requests a retry
/// without an explicit countdown, or when automatic retry on failure is enabled.
/// </remarks>
public sealed record RetryPolicy
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
    /// Gets the delay before the first retry. Default: 1 second.
    /// </summary>
    public TimeSpan InitialDelay { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets the longest delay between retries. Default: 60 seconds.
    /// </summary>
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Gets the multiplier applied to the delay after each retry when
    /// <see cref="Strategy"/> is <see cref="BackoffStrategy.Exponential"/>. Default: 2.0.
    /// </summary>
    public double BackoffMultiplier { get; init; } = 2.0;

    /// <summary>
    /// Gets whether the computed delay is randomized into [50%, 100%] of its value
    /// to spread retries from concurrent workers. Default: true.
    /// </summary>
    public bool Jitter { get; init; } = true;

    /// <summary>
    /// Gets the exception types that may be retried. When null, every exception is
    /// retryable unless it matches <see cref="NonRetryableExceptions"/>.
    /// </summary>
    public IReadOnlyList<Type>? RetryableExceptions { get; init; }

    /// <summary>
    /// Gets the exception types that must never be retried. This list always wins
    /// over <see cref="RetryableExceptions"/>.
    /// </summary>
    public IReadOnlyList<Type>? NonRetryableExceptions { get; init; }

    /// <summary>
    /// Creates a policy that never retries.
    /// </summary>
    /// <returns>A new RetryPolicy with <see cref="MaxRetries"/> of 0.</returns>
    public static RetryPolicy None() => new() { MaxRetries = 0 };

    /// <summary>
    /// Creates the default policy: 3 retries with exponential backoff starting at 1 second,
    /// capped at 60 seconds, with jitter.
    /// </summary>
    /// <returns>A new RetryPolicy with default settings.</returns>
    public static RetryPolicy Default() => new();

    /// <summary>
    /// Creates a policy that waits the same delay before every retry.
    /// </summary>
    /// <param name="maxRetries">The maximum number of retries (default: 3).</param>
    /// <param name="delay">The delay before each retry (default: 1 second).</param>
    /// <param name="jitter">Whether to randomize the delay (default: true).</param>
    /// <returns>A new RetryPolicy with a constant backoff.</returns>
    public static RetryPolicy Constant(
        int maxRetries = 3,
        TimeSpan? delay = null,
        bool jitter = true
    ) =>
        new()
        {
            MaxRetries = maxRetries,
            Strategy = BackoffStrategy.Constant,
            InitialDelay = delay ?? TimeSpan.FromSeconds(1),
            MaxDelay = delay ?? TimeSpan.FromSeconds(1),
            Jitter = jitter,
        };

    /// <summary>
    /// Creates a policy whose delay grows by the multiplier after each retry.
    /// </summary>
    /// <param name="maxRetries">The maximum number of retries (default: 3).</param>
    /// <param name="initialDelay">The delay before the first retry (default: 1 second).</param>
    /// <param name="maxDelay">The longest delay between retries (default: 60 seconds).</param>
    /// <param name="multiplier">The multiplier applied after each retry (default: 2.0).</param>
    /// <param name="jitter">Whether to randomize the delay (default: true).</param>
    /// <returns>A new RetryPolicy with an exponential backoff.</returns>
    public static RetryPolicy Exponential(
        int maxRetries = 3,
        TimeSpan? initialDelay = null,
        TimeSpan? maxDelay = null,
        double multiplier = 2.0,
        bool jitter = true
    ) =>
        new()
        {
            MaxRetries = maxRetries,
            Strategy = BackoffStrategy.Exponential,
            InitialDelay = initialDelay ?? TimeSpan.FromSeconds(1),
            MaxDelay = maxDelay ?? TimeSpan.FromSeconds(60),
            BackoffMultiplier = multiplier,
            Jitter = jitter,
        };

    /// <summary>
    /// Computes the delay before the next retry.
    /// </summary>
    /// <param name="retriesAlreadyPerformed">
    /// How many retries the task has already performed; the first retry passes 0
    /// and waits <see cref="InitialDelay"/>.
    /// </param>
    /// <returns>The delay before the next retry.</returns>
    public TimeSpan ComputeDelay(int retriesAlreadyPerformed)
    {
        var scale =
            Strategy == BackoffStrategy.Exponential
                ? Math.Pow(BackoffMultiplier, retriesAlreadyPerformed)
                : 1.0;

        var delayMilliseconds = Math.Min(
            InitialDelay.TotalMilliseconds * scale,
            MaxDelay.TotalMilliseconds
        );

        if (Jitter)
        {
            delayMilliseconds *= 0.5 + (Random.Shared.NextDouble() * 0.5);
        }

        return TimeSpan.FromMilliseconds(delayMilliseconds);
    }

    /// <summary>
    /// Determines whether a failure with the given exception may be retried.
    /// </summary>
    /// <param name="exception">The exception the task failed with.</param>
    /// <returns>
    /// False when the exception matches <see cref="NonRetryableExceptions"/>; otherwise true
    /// when <see cref="RetryableExceptions"/> is null or the exception matches one of its entries.
    /// </returns>
    public bool ShouldRetry(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        if (
            NonRetryableExceptions is not null
            && NonRetryableExceptions.Any(t => t.IsInstanceOfType(exception))
        )
        {
            return false;
        }

        return RetryableExceptions is null
            || RetryableExceptions.Any(t => t.IsInstanceOfType(exception));
    }

    /// <summary>
    /// Validates the policy's settings.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// Thrown when a delay is negative, <see cref="MaxDelay"/> is shorter than
    /// <see cref="InitialDelay"/>, <see cref="BackoffMultiplier"/> is less than 1,
    /// or <see cref="MaxRetries"/> is negative.
    /// </exception>
    public void Validate()
    {
        if (MaxRetries < 0)
        {
            throw new ArgumentException("MaxRetries cannot be negative.", nameof(MaxRetries));
        }

        if (InitialDelay < TimeSpan.Zero)
        {
            throw new ArgumentException("InitialDelay cannot be negative.", nameof(InitialDelay));
        }

        if (MaxDelay < TimeSpan.Zero)
        {
            throw new ArgumentException("MaxDelay cannot be negative.", nameof(MaxDelay));
        }

        if (MaxDelay < InitialDelay)
        {
            throw new ArgumentException(
                "MaxDelay cannot be shorter than InitialDelay.",
                nameof(MaxDelay)
            );
        }

        if (BackoffMultiplier < 1.0)
        {
            throw new ArgumentException(
                "BackoffMultiplier cannot be less than 1.",
                nameof(BackoffMultiplier)
            );
        }
    }
}
