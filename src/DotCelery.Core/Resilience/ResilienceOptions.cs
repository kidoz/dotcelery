namespace DotCelery.Core.Resilience;

/// <summary>
/// Options for configuring resilience behavior in stores.
/// </summary>
public sealed class ResilienceOptions
{
    /// <summary>
    /// Gets or sets whether retry is enabled. Defaults to true.
    /// </summary>
    public bool EnableRetry { get; set; } = true;

    /// <summary>
    /// Gets or sets the maximum number of retry attempts. Defaults to 3.
    /// </summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// Gets or sets the initial delay between retries. Defaults to 100ms.
    /// </summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Gets or sets the maximum delay between retries. Defaults to 5 seconds.
    /// </summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the backoff multiplier for exponential backoff. Defaults to 2.0.
    /// </summary>
    public double BackoffMultiplier { get; set; } = 2.0;

    /// <summary>
    /// Creates a <see cref="RetryPolicy"/> from these options.
    /// </summary>
    /// <param name="shouldRetry">Optional predicate to determine if an exception should trigger a retry.</param>
    /// <returns>A configured retry policy.</returns>
    public RetryPolicy CreatePolicy(Func<Exception, bool>? shouldRetry = null)
    {
        if (!EnableRetry)
        {
            return RetryPolicy.None;
        }

        return new RetryPolicy(
            maxRetries: MaxRetries,
            initialDelay: InitialDelay,
            maxDelay: MaxDelay,
            backoffMultiplier: BackoffMultiplier,
            shouldRetry: shouldRetry
        );
    }
}
