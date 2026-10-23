using DotCelery.Core.Resilience;

namespace DotCelery.Worker.Resilience;

/// <summary>
/// Configuration options for task-execution resilience: the default retry policy,
/// automatic retry on failure, and the worker-side limit on publisher-supplied retries.
/// </summary>
public sealed class ResilienceOptions
{
    /// <summary>
    /// Gets or sets the retry policy applied to tasks that do not declare one with
    /// <see cref="DotCelery.Core.Attributes.RetryAttribute"/>.
    /// Default: null, which preserves the behavior without a policy (a requested retry
    /// without an explicit countdown is republished immediately).
    /// </summary>
    public RetryPolicy? DefaultRetryPolicy { get; set; }

    /// <summary>
    /// Gets or sets whether an unhandled task exception that passes the effective policy's
    /// <see cref="RetryPolicy.ShouldRetry"/> and has retries remaining is retried with the
    /// computed backoff instead of recorded as a failure. Default: false.
    /// </summary>
    public bool AutoRetryOnFailure { get; set; }

    /// <summary>
    /// Gets or sets the worker-side limit applied to the publisher-supplied
    /// MaxRetries on every message. Default: 100.
    /// </summary>
    public int MaxRetriesLimit { get; set; } = 100;

    /// <summary>
    /// Validates the options.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when <see cref="MaxRetriesLimit"/> is negative or
    /// <see cref="DefaultRetryPolicy"/> is invalid.
    /// </exception>
    public void Validate()
    {
        if (MaxRetriesLimit < 0)
        {
            throw new InvalidOperationException("MaxRetriesLimit cannot be negative.");
        }

        try
        {
            DefaultRetryPolicy?.Validate();
        }
        catch (ArgumentException ex)
        {
            throw new InvalidOperationException($"DefaultRetryPolicy is invalid: {ex.Message}", ex);
        }
    }
}
