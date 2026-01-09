using System.Net.Sockets;

namespace DotCelery.Core.Resilience;

/// <summary>
/// Provides retry functionality for transient failures.
/// Self-contained implementation without external dependencies.
/// </summary>
public sealed class RetryPolicy
{
    private readonly int _maxRetries;
    private readonly TimeSpan _initialDelay;
    private readonly TimeSpan _maxDelay;
    private readonly double _backoffMultiplier;
    private readonly Func<Exception, bool>? _shouldRetry;

    /// <summary>
    /// Initializes a new instance of the <see cref="RetryPolicy"/> class.
    /// </summary>
    /// <param name="maxRetries">Maximum number of retry attempts.</param>
    /// <param name="initialDelay">Initial delay between retries.</param>
    /// <param name="maxDelay">Maximum delay between retries.</param>
    /// <param name="backoffMultiplier">Multiplier for exponential backoff.</param>
    /// <param name="shouldRetry">Optional predicate to determine if an exception should trigger a retry.</param>
    public RetryPolicy(
        int maxRetries = 3,
        TimeSpan? initialDelay = null,
        TimeSpan? maxDelay = null,
        double backoffMultiplier = 2.0,
        Func<Exception, bool>? shouldRetry = null
    )
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxRetries);
        ArgumentOutOfRangeException.ThrowIfLessThan(backoffMultiplier, 1.0);

        _maxRetries = maxRetries;
        _initialDelay = initialDelay ?? TimeSpan.FromMilliseconds(100);
        _maxDelay = maxDelay ?? TimeSpan.FromSeconds(30);
        _backoffMultiplier = backoffMultiplier;
        _shouldRetry = shouldRetry;
    }

    /// <summary>
    /// Creates a default retry policy for database operations.
    /// </summary>
    public static RetryPolicy Default { get; } =
        new(
            maxRetries: 3,
            initialDelay: TimeSpan.FromMilliseconds(100),
            maxDelay: TimeSpan.FromSeconds(5),
            backoffMultiplier: 2.0
        );

    /// <summary>
    /// Creates a retry policy with no retries (fail fast).
    /// </summary>
    public static RetryPolicy None { get; } = new(maxRetries: 0);

    /// <summary>
    /// Executes an action with retry logic.
    /// </summary>
    /// <param name="action">The action to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async ValueTask ExecuteAsync(
        Func<CancellationToken, ValueTask> action,
        CancellationToken cancellationToken = default
    )
    {
        var attempt = 0;
        var delay = _initialDelay;

        while (true)
        {
            try
            {
                await action(cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (ShouldRetry(ex, attempt))
            {
                attempt++;
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                delay = CalculateNextDelay(delay);
            }
        }
    }

    /// <summary>
    /// Executes a function with retry logic and returns the result.
    /// </summary>
    /// <typeparam name="T">The return type.</typeparam>
    /// <param name="func">The function to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the function.</returns>
    public async ValueTask<T> ExecuteAsync<T>(
        Func<CancellationToken, ValueTask<T>> func,
        CancellationToken cancellationToken = default
    )
    {
        var attempt = 0;
        var delay = _initialDelay;

        while (true)
        {
            try
            {
                return await func(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ShouldRetry(ex, attempt))
            {
                attempt++;
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                delay = CalculateNextDelay(delay);
            }
        }
    }

    /// <summary>
    /// Executes a Task-returning function with retry logic.
    /// </summary>
    /// <typeparam name="T">The return type.</typeparam>
    /// <param name="func">The function to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the function.</returns>
    public async Task<T> ExecuteTaskAsync<T>(
        Func<CancellationToken, Task<T>> func,
        CancellationToken cancellationToken = default
    )
    {
        var attempt = 0;
        var delay = _initialDelay;

        while (true)
        {
            try
            {
                return await func(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ShouldRetry(ex, attempt))
            {
                attempt++;
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                delay = CalculateNextDelay(delay);
            }
        }
    }

    private bool ShouldRetry(Exception ex, int attempt)
    {
        if (attempt >= _maxRetries)
        {
            return false;
        }

        // Don't retry on cancellation
        if (ex is OperationCanceledException)
        {
            return false;
        }

        // Use custom predicate if provided
        if (_shouldRetry is not null)
        {
            return _shouldRetry(ex);
        }

        // Default: retry on transient exceptions
        return IsTransientException(ex);
    }

    private static bool IsTransientException(Exception ex)
    {
        // Common transient exception types
        return ex is TimeoutException or IOException or SocketException
            // Redis-specific
            || ex.GetType().Name.Contains("RedisConnectionException", StringComparison.Ordinal)
            || ex.GetType().Name.Contains("RedisTimeoutException", StringComparison.Ordinal)
            // PostgreSQL-specific
            || ex.GetType().Name.Contains("NpgsqlException", StringComparison.Ordinal)
            // Generic transient indicators
            || ex.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase)
            || ex.Message.Contains("connection", StringComparison.OrdinalIgnoreCase)
            || ex.Message.Contains("transient", StringComparison.OrdinalIgnoreCase);
    }

    private TimeSpan CalculateNextDelay(TimeSpan currentDelay)
    {
        // Add jitter (±25%)
        var jitter = Random.Shared.NextDouble() * 0.5 - 0.25;
        var nextDelay = TimeSpan.FromTicks(
            (long)(currentDelay.Ticks * _backoffMultiplier * (1 + jitter))
        );

        return nextDelay > _maxDelay ? _maxDelay : nextDelay;
    }
}
