namespace DotCelery.Core.Models;

/// <summary>
/// Result of task execution.
/// </summary>
public sealed record TaskResult
{
    /// <summary>
    /// Gets the task invocation ID.
    /// </summary>
    public required string TaskId { get; init; }

    /// <summary>
    /// Gets the final task state.
    /// </summary>
    public required TaskState State { get; init; }

    /// <summary>
    /// Gets the serialized result data (for Success state).
    /// </summary>
#pragma warning disable CA1819 // Properties should not return arrays - Required for serialization
    public byte[]? Result { get; init; }
#pragma warning restore CA1819

    /// <summary>
    /// Gets the content type of Result.
    /// </summary>
    public string? ContentType { get; init; }

    /// <summary>
    /// Gets the exception info (for Failure state).
    /// </summary>
    public TaskExceptionInfo? Exception { get; init; }

    /// <summary>
    /// Gets when the task completed.
    /// </summary>
    public required DateTimeOffset CompletedAt { get; init; }

    /// <summary>
    /// Gets the total execution duration.
    /// </summary>
    public required TimeSpan Duration { get; init; }

    /// <summary>
    /// Gets the number of retries performed.
    /// </summary>
    public int Retries { get; init; }

    /// <summary>
    /// Gets the worker that executed the task.
    /// </summary>
    public string? Worker { get; init; }

    /// <summary>
    /// Gets the custom metadata.
    /// </summary>
    public IReadOnlyDictionary<string, object>? Metadata { get; init; }

    /// <summary>
    /// Gets the suggested delay before retrying (for rate-limited tasks).
    /// </summary>
    public TimeSpan? RetryAfter { get; init; }

    /// <summary>
    /// Gets the delay before requeueing the message.
    /// Used when a filter or rate limiter requests a requeue with a delay to prevent hot loops.
    /// </summary>
    public TimeSpan? RequeueDelay { get; init; }

    /// <summary>
    /// Gets whether retry count should not be incremented.
    /// True for rate-limited retries where the task never executed.
    /// </summary>
    public bool DoNotIncrementRetries { get; init; }
}

/// <summary>
/// Serializable exception information.
/// </summary>
public sealed record TaskExceptionInfo
{
    /// <summary>
    /// Gets the exception type name.
    /// </summary>
    public required string Type { get; init; }

    /// <summary>
    /// Gets the exception message.
    /// </summary>
    public required string Message { get; init; }

    /// <summary>
    /// Gets the stack trace (if available).
    /// </summary>
    public string? StackTrace { get; init; }

    /// <summary>
    /// Gets the inner exception (if any).
    /// </summary>
    public TaskExceptionInfo? InnerException { get; init; }

    /// <summary>
    /// Creates a <see cref="TaskExceptionInfo"/> from an exception.
    /// </summary>
    /// <param name="exception">The exception.</param>
    /// <returns>The exception info.</returns>
    public static TaskExceptionInfo FromException(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        return new TaskExceptionInfo
        {
            Type = exception.GetType().FullName ?? exception.GetType().Name,
            Message = exception.Message,
            StackTrace = exception.StackTrace,
            InnerException = exception.InnerException is not null
                ? FromException(exception.InnerException)
                : null,
        };
    }
}
