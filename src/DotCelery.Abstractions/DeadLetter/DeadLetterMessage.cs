namespace DotCelery.Core.DeadLetter;

/// <summary>
/// Reasons for a task being sent to the dead letter queue.
/// </summary>
public enum DeadLetterReason
{
    /// <summary>
    /// Task exceeded maximum retry attempts.
    /// </summary>
    MaxRetriesExceeded,

    /// <summary>
    /// Task was explicitly rejected by a filter or handler.
    /// </summary>
    Rejected,

    /// <summary>
    /// Task exceeded its time limit.
    /// </summary>
    TimeLimitExceeded,

    /// <summary>
    /// Task message expired before processing.
    /// </summary>
    Expired,

    /// <summary>
    /// Task type was not found in the registry.
    /// </summary>
    UnknownTask,

    /// <summary>
    /// Task failed due to an unhandled exception.
    /// </summary>
    Failed,

    /// <summary>
    /// Message could not be deserialized.
    /// </summary>
    DeserializationFailed,
}

/// <summary>
/// Represents a message in the dead letter queue.
/// </summary>
public sealed record DeadLetterMessage
{
    /// <summary>
    /// Gets the unique message ID in the DLQ.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Gets the original task ID.
    /// </summary>
    public required string TaskId { get; init; }

    /// <summary>
    /// Gets the task name.
    /// </summary>
    public required string TaskName { get; init; }

    /// <summary>
    /// Gets the original queue.
    /// </summary>
    public required string Queue { get; init; }

    /// <summary>
    /// Gets the reason for dead-lettering.
    /// </summary>
    public required DeadLetterReason Reason { get; init; }

    /// <summary>
    /// Gets the serialized original task message.
    /// </summary>
    public required byte[] OriginalMessage { get; init; }

    /// <summary>
    /// Gets the exception message (if failed due to an exception).
    /// </summary>
    public string? ExceptionMessage { get; init; }

    /// <summary>
    /// Gets the exception type (if failed due to an exception).
    /// </summary>
    public string? ExceptionType { get; init; }

    /// <summary>
    /// Gets the exception stack trace (if failed due to an exception).
    /// </summary>
    public string? StackTrace { get; init; }

    /// <summary>
    /// Gets the number of retry attempts before dead-lettering.
    /// </summary>
    public int RetryCount { get; init; }

    /// <summary>
    /// Gets when the message was added to the DLQ.
    /// </summary>
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets when the message expires from the DLQ.
    /// </summary>
    public DateTimeOffset? ExpiresAt { get; init; }

    /// <summary>
    /// Gets the worker that processed the task.
    /// </summary>
    public string? Worker { get; init; }
}
