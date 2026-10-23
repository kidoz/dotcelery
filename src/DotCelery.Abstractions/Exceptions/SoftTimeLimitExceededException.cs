namespace DotCelery.Core.Exceptions;

/// <summary>
/// Exception thrown when a task's soft time limit has been exceeded.
/// Tasks can catch this exception to perform cleanup before termination.
/// </summary>
public sealed class SoftTimeLimitExceededException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SoftTimeLimitExceededException"/> class.
    /// </summary>
    public SoftTimeLimitExceededException()
        : base("Soft time limit exceeded.") { }

    /// <summary>
    /// Initializes a new instance of the <see cref="SoftTimeLimitExceededException"/> class.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public SoftTimeLimitExceededException(string message)
        : base(message) { }

    /// <summary>
    /// Initializes a new instance of the <see cref="SoftTimeLimitExceededException"/> class.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    public SoftTimeLimitExceededException(string message, Exception innerException)
        : base(message, innerException) { }

    /// <summary>
    /// Initializes a new instance of the <see cref="SoftTimeLimitExceededException"/> class.
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <param name="softLimit">The soft limit that was exceeded.</param>
    public SoftTimeLimitExceededException(string taskId, TimeSpan softLimit)
        : base($"Task {taskId} exceeded soft time limit of {softLimit.TotalSeconds} seconds.")
    {
        TaskId = taskId;
        SoftLimit = softLimit;
    }

    /// <summary>
    /// Gets the task ID that exceeded the time limit.
    /// </summary>
    public string? TaskId { get; }

    /// <summary>
    /// Gets the soft limit that was exceeded.
    /// </summary>
    public TimeSpan? SoftLimit { get; }
}
