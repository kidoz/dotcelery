namespace DotCelery.Client;

/// <summary>
/// Reports a broker publication failure whose acceptance may be ambiguous. Retry with the
/// same <see cref="TaskId"/> rather than generating a new invocation ID.
/// </summary>
public sealed class TaskSubmissionException : Exception
{
    /// <summary>Gets the invocation ID assigned before publication.</summary>
    public string TaskId { get; }

    /// <summary>Initializes a publication failure with its invocation ID.</summary>
    /// <param name="taskId">The invocation ID.</param>
    /// <param name="innerException">The broker error.</param>
    public TaskSubmissionException(string taskId, Exception innerException)
        : base(
            $"Publication of task {taskId} failed; broker acceptance may be ambiguous.",
            innerException
        )
    {
        TaskId = taskId;
    }
}
