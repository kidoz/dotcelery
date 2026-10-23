namespace DotCelery.Core.Exceptions;

/// <summary>
/// Exception thrown when a task type is not registered.
/// </summary>
public sealed class UnknownTaskException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="UnknownTaskException"/> class.
    /// </summary>
    public UnknownTaskException()
        : base("Unknown task")
    {
        TaskName = string.Empty;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="UnknownTaskException"/> class.
    /// </summary>
    /// <param name="taskName">The unknown task name.</param>
    public UnknownTaskException(string taskName)
        : base($"Unknown task: {taskName}")
    {
        TaskName = taskName;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="UnknownTaskException"/> class.
    /// </summary>
    /// <param name="taskName">The unknown task name.</param>
    /// <param name="innerException">The inner exception.</param>
    public UnknownTaskException(string taskName, Exception? innerException)
        : base($"Unknown task: {taskName}", innerException)
    {
        TaskName = taskName;
    }

    /// <summary>
    /// Gets the unknown task name.
    /// </summary>
    public string TaskName { get; }
}
