using DotCelery.Core.Models;

namespace DotCelery.Core.Exceptions;

/// <summary>
/// Exception thrown when task execution fails.
/// </summary>
public sealed class TaskExecutionException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="TaskExecutionException"/> class.
    /// </summary>
    public TaskExecutionException()
        : base("Task execution failed") { }

    /// <summary>
    /// Initializes a new instance of the <see cref="TaskExecutionException"/> class.
    /// </summary>
    /// <param name="result">The task result.</param>
    public TaskExecutionException(TaskResult result)
        : base(GetMessage(result))
    {
        Result = result;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="TaskExecutionException"/> class.
    /// </summary>
    /// <param name="message">The message.</param>
    public TaskExecutionException(string message)
        : base(message) { }

    /// <summary>
    /// Initializes a new instance of the <see cref="TaskExecutionException"/> class.
    /// </summary>
    /// <param name="message">The message.</param>
    /// <param name="innerException">The inner exception.</param>
    public TaskExecutionException(string message, Exception? innerException)
        : base(message, innerException) { }

    /// <summary>
    /// Gets the task result.
    /// </summary>
    public TaskResult? Result { get; }

    private static string GetMessage(TaskResult result)
    {
        if (result.Exception is not null)
        {
            return $"Task {result.TaskId} failed: {result.Exception.Type}: {result.Exception.Message}";
        }

        return $"Task {result.TaskId} failed with state {result.State}";
    }
}
