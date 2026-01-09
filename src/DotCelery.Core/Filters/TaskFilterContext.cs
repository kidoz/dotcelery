using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;

namespace DotCelery.Core.Filters;

/// <summary>
/// Context for <see cref="ITaskFilter.OnExecutingAsync"/> filter method.
/// </summary>
public sealed class TaskExecutingContext
{
    /// <summary>
    /// Gets the unique task invocation ID.
    /// </summary>
    public required string TaskId { get; init; }

    /// <summary>
    /// Gets the task name for routing.
    /// </summary>
    public required string TaskName { get; init; }

    /// <summary>
    /// Gets the original task message.
    /// </summary>
    public required TaskMessage Message { get; init; }

    /// <summary>
    /// Gets the deserialized task input (may be null for tasks without input).
    /// </summary>
    public object? Input { get; init; }

    /// <summary>
    /// Gets the task implementation type.
    /// </summary>
    public required Type TaskType { get; init; }

    /// <summary>
    /// Gets the task execution context.
    /// </summary>
    public required ITaskContext TaskContext { get; init; }

    /// <summary>
    /// Gets the service provider for the current scope.
    /// </summary>
    public required IServiceProvider ServiceProvider { get; init; }

    /// <summary>
    /// Gets the current retry count (0-based).
    /// </summary>
    public int RetryCount { get; init; }

    /// <summary>
    /// Gets or sets custom properties that flow through the filter pipeline.
    /// </summary>
    public IDictionary<string, object?> Properties { get; } = new Dictionary<string, object?>();

    /// <summary>
    /// Gets or sets whether to skip task execution.
    /// When true, the task will not be executed and <see cref="SkipResult"/> will be used as the output.
    /// </summary>
    public bool SkipExecution { get; set; }

    /// <summary>
    /// Gets or sets the result to use when <see cref="SkipExecution"/> is true.
    /// This is the task's return value (before wrapping in TaskResult).
    /// </summary>
    public object? Result { get; set; }

    /// <summary>
    /// Gets or sets the full task result to use when <see cref="SkipExecution"/> is true.
    /// If set, this takes precedence over <see cref="Result"/>.
    /// Use this when the filter needs to set the entire TaskResult (e.g., for rejected tasks).
    /// </summary>
    public TaskResult? SkipResult { get; set; }

    /// <summary>
    /// Gets or sets whether to requeue the message when <see cref="SkipExecution"/> is true.
    /// When true, the message will be rejected with requeue instead of being acked.
    /// </summary>
    public bool RequeueMessage { get; set; }

    /// <summary>
    /// Gets or sets the delay before requeueing the message.
    /// Used to prevent hot requeue loops when a partition is locked or other conditions prevent execution.
    /// Only applies when <see cref="RequeueMessage"/> is true.
    /// </summary>
    public TimeSpan? RequeueDelay { get; set; }
}

/// <summary>
/// Context for <see cref="ITaskFilter.OnExecutedAsync"/> filter method.
/// </summary>
public sealed class TaskExecutedContext
{
    /// <summary>
    /// Gets the unique task invocation ID.
    /// </summary>
    public required string TaskId { get; init; }

    /// <summary>
    /// Gets the task name for routing.
    /// </summary>
    public required string TaskName { get; init; }

    /// <summary>
    /// Gets the original task message.
    /// </summary>
    public required TaskMessage Message { get; init; }

    /// <summary>
    /// Gets the deserialized task input (may be null for tasks without input).
    /// </summary>
    public object? Input { get; init; }

    /// <summary>
    /// Gets the task implementation type.
    /// </summary>
    public required Type TaskType { get; init; }

    /// <summary>
    /// Gets the task execution context.
    /// </summary>
    public required ITaskContext TaskContext { get; init; }

    /// <summary>
    /// Gets the service provider for the current scope.
    /// </summary>
    public required IServiceProvider ServiceProvider { get; init; }

    /// <summary>
    /// Gets the task execution duration.
    /// </summary>
    public required TimeSpan Duration { get; init; }

    /// <summary>
    /// Gets or sets the task result value (null for void tasks or failures).
    /// Filters can modify this value.
    /// </summary>
    public object? Result { get; set; }

    /// <summary>
    /// Gets or sets the full task result (including state, metadata, etc.).
    /// This is set by the executor after task completion.
    /// </summary>
    public TaskResult? TaskResult { get; set; }

    /// <summary>
    /// Gets or sets the exception that occurred during execution (null if successful).
    /// Filters can modify this to replace or suppress exceptions.
    /// </summary>
    public Exception? Exception { get; set; }

    /// <summary>
    /// Gets whether the task execution succeeded (no exception).
    /// </summary>
    public bool Succeeded => Exception is null;

    /// <summary>
    /// Gets the custom properties that flowed through the filter pipeline.
    /// </summary>
    public IDictionary<string, object?> Properties { get; init; } =
        new Dictionary<string, object?>();
}

/// <summary>
/// Context for <see cref="ITaskExceptionFilter.OnExceptionAsync"/> filter method.
/// </summary>
public sealed class TaskExceptionContext
{
    /// <summary>
    /// Gets the unique task invocation ID.
    /// </summary>
    public required string TaskId { get; init; }

    /// <summary>
    /// Gets the task name for routing.
    /// </summary>
    public required string TaskName { get; init; }

    /// <summary>
    /// Gets the deserialized task input (may be null for tasks without input).
    /// </summary>
    public object? Input { get; init; }

    /// <summary>
    /// Gets the task implementation type.
    /// </summary>
    public required Type TaskType { get; init; }

    /// <summary>
    /// Gets the task execution context.
    /// </summary>
    public required ITaskContext TaskContext { get; init; }

    /// <summary>
    /// Gets the service provider for the current scope.
    /// </summary>
    public required IServiceProvider ServiceProvider { get; init; }

    /// <summary>
    /// Gets the exception that occurred during execution.
    /// </summary>
    public required Exception Exception { get; init; }

    /// <summary>
    /// Gets or sets whether the exception has been handled.
    /// When true, the exception will not propagate and <see cref="Result"/> will be used.
    /// </summary>
    public bool ExceptionHandled { get; set; }

    /// <summary>
    /// Gets or sets the result to use when <see cref="ExceptionHandled"/> is true.
    /// </summary>
    public object? Result { get; set; }

    /// <summary>
    /// Gets the custom properties from the filter pipeline.
    /// </summary>
    public IDictionary<string, object?> Properties { get; init; } =
        new Dictionary<string, object?>();
}
