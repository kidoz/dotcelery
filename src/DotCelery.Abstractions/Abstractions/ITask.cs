namespace DotCelery.Core.Abstractions;

/// <summary>
/// Base interface for all Celery tasks.
/// </summary>
public interface ITask
{
    /// <summary>
    /// Gets the unique name for this task type used for routing.
    /// </summary>
    static abstract string TaskName { get; }
}

/// <summary>
/// Task that accepts input and produces output.
/// </summary>
/// <typeparam name="TInput">Input type (must be serializable).</typeparam>
/// <typeparam name="TOutput">Output type (must be serializable).</typeparam>
public interface ITask<TInput, TOutput> : ITask
    where TInput : class
    where TOutput : class
{
    /// <summary>
    /// Executes the task logic.
    /// </summary>
    /// <param name="input">Task input data.</param>
    /// <param name="context">Execution context with utilities.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Task result.</returns>
    Task<TOutput> ExecuteAsync(
        TInput input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    );
}

/// <summary>
/// Task that accepts input with no return value.
/// </summary>
/// <typeparam name="TInput">Input type (must be serializable).</typeparam>
public interface ITask<TInput> : ITask
    where TInput : class
{
    /// <summary>
    /// Executes the task logic.
    /// </summary>
    /// <param name="input">Task input data.</param>
    /// <param name="context">Execution context with utilities.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task ExecuteAsync(
        TInput input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    );
}
