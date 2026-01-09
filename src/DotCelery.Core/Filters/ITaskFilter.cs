namespace DotCelery.Core.Filters;

/// <summary>
/// Filter that executes before and after task execution.
/// Filters run in order of <see cref="Order"/> property (ascending on executing, descending on executed).
/// </summary>
public interface ITaskFilter
{
    /// <summary>
    /// Gets the order of execution. Lower values execute first on entry, last on exit.
    /// Default is 0.
    /// </summary>
    int Order => 0;

    /// <summary>
    /// Called before task execution begins.
    /// </summary>
    /// <param name="context">The executing context with task details.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask OnExecutingAsync(
        TaskExecutingContext context,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Called after task execution completes (success or failure).
    /// </summary>
    /// <param name="context">The executed context with result details.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask OnExecutedAsync(
        TaskExecutedContext context,
        CancellationToken cancellationToken = default
    );
}

/// <summary>
/// Filter specifically for handling exceptions during task execution.
/// </summary>
public interface ITaskExceptionFilter
{
    /// <summary>
    /// Called when task execution throws an exception.
    /// </summary>
    /// <param name="context">The exception context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the exception was handled and should not propagate, false otherwise.</returns>
    ValueTask<bool> OnExceptionAsync(
        TaskExceptionContext context,
        CancellationToken cancellationToken = default
    );
}

/// <summary>
/// Combined filter interface for convenience.
/// Implements both <see cref="ITaskFilter"/> and <see cref="ITaskExceptionFilter"/>.
/// </summary>
public interface ITaskFilterWithExceptionHandling : ITaskFilter, ITaskExceptionFilter;
