namespace DotCelery.Core.Abstractions;

/// <summary>
/// Invokes a task through a typed adapter, so a generated registration invokes its tasks
/// without reflection or expression compilation.
/// </summary>
public interface ITaskInvoker
{
    /// <summary>
    /// Invokes the task with its input.
    /// </summary>
    /// <param name="task">The task instance.</param>
    /// <param name="input">The task input, or <c>null</c> for a task without input.</param>
    /// <param name="context">The task context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The task's result, or <c>null</c> when it returns no value.</returns>
    Task<object?> InvokeAsync(
        object task,
        object? input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    );
}
