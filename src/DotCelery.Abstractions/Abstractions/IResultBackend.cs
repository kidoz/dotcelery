using DotCelery.Core.Models;

namespace DotCelery.Core.Abstractions;

/// <summary>
/// Backend for storing task results and state.
/// </summary>
public interface IResultBackend : IAsyncDisposable
{
    /// <summary>
    /// Stores task result.
    /// </summary>
    /// <param name="result">The result to store.</param>
    /// <param name="expiry">Optional expiry time for the result.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A value task representing the async operation.</returns>
    ValueTask StoreResultAsync(
        TaskResult result,
        TimeSpan? expiry = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Retrieves task result.
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The task result, or null if not found.</returns>
    ValueTask<TaskResult?> GetResultAsync(
        string taskId,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Waits for task completion and returns result.
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <param name="timeout">Optional timeout.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The task result.</returns>
    Task<TaskResult> WaitForResultAsync(
        string taskId,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Updates task state with optional metadata.
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <param name="state">The new state.</param>
    /// <param name="metadata">Optional metadata.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A value task representing the async operation.</returns>
    ValueTask UpdateStateAsync(
        string taskId,
        TaskState state,
        object? metadata = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets current task state.
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The task state, or null if not found.</returns>
    ValueTask<TaskState?> GetStateAsync(
        string taskId,
        CancellationToken cancellationToken = default
    );
}
