using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;

namespace DotCelery.Client;

/// <summary>
/// Client for sending tasks to the distributed queue.
/// </summary>
public interface ICeleryClient
{
    // ========== Task Revocation ==========

    /// <summary>
    /// Revokes a pending or running task.
    /// </summary>
    /// <param name="taskId">The task ID to revoke.</param>
    /// <param name="options">Optional revocation options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the revocation is recorded.</returns>
    /// <remarks>
    /// Revocation prevents the task from being executed if it hasn't started yet.
    /// If the task is already running and <see cref="RevokeOptions.Terminate"/> is true,
    /// the task's cancellation token will be triggered.
    /// </remarks>
    ValueTask RevokeAsync(
        string taskId,
        RevokeOptions? options = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Revokes multiple pending or running tasks.
    /// </summary>
    /// <param name="taskIds">The task IDs to revoke.</param>
    /// <param name="options">Optional revocation options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when all revocations are recorded.</returns>
    ValueTask RevokeAsync(
        IEnumerable<string> taskIds,
        RevokeOptions? options = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Checks if a task has been revoked.
    /// </summary>
    /// <param name="taskId">The task ID to check.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the task has been revoked; otherwise false.</returns>
    ValueTask<bool> IsRevokedAsync(string taskId, CancellationToken cancellationToken = default);

    // ========== Scheduled Task Management ==========

    /// <summary>
    /// Cancels a scheduled (delayed) task before it executes.
    /// </summary>
    /// <param name="taskId">The task ID to cancel.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the task was found and cancelled; false if not found or already executed.</returns>
    /// <remarks>
    /// This only cancels tasks that were scheduled with ETA or Countdown and haven't started yet.
    /// For tasks that are already running, use <see cref="RevokeAsync(string, RevokeOptions?, CancellationToken)"/> instead.
    /// </remarks>
    ValueTask<bool> CancelScheduledAsync(
        string taskId,
        CancellationToken cancellationToken = default
    );

    // ========== Task Sending ==========

    /// <summary>
    /// Sends a task for asynchronous execution.
    /// </summary>
    /// <typeparam name="TTask">Task type.</typeparam>
    /// <typeparam name="TInput">Input type.</typeparam>
    /// <typeparam name="TOutput">Output type.</typeparam>
    /// <param name="input">Task input.</param>
    /// <param name="options">Optional task options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Async result for tracking.</returns>
    ValueTask<AsyncResult<TOutput>> SendAsync<TTask, TInput, TOutput>(
        TInput input,
        SendOptions? options = null,
        CancellationToken cancellationToken = default
    )
        where TTask : ITask<TInput, TOutput>
        where TInput : class
        where TOutput : class;

    /// <summary>
    /// Sends a task with no return value.
    /// </summary>
    /// <typeparam name="TTask">Task type.</typeparam>
    /// <typeparam name="TInput">Input type.</typeparam>
    /// <param name="input">Task input.</param>
    /// <param name="options">Optional task options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Async result for tracking.</returns>
    ValueTask<AsyncResult> SendAsync<TTask, TInput>(
        TInput input,
        SendOptions? options = null,
        CancellationToken cancellationToken = default
    )
        where TTask : ITask<TInput>
        where TInput : class;

    /// <summary>
    /// Gets the result of a previously sent task.
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The task result, or null if not found.</returns>
    ValueTask<TaskResult?> GetResultAsync(
        string taskId,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Waits for a task to complete.
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
}
