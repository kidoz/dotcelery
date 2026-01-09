using DotCelery.Core.Abstractions;
using DotCelery.Core.Exceptions;
using DotCelery.Core.Models;

namespace DotCelery.Client;

/// <summary>
/// Represents a pending task result.
/// </summary>
public class AsyncResult
{
    private readonly ICeleryClient _client;

    /// <summary>
    /// Initializes a new instance of the <see cref="AsyncResult"/> class.
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <param name="client">The celery client.</param>
    /// <exception cref="ArgumentNullException">Thrown if taskId or client is null.</exception>
    public AsyncResult(string taskId, ICeleryClient client)
    {
        ArgumentNullException.ThrowIfNull(taskId);
        ArgumentNullException.ThrowIfNull(client);
        TaskId = taskId;
        _client = client;
    }

    /// <summary>
    /// Gets the task invocation ID.
    /// </summary>
    public string TaskId { get; }

    /// <summary>
    /// Gets the current task state.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The task state, or null if not found.</returns>
    public async ValueTask<TaskState?> GetStateAsync(CancellationToken cancellationToken = default)
    {
        var result = await _client.GetResultAsync(TaskId, cancellationToken).ConfigureAwait(false);
        return result?.State;
    }

    /// <summary>
    /// Checks if task is complete (success, failure, or revoked).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the task is complete.</returns>
    public async ValueTask<bool> IsCompleteAsync(CancellationToken cancellationToken = default)
    {
        var state = await GetStateAsync(cancellationToken).ConfigureAwait(false);
        return state
            is TaskState.Success
                or TaskState.Failure
                or TaskState.Revoked
                or TaskState.Rejected;
    }

    /// <summary>
    /// Waits for task completion.
    /// </summary>
    /// <param name="timeout">Optional timeout.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The task result.</returns>
    public Task<TaskResult> WaitAsync(
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default
    )
    {
        return _client.WaitForResultAsync(TaskId, timeout, cancellationToken);
    }
}

/// <summary>
/// Represents a pending task result with typed output.
/// </summary>
/// <typeparam name="TOutput">The output type.</typeparam>
public sealed class AsyncResult<TOutput> : AsyncResult
    where TOutput : class
{
    private readonly IMessageSerializer _serializer;

    /// <summary>
    /// Initializes a new instance of the <see cref="AsyncResult{TOutput}"/> class.
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <param name="client">The celery client.</param>
    /// <param name="serializer">The message serializer.</param>
    /// <exception cref="ArgumentNullException">Thrown if serializer is null.</exception>
    public AsyncResult(string taskId, ICeleryClient client, IMessageSerializer serializer)
        : base(taskId, client)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        _serializer = serializer;
    }

    /// <summary>
    /// Waits for task and returns typed result.
    /// </summary>
    /// <param name="timeout">Optional timeout.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The typed result.</returns>
    public async Task<TOutput> GetAsync(
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default
    )
    {
        var result = await WaitAsync(timeout, cancellationToken).ConfigureAwait(false);

        if (result.State != TaskState.Success)
        {
            throw new TaskExecutionException(result);
        }

        if (result.Result is null)
        {
            throw new InvalidOperationException("Task succeeded but result is null");
        }

        return _serializer.Deserialize<TOutput>(result.Result);
    }
}
