using System.Collections.Concurrent;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;

namespace DotCelery.Backend.InMemory;

/// <summary>
/// In-memory result backend for testing and development.
/// </summary>
public sealed class InMemoryResultBackend : IResultBackend
{
    private readonly ConcurrentDictionary<string, TaskResult> _results = new();
    private readonly ConcurrentDictionary<string, TaskState> _states = new();
    private readonly ConcurrentDictionary<string, TaskCompletionSource<TaskResult>> _waiters =
        new();
    private bool _disposed;

    /// <inheritdoc />
    public ValueTask StoreResultAsync(
        TaskResult result,
        TimeSpan? expiry = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _results[result.TaskId] = result;
        _states[result.TaskId] = result.State;

        // Notify any waiters
        if (_waiters.TryRemove(result.TaskId, out var tcs))
        {
            tcs.TrySetResult(result);
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<TaskResult?> GetResultAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _results.TryGetValue(taskId, out var result);
        return ValueTask.FromResult(result);
    }

    /// <inheritdoc />
    public async Task<TaskResult> WaitForResultAsync(
        string taskId,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Check if result already exists
        if (_results.TryGetValue(taskId, out var existing))
        {
            return existing;
        }

        // Create or get existing waiter with RunContinuationsAsynchronously to prevent inline continuations
        var tcs = _waiters.GetOrAdd(
            taskId,
            _ => new TaskCompletionSource<TaskResult>(
                TaskCreationOptions.RunContinuationsAsynchronously
            )
        );

        // Check again after adding waiter (race condition)
        if (_results.TryGetValue(taskId, out existing))
        {
            _waiters.TryRemove(taskId, out _);
            return existing;
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        if (timeout.HasValue)
        {
            cts.CancelAfter(timeout.Value);
        }

        await using var registration = cts.Token.Register(() => tcs.TrySetCanceled(cts.Token));

        try
        {
            return await tcs.Task.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (timeout.HasValue && !cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Timeout waiting for task {taskId} result after {timeout.Value}"
            );
        }
        finally
        {
            // Always clean up waiter to prevent memory leaks
            _waiters.TryRemove(taskId, out _);
        }
    }

    /// <inheritdoc />
    public ValueTask UpdateStateAsync(
        string taskId,
        TaskState state,
        object? metadata = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _states[taskId] = state;
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<TaskState?> GetStateAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_states.TryGetValue(taskId, out var state))
        {
            return ValueTask.FromResult<TaskState?>(state);
        }

        return ValueTask.FromResult<TaskState?>(null);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return ValueTask.CompletedTask;
        }

        _disposed = true;

        // Cancel all waiters
        foreach (var tcs in _waiters.Values)
        {
            tcs.TrySetCanceled();
        }

        _waiters.Clear();
        _results.Clear();
        _states.Clear();

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Clears all stored results and states.
    /// </summary>
    public void Clear()
    {
        _results.Clear();
        _states.Clear();
    }

    /// <summary>
    /// Gets the count of stored results.
    /// </summary>
    public int Count => _results.Count;
}
