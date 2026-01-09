using System.Collections.Concurrent;
using DotCelery.Core.Batches;

namespace DotCelery.Backend.InMemory.Batches;

/// <summary>
/// In-memory implementation of <see cref="IBatchStore"/>.
/// </summary>
public sealed class InMemoryBatchStore : IBatchStore
{
    private readonly ConcurrentDictionary<string, Batch> _batches = new();
    private readonly ConcurrentDictionary<string, string> _taskToBatch = new();

    /// <inheritdoc />
    public ValueTask CreateAsync(Batch batch, CancellationToken cancellationToken = default)
    {
        _batches[batch.Id] = batch;

        // Index task IDs
        foreach (var taskId in batch.TaskIds)
        {
            _taskToBatch[taskId] = batch.Id;
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<Batch?> GetAsync(string batchId, CancellationToken cancellationToken = default)
    {
        _batches.TryGetValue(batchId, out var batch);
        return ValueTask.FromResult(batch);
    }

    /// <inheritdoc />
    public ValueTask UpdateStateAsync(
        string batchId,
        BatchState state,
        CancellationToken cancellationToken = default
    )
    {
        if (_batches.TryGetValue(batchId, out var batch))
        {
            var completedAt = state
                is BatchState.Completed
                    or BatchState.Failed
                    or BatchState.PartiallyCompleted
                    or BatchState.Cancelled
                ? DateTimeOffset.UtcNow
                : batch.CompletedAt;

            _batches[batchId] = batch with { State = state, CompletedAt = completedAt };
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<Batch?> MarkTaskCompletedAsync(
        string batchId,
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        if (!_batches.TryGetValue(batchId, out var batch))
        {
            return ValueTask.FromResult<Batch?>(null);
        }

        var completedIds = batch.CompletedTaskIds.ToList();
        if (!completedIds.Contains(taskId))
        {
            completedIds.Add(taskId);
        }

        var updatedBatch = batch with { CompletedTaskIds = completedIds };

        // Update state if all tasks completed
        if (updatedBatch.IsFinished)
        {
            var newState =
                updatedBatch.FailedCount > 0 ? BatchState.PartiallyCompleted : BatchState.Completed;

            updatedBatch = updatedBatch with
            {
                State = newState,
                CompletedAt = DateTimeOffset.UtcNow,
            };
        }
        else if (batch.State == BatchState.Pending)
        {
            updatedBatch = updatedBatch with { State = BatchState.Processing };
        }

        _batches[batchId] = updatedBatch;
        return ValueTask.FromResult<Batch?>(updatedBatch);
    }

    /// <inheritdoc />
    public ValueTask<Batch?> MarkTaskFailedAsync(
        string batchId,
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        if (!_batches.TryGetValue(batchId, out var batch))
        {
            return ValueTask.FromResult<Batch?>(null);
        }

        var failedIds = batch.FailedTaskIds.ToList();
        if (!failedIds.Contains(taskId))
        {
            failedIds.Add(taskId);
        }

        var updatedBatch = batch with { FailedTaskIds = failedIds };

        // Update state if all tasks completed
        if (updatedBatch.IsFinished)
        {
            var newState =
                updatedBatch.CompletedCount > 0 ? BatchState.PartiallyCompleted : BatchState.Failed;

            updatedBatch = updatedBatch with
            {
                State = newState,
                CompletedAt = DateTimeOffset.UtcNow,
            };
        }
        else if (batch.State == BatchState.Pending)
        {
            updatedBatch = updatedBatch with { State = BatchState.Processing };
        }

        _batches[batchId] = updatedBatch;
        return ValueTask.FromResult<Batch?>(updatedBatch);
    }

    /// <inheritdoc />
    public ValueTask<bool> DeleteAsync(
        string batchId,
        CancellationToken cancellationToken = default
    )
    {
        if (!_batches.TryRemove(batchId, out var batch))
        {
            return ValueTask.FromResult(false);
        }

        // Remove task mappings
        foreach (var taskId in batch.TaskIds)
        {
            _taskToBatch.TryRemove(taskId, out _);
        }

        return ValueTask.FromResult(true);
    }

    /// <inheritdoc />
    public ValueTask<string?> GetBatchIdForTaskAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        _taskToBatch.TryGetValue(taskId, out var batchId);
        return ValueTask.FromResult(batchId);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _batches.Clear();
        _taskToBatch.Clear();
        return ValueTask.CompletedTask;
    }
}
