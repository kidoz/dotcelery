using System.Text.Json.Serialization.Metadata;
using DotCelery.Core.Batches;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// <see cref="IBatchStore"/> on the storage primitives.
/// </summary>
/// <remarks>
/// Batch updates are atomic, so tasks that finish at the same time are all counted. A batch is
/// finished when every task has completed or failed: it is completed when none failed, failed
/// when all failed, and partially completed otherwise. A cancelled batch stays cancelled.
/// </remarks>
public sealed class BatchStore : IBatchStore
{
    private static readonly JsonTypeInfo<Batch> BatchTypeInfo = StoreJson.TypeInfo<Batch>();

    private readonly IDocumentStore _documents;
    private readonly TimeProvider _timeProvider;
    private readonly string _batches;
    private readonly string _tasks;

    /// <summary>
    /// Initializes a new instance of the <see cref="BatchStore"/> class.
    /// </summary>
    /// <param name="storage">The storage provider.</param>
    /// <param name="options">The store options.</param>
    /// <param name="timeProvider">The clock for completion times.</param>
    public BatchStore(
        IStorageProvider storage,
        IOptions<StorageStoreOptions>? options = null,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(storage);
        _documents = storage.Documents;
        _timeProvider = timeProvider ?? TimeProvider.System;
        var storeOptions = options?.Value ?? new StorageStoreOptions();
        _batches = storeOptions.Name("batches");
        _tasks = storeOptions.Name("batch-tasks");
    }

    /// <inheritdoc />
    public async ValueTask CreateAsync(Batch batch, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batch);

        await _documents
            .UpsertAsync(
                _batches,
                batch.Id,
                batch,
                BatchTypeInfo,
                WriteOptions(batch),
                cancellationToken
            )
            .ConfigureAwait(false);

        foreach (var taskId in batch.TaskIds)
        {
            await _documents
                .UpsertAsync(
                    _tasks,
                    taskId,
                    ReadOnlyMemory<byte>.Empty,
                    new DocumentWriteOptions { IndexKey = batch.Id },
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async ValueTask<Batch?> GetAsync(
        string batchId,
        CancellationToken cancellationToken = default
    ) =>
        (
            await _documents
                .GetAsync(_batches, batchId, BatchTypeInfo, cancellationToken)
                .ConfigureAwait(false)
        )?.Value;

    /// <inheritdoc />
    public async ValueTask UpdateStateAsync(
        string batchId,
        BatchState state,
        CancellationToken cancellationToken = default
    ) =>
        await UpdateAsync(
                batchId,
                batch =>
                    batch with
                    {
                        State = state,
                        CompletedAt = IsFinished(state)
                            ? _timeProvider.GetUtcNow()
                            : batch.CompletedAt,
                    },
                cancellationToken
            )
            .ConfigureAwait(false);

    /// <inheritdoc />
    public ValueTask<Batch?> MarkTaskCompletedAsync(
        string batchId,
        string taskId,
        CancellationToken cancellationToken = default
    ) =>
        UpdateAsync(
            batchId,
            batch =>
                batch.CompletedTaskIds.Contains(taskId)
                    ? null
                    : Advance(
                        batch with
                        {
                            CompletedTaskIds = [.. batch.CompletedTaskIds, taskId],
                        }
                    ),
            cancellationToken
        );

    /// <inheritdoc />
    public ValueTask<Batch?> MarkTaskFailedAsync(
        string batchId,
        string taskId,
        CancellationToken cancellationToken = default
    ) =>
        UpdateAsync(
            batchId,
            batch =>
                batch.FailedTaskIds.Contains(taskId)
                    ? null
                    : Advance(batch with { FailedTaskIds = [.. batch.FailedTaskIds, taskId] }),
            cancellationToken
        );

    /// <inheritdoc />
    public async ValueTask<bool> DeleteAsync(
        string batchId,
        CancellationToken cancellationToken = default
    )
    {
        var deleted = await _documents
            .DeleteAsync(_batches, batchId, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        await _documents
            .DeleteManyAsync(_tasks, new DocumentFilter { IndexKey = batchId }, cancellationToken)
            .ConfigureAwait(false);

        return deleted;
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetBatchIdForTaskAsync(
        string taskId,
        CancellationToken cancellationToken = default
    ) =>
        (
            await _documents.GetAsync(_tasks, taskId, cancellationToken).ConfigureAwait(false)
        )?.IndexKey;

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static bool IsFinished(BatchState state) =>
        state
            is BatchState.Completed
                or BatchState.Failed
                or BatchState.PartiallyCompleted
                or BatchState.Cancelled;

    private static DocumentWriteOptions WriteOptions(Batch batch) =>
        new() { SortKey = batch.CreatedAt };

    private Batch Advance(Batch batch)
    {
        if (batch.State == BatchState.Cancelled)
        {
            return batch;
        }

        if (batch.IsFinished)
        {
            return batch with
            {
                State =
                    batch.FailedCount == 0 ? BatchState.Completed
                    : batch.CompletedCount == 0 ? BatchState.Failed
                    : BatchState.PartiallyCompleted,
                CompletedAt = _timeProvider.GetUtcNow(),
            };
        }

        return batch.State == BatchState.Pending
            ? batch with
            {
                State = BatchState.Processing,
            }
            : batch;
    }

    private ValueTask<Batch?> UpdateAsync(
        string batchId,
        Func<Batch, Batch?> update,
        CancellationToken cancellationToken
    ) =>
        _documents.UpdateAsync(
            _batches,
            batchId,
            BatchTypeInfo,
            current => update(current.Value),
            WriteOptions,
            cancellationToken
        );
}
