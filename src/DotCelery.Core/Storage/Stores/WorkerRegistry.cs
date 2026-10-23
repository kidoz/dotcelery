using System.Runtime.CompilerServices;
using DotCelery.Core.Dashboard;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// <see cref="IWorkerRegistry"/> on the storage primitives, shared by every dashboard and worker
/// that uses the same storage.
/// </summary>
/// <remarks>
/// <para>
/// A worker's record is refreshed by every heartbeat and expires after
/// <see cref="StorageStoreOptions.WorkerRetention"/> without one, so a worker that stops without
/// unregistering stops being listed even when no dashboard cleans the registry up.
/// </para>
/// <para>
/// <see cref="CleanupStaleWorkersAsync"/> marks a worker that missed its heartbeat budget
/// unresponsive and removes it once it missed a second one. A heartbeat always wins over a
/// concurrent cleanup, so a live worker is not left marked unresponsive.
/// </para>
/// </remarks>
public sealed class WorkerRegistry : IWorkerRegistry
{
    private readonly IDocumentStore _documents;
    private readonly StorageStoreOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly string _collection;

    /// <summary>
    /// Initializes a new instance of the <see cref="WorkerRegistry"/> class.
    /// </summary>
    /// <param name="storage">The storage provider.</param>
    /// <param name="options">The store options.</param>
    /// <param name="timeProvider">The clock for heartbeats and cleanup.</param>
    public WorkerRegistry(
        IStorageProvider storage,
        IOptions<StorageStoreOptions>? options = null,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(storage);

        _documents = TenantStorageScope.For(storage, options).Documents;
        _options = options?.Value ?? new StorageStoreOptions();
        _timeProvider = timeProvider ?? TimeProvider.System;
        _collection = _options.Name("workers");
    }

    /// <inheritdoc />
    public async ValueTask RegisterWorkerAsync(
        WorkerInfo worker,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(worker);

        await _documents
            .UpsertAsync(
                _collection,
                worker.WorkerId,
                worker,
                DotCeleryJsonContext.Default.WorkerInfo,
                WriteOptions(worker.LastHeartbeat),
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    /// <remarks>
    /// A heartbeat from a worker with no record, such as one whose record expired after it
    /// stopped heartbeating for too long, changes nothing: the worker registers itself again when
    /// it restarts.
    /// </remarks>
    public async ValueTask HeartbeatAsync(
        string workerId,
        int activeTasks = 0,
        long processedCount = 0,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(workerId);

        var document = await _documents
            .GetAsync(
                _collection,
                workerId,
                DotCeleryJsonContext.Default.WorkerInfo,
                cancellationToken
            )
            .ConfigureAwait(false);

        if (document is null)
        {
            return;
        }

        var heartbeatAt = _timeProvider.GetUtcNow();

        await _documents
            .UpsertAsync(
                _collection,
                workerId,
                document.Value with
                {
                    LastHeartbeat = heartbeatAt,
                    ActiveTasks = activeTasks,
                    ProcessedCount = processedCount,
                    Status = WorkerStatus.Online,
                },
                DotCeleryJsonContext.Default.WorkerInfo,
                WriteOptions(heartbeatAt),
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask UnregisterWorkerAsync(
        string workerId,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(workerId);

        await _documents
            .DeleteAsync(_collection, workerId, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<WorkerInfo> GetActiveWorkersAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        await foreach (
            var document in _documents
                .QueryAsync(
                    _collection,
                    DocumentFilter.All,
                    DotCeleryJsonContext.Default.WorkerInfo,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false)
        )
        {
            yield return document.Value;
        }
    }

    /// <inheritdoc />
    public async ValueTask<WorkerInfo?> GetWorkerAsync(
        string workerId,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(workerId);

        var document = await _documents
            .GetAsync(
                _collection,
                workerId,
                DotCeleryJsonContext.Default.WorkerInfo,
                cancellationToken
            )
            .ConfigureAwait(false);

        return document?.Value;
    }

    /// <inheritdoc />
    public async ValueTask<int> CleanupStaleWorkersAsync(
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    )
    {
        var removed = 0;

        await foreach (
            var document in _documents
                .QueryAsync(
                    _collection,
                    new DocumentFilter { SortKeyBefore = _timeProvider.GetUtcNow() - timeout },
                    DotCeleryJsonContext.Default.WorkerInfo,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false)
        )
        {
            if (document.Value.Status == WorkerStatus.Unresponsive)
            {
                // Removing the record it was read with is what counts the worker as removed, so
                // two dashboards cleaning up at once remove it once
                if (
                    await _documents
                        .DeleteAsync(_collection, document.Id, document.Version, cancellationToken)
                        .ConfigureAwait(false)
                )
                {
                    removed++;
                }

                continue;
            }

            // The status is written only while the record is unchanged, so a heartbeat that
            // arrived after it was read is not overwritten
            await _documents
                .TryReplaceAsync(
                    _collection,
                    document.Id,
                    document.Value with
                    {
                        Status = WorkerStatus.Unresponsive,
                    },
                    document.Version,
                    DotCeleryJsonContext.Default.WorkerInfo,
                    WriteOptions(document.Value.LastHeartbeat),
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        return removed;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    // The last heartbeat is what queries sort and time out on, and the record expires a retention
    // after it was last written
    private DocumentWriteOptions WriteOptions(DateTimeOffset lastHeartbeat) =>
        new() { SortKey = lastHeartbeat, TimeToLive = _options.WorkerRetention };
}
