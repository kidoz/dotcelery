using System.Runtime.CompilerServices;
using System.Text.Json;
using DotCelery.Core.Dashboard;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage;
using DotCelery.Core.Storage.Stores;
using DotCelery.Dashboard.Models;
using Microsoft.Extensions.Options;

namespace DotCelery.Dashboard.Services;

/// <summary>
/// Stores a record of each task the dashboard's hosts run, so the dashboard can list and count
/// tasks: results themselves are keyed by ID and cannot be queried.
/// </summary>
/// <remarks>
/// <para>
/// Records live on the storage primitives' documents, so they work on every provider the
/// application already uses. They expire after <see cref="DashboardOptions.TaskRecordRetention"/>
/// and are indexed by state and by task name (each sorted by the time it was last updated), so
/// listing by state, listing by name, and counting states do not scan: each write stores the
/// record and its name index entry, and both expire with the retention.
/// </para>
/// <para>
/// The list of task names the dashboard offers is kept in a marker per name, written with the
/// record and expiring with it, so a name is offered while any of its records is retained.
/// </para>
/// <para>
/// Without a registered storage provider the store is unavailable and every read returns empty,
/// so a dashboard can run without one.
/// </para>
/// </remarks>
public sealed class TaskRecordStore
{
    private readonly IDocumentStore? _documents;
    private readonly DashboardOptions _options;
    private readonly string _collection;
    private readonly string _names;
    private readonly string _nameMarkers;

    /// <summary>
    /// Initializes a new instance of the <see cref="TaskRecordStore"/> class.
    /// </summary>
    /// <param name="options">The dashboard options.</param>
    /// <param name="storage">The storage provider, when one is registered.</param>
    /// <param name="storeOptions">The store options, for the collection prefix.</param>
    public TaskRecordStore(
        IOptions<DashboardOptions> options,
        IStorageProvider? storage = null,
        IOptions<StorageStoreOptions>? storeOptions = null
    )
    {
        _options = options.Value;
        _documents = storage?.Documents;

        var prefix = (storeOptions?.Value ?? new StorageStoreOptions()).Prefix;
        _collection = $"{prefix}.dashboard.tasks";
        _names = $"{prefix}.dashboard.task-names";
        _nameMarkers = $"{prefix}.dashboard.task-name-markers";
    }

    /// <summary>
    /// Gets whether task records can be stored, which a storage provider is registered for.
    /// </summary>
    public bool IsAvailable => _documents is not null;

    /// <summary>
    /// Writes a task record, replacing the previous one for the same task.
    /// </summary>
    /// <param name="summary">The record to store.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public ValueTask RecordAsync(TaskSummary summary, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(summary);
        return UpdateAsync(summary.TaskId, _ => summary, cancellationToken);
    }

    // Lifecycle signals can arrive concurrently, even from different hosts. Re-evaluate each
    // merge against the current document after a version conflict rather than losing fields or
    // replacing a terminal outcome with a late publish notification.
    internal async ValueTask UpdateAsync(
        string taskId,
        Func<TaskSummary?, TaskSummary> update,
        CancellationToken cancellationToken
    )
    {
        if (_documents is null)
            return;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var existing = await _documents
                .GetAsync(_collection, taskId, cancellationToken)
                .ConfigureAwait(false);
            var summary = update(existing is null ? null : Deserialize(existing.Value.Span));
            var updatedAt =
                summary.CompletedAt ?? summary.StartedAt ?? summary.SentAt ?? DateTimeOffset.UtcNow;
            var value = JsonSerializer.SerializeToUtf8Bytes(
                summary,
                DotCeleryJsonContext.Default.TaskSummary
            );
            var options = new DocumentWriteOptions
            {
                TimeToLive = _options.TaskRecordRetention,
                IndexKey = StateIndexKey(summary.State),
                SortKey = updatedAt,
            };
            var version = existing is null
                ? await _documents
                    .TryInsertAsync(_collection, taskId, value, options, cancellationToken)
                    .ConfigureAwait(false)
                : await _documents
                    .TryReplaceAsync(
                        _collection,
                        taskId,
                        value,
                        existing.Version,
                        options,
                        cancellationToken
                    )
                    .ConfigureAwait(false);
            if (version is null)
                continue;

            await WriteIndexAsync(
                    _names,
                    taskId,
                    value,
                    options with
                    {
                        IndexKey = NameIndexKey(summary.TaskName),
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
            await WriteIndexAsync(
                    _nameMarkers,
                    summary.TaskName,
                    Array.Empty<byte>(),
                    options with
                    {
                        IndexKey = null,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
            return;
        }
    }

    // A slower writer must not move an index back to an older update time.
    private async ValueTask WriteIndexAsync(
        string collection,
        string id,
        ReadOnlyMemory<byte> value,
        DocumentWriteOptions options,
        CancellationToken cancellationToken
    )
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var existing = await _documents!
                .GetAsync(collection, id, cancellationToken)
                .ConfigureAwait(false);
            if (existing?.SortKey > options.SortKey)
                return;
            var version = existing is null
                ? await _documents
                    .TryInsertAsync(collection, id, value, options, cancellationToken)
                    .ConfigureAwait(false)
                : await _documents
                    .TryReplaceAsync(
                        collection,
                        id,
                        value,
                        existing.Version,
                        options,
                        cancellationToken
                    )
                    .ConfigureAwait(false);
            if (version is not null)
                return;
        }
    }

    /// <summary>
    /// Reads a task record.
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The record, or <c>null</c> if none is stored.</returns>
    public async ValueTask<TaskSummary?> GetAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        if (_documents is null)
        {
            return null;
        }

        var document = await _documents
            .GetAsync(_collection, taskId, cancellationToken)
            .ConfigureAwait(false);

        return document is null ? null : Deserialize(document.Value.Span);
    }

    /// <summary>
    /// Queries the most recently updated records in a state.
    /// </summary>
    /// <param name="state">The state to match.</param>
    /// <param name="limit">Maximum number of records to return.</param>
    /// <param name="offset">Number of records to skip.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The matching records, most recently updated first.</returns>
    public IAsyncEnumerable<TaskSummary> QueryByStateAsync(
        TaskState state,
        int limit,
        int offset,
        CancellationToken cancellationToken = default
    ) =>
        QueryAsync(
            new DocumentFilter { IndexKey = StateIndexKey(state.ToString()) },
            new DocumentPage
            {
                Descending = true,
                Limit = limit,
                Offset = offset,
            },
            cancellationToken
        );

    /// <summary>
    /// Queries records by task name, most recently updated first.
    /// </summary>
    /// <param name="taskName">The task name to match.</param>
    /// <param name="limit">Maximum number of records to return.</param>
    /// <param name="offset">Number of matching records to skip.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The matching records.</returns>
    public IAsyncEnumerable<TaskSummary> QueryByNameAsync(
        string taskName,
        int limit,
        int offset,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(taskName);

        return QueryAsync(
            _names,
            new DocumentFilter { IndexKey = NameIndexKey(taskName) },
            new DocumentPage
            {
                Descending = true,
                Limit = limit,
                Offset = offset,
            },
            cancellationToken
        );
    }

    /// <summary>
    /// Queries the records updated since a time, most recently updated first.
    /// </summary>
    /// <param name="from">The earliest update time to match, inclusive.</param>
    /// <param name="limit">Maximum number of records to return.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The matching records.</returns>
    public IAsyncEnumerable<TaskSummary> QueryRecentAsync(
        DateTimeOffset from,
        int limit,
        CancellationToken cancellationToken = default
    ) =>
        QueryAsync(
            new DocumentFilter { SortKeyFrom = from },
            new DocumentPage { Descending = true, Limit = limit },
            cancellationToken
        );

    /// <summary>
    /// Counts the retained records of every state.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The count per state.</returns>
    public async ValueTask<IReadOnlyDictionary<TaskState, long>> GetStateCountsAsync(
        CancellationToken cancellationToken = default
    )
    {
        var counts = new Dictionary<TaskState, long>();

        if (_documents is null)
        {
            return counts;
        }

        foreach (var state in Enum.GetValues<TaskState>())
        {
            counts[state] = await _documents
                .CountAsync(
                    _collection,
                    new DocumentFilter { IndexKey = StateIndexKey(state.ToString()) },
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        return counts;
    }

    /// <summary>
    /// Gets the task names among the retained records.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The task names, sorted.</returns>
    public async ValueTask<IReadOnlyList<string>> GetTaskNamesAsync(
        CancellationToken cancellationToken = default
    )
    {
        if (_documents is null)
        {
            return [];
        }

        var names = new List<string>();

        await foreach (
            var marker in _documents
                .QueryAsync(_nameMarkers, DocumentFilter.All, cancellationToken: cancellationToken)
                .ConfigureAwait(false)
        )
        {
            names.Add(marker.Id);
        }

        names.Sort(StringComparer.Ordinal);

        return names;
    }

    // Apply secondary criteria before paging. The state/name and time predicates are pushed
    // into the document index; remaining predicates stream records without buffering history.
    internal async ValueTask<IReadOnlyList<TaskSummary>> SearchAsync(
        TaskSearchRequest query,
        int limit,
        int offset,
        CancellationToken cancellationToken
    )
    {
        var byName = !string.IsNullOrEmpty(query.TaskName);
        var filter = new DocumentFilter
        {
            IndexKey =
                byName ? NameIndexKey(query.TaskName!)
                : query.State.HasValue ? StateIndexKey(query.State.Value.ToString())
                : null,
            SortKeyFrom = query.From,
            SortKeyBefore = query.Until,
        };
        var tasks = new List<TaskSummary>();
        var skipped = 0;
        await foreach (
            var task in QueryAsync(
                    byName ? _names : _collection,
                    filter,
                    new DocumentPage { Descending = !query.OldestFirst },
                    cancellationToken
                )
                .ConfigureAwait(false)
        )
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!query.Matches(task))
                continue;
            if (skipped < offset)
            {
                skipped++;
                continue;
            }
            tasks.Add(task);
            if (tasks.Count >= limit)
                break;
        }
        return tasks;
    }

    private IAsyncEnumerable<TaskSummary> QueryAsync(
        DocumentFilter filter,
        DocumentPage? page,
        CancellationToken cancellationToken
    ) => QueryAsync(_collection, filter, page, cancellationToken);

    private async IAsyncEnumerable<TaskSummary> QueryAsync(
        string collection,
        DocumentFilter filter,
        DocumentPage? page,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        if (_documents is null)
        {
            yield break;
        }

        await foreach (
            var document in _documents
                .QueryAsync(collection, filter, page, cancellationToken)
                .ConfigureAwait(false)
        )
        {
            // The name index may be maintained by a different concurrent writer. Always
            // return the primary record so its state and metadata agree with detail queries.
            var summary =
                collection == _names
                    ? await GetAsync(document.Id, cancellationToken).ConfigureAwait(false)
                    : Deserialize(document.Value.Span);
            if (summary is not null)
                yield return summary;
        }
    }

    private static string StateIndexKey(string state) => $"state:{state}";

    private static string NameIndexKey(string taskName) => $"name:{taskName}";

    private static TaskSummary Deserialize(ReadOnlySpan<byte> value) =>
        JsonSerializer.Deserialize(value, DotCeleryJsonContext.Default.TaskSummary)!;
}
