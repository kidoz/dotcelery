using System.Runtime.CompilerServices;
using System.Text.Json;
using DotCelery.Core.Dashboard;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage;
using DotCelery.Core.Storage.Stores;
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
/// and are indexed by state (and sorted by the time they were last updated), so listing by
/// state and counting states do not scan. Listing by task name scans the retained records,
/// bounded by <see cref="NameScanLimit"/>.
/// </para>
/// <para>
/// Without a registered storage provider the store is unavailable and every read returns empty,
/// so a dashboard can run without one.
/// </para>
/// </remarks>
public sealed class TaskRecordStore
{
    private const int NameScanLimit = 1000;

    private readonly IDocumentStore? _documents;
    private readonly DashboardOptions _options;
    private readonly string _collection;

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
    public async ValueTask RecordAsync(
        TaskSummary summary,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(summary);

        if (_documents is null)
        {
            return;
        }

        var updatedAt =
            summary.CompletedAt ?? summary.StartedAt ?? summary.SentAt ?? DateTimeOffset.UtcNow;

        await _documents
            .UpsertAsync(
                _collection,
                summary.TaskId,
                JsonSerializer.SerializeToUtf8Bytes(
                    summary,
                    DotCeleryJsonContext.Default.TaskSummary
                ),
                new DocumentWriteOptions
                {
                    TimeToLive = _options.TaskRecordRetention,
                    IndexKey = StateIndexKey(summary.State),
                    SortKey = updatedAt,
                },
                cancellationToken
            )
            .ConfigureAwait(false);
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
    /// Queries records by task name, scanning the retained records, most recently updated first.
    /// </summary>
    /// <param name="taskName">The task name to match.</param>
    /// <param name="limit">Maximum number of records to return.</param>
    /// <param name="offset">Number of matching records to skip.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The matching records.</returns>
    public async IAsyncEnumerable<TaskSummary> QueryByNameAsync(
        string taskName,
        int limit,
        int offset,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(taskName);

        var skipped = 0;
        var returned = 0;

        await foreach (
            var summary in QueryAsync(
                    DocumentFilter.All,
                    new DocumentPage { Descending = true, Limit = NameScanLimit },
                    cancellationToken
                )
                .ConfigureAwait(false)
        )
        {
            if (!string.Equals(summary.TaskName, taskName, StringComparison.Ordinal))
            {
                continue;
            }

            if (skipped < offset)
            {
                skipped++;
                continue;
            }

            if (returned >= limit)
            {
                yield break;
            }

            returned++;
            yield return summary;
        }
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
    /// <returns>The distinct task names.</returns>
    public async ValueTask<IReadOnlyList<string>> GetTaskNamesAsync(
        CancellationToken cancellationToken = default
    )
    {
        if (_documents is null)
        {
            return [];
        }

        var names = new HashSet<string>(StringComparer.Ordinal);

        await foreach (
            var summary in QueryAsync(
                    DocumentFilter.All,
                    new DocumentPage { Descending = true, Limit = NameScanLimit },
                    cancellationToken
                )
                .ConfigureAwait(false)
        )
        {
            names.Add(summary.TaskName);
        }

        return [.. names];
    }

    private async IAsyncEnumerable<TaskSummary> QueryAsync(
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
                .QueryAsync(_collection, filter, page, cancellationToken)
                .ConfigureAwait(false)
        )
        {
            yield return Deserialize(document.Value.Span);
        }
    }

    private static string StateIndexKey(string state) => $"state:{state}";

    private static TaskSummary Deserialize(ReadOnlySpan<byte> value) =>
        JsonSerializer.Deserialize(value, DotCeleryJsonContext.Default.TaskSummary)!;
}
