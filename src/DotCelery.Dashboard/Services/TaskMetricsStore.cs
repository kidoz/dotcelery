using DotCelery.Core.Abstractions;
using DotCelery.Core.Storage;
using DotCelery.Core.Storage.Stores;
using Microsoft.Extensions.Options;

namespace DotCelery.Dashboard.Services;

/// <summary>
/// Counts the tasks each name completed, so the dashboard's metrics collector records how many
/// of each task name settled in an interval in the history, which outlives the task records.
/// </summary>
/// <remarks>
/// The totals are cumulative counters on the storage primitives, and every task name gets a
/// marker document so the collector lists the names that have totals without scanning the
/// records. Without a storage provider the store is unavailable and reports nothing.
/// </remarks>
public sealed class TaskMetricsStore
{
    private const string Succeeded = "succeeded";
    private const string Failed = "failed";
    private const string DurationTicks = "duration-ticks";

    private readonly IDocumentStore? _documents;
    private readonly ICounterStore? _counters;
    private readonly string _counterPrefix;
    private readonly string _names;
    private readonly TimeProvider _timeProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="TaskMetricsStore"/> class.
    /// </summary>
    /// <param name="storage">The storage provider, when one is registered.</param>
    /// <param name="storeOptions">The store options, for the prefix.</param>
    /// <param name="timeProvider">The clock for the names' markers.</param>
    public TaskMetricsStore(
        IStorageProvider? storage = null,
        IOptions<StorageStoreOptions>? storeOptions = null,
        TimeProvider? timeProvider = null
    )
    {
        var prefix = (storeOptions?.Value ?? new StorageStoreOptions()).Prefix;
        _documents = storage?.Documents;
        _counters = storage?.Counters;
        _counterPrefix = $"{prefix}.dashboard.task-metrics/";
        _names = $"{prefix}.dashboard.task-metrics-names";
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>
    /// Gets whether the totals can be stored, which a storage provider is registered for.
    /// </summary>
    public bool IsAvailable => _documents is not null && _counters is not null;

    /// <summary>
    /// Records that a task of this name settled, with how long it took.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <param name="success">Whether the task succeeded.</param>
    /// <param name="duration">How long the task took.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async ValueTask RecordCompletedAsync(
        string taskName,
        bool success,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(taskName);

        if (_documents is null || _counters is null)
        {
            return;
        }

        await _counters
            .IncrementAsync(
                CounterKey(taskName, success ? Succeeded : Failed),
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);
        await _counters
            .IncrementAsync(
                CounterKey(taskName, DurationTicks),
                duration.Ticks,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);

        // The marker names the tasks that have totals, so reading them does not scan; its value
        // is unused
        await _documents
            .UpsertAsync(
                _names,
                taskName,
                Array.Empty<byte>(),
                new DocumentWriteOptions { SortKey = _timeProvider.GetUtcNow() },
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Gets the cumulative totals of each task name that has completed tasks.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The totals by task name.</returns>
    public async ValueTask<IReadOnlyDictionary<string, TaskTotals>> GetTotalsAsync(
        CancellationToken cancellationToken = default
    )
    {
        var totals = new Dictionary<string, TaskTotals>(StringComparer.Ordinal);

        if (_documents is null || _counters is null)
        {
            return totals;
        }

        await foreach (
            var marker in _documents
                .QueryAsync(_names, DocumentFilter.All, cancellationToken: cancellationToken)
                .ConfigureAwait(false)
        )
        {
            totals[marker.Id] = new TaskTotals(
                await _counters
                    .GetAsync(CounterKey(marker.Id, Succeeded), cancellationToken)
                    .ConfigureAwait(false),
                await _counters
                    .GetAsync(CounterKey(marker.Id, Failed), cancellationToken)
                    .ConfigureAwait(false),
                await _counters
                    .GetAsync(CounterKey(marker.Id, DurationTicks), cancellationToken)
                    .ConfigureAwait(false)
            );
        }

        return totals;
    }

    // Escaping keeps task names that contain '/' unambiguous
    private string CounterKey(string taskName, string counter) =>
        $"{_counterPrefix}{Uri.EscapeDataString(taskName)}/{counter}";
}

/// <summary>
/// The cumulative totals of a task name, as the dashboard's metrics collector reads them.
/// </summary>
/// <param name="Succeeded">The number of tasks that succeeded.</param>
/// <param name="Failed">The number of tasks that failed.</param>
/// <param name="DurationTicks">The total execution time of the settled tasks, in ticks.</param>
public sealed record TaskTotals(long Succeeded, long Failed, long DurationTicks)
{
    /// <summary>
    /// Gets the number of tasks that settled, however they settled.
    /// </summary>
    public long Total => Succeeded + Failed;
}
