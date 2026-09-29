using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// <see cref="IQueueMetrics"/> on the storage primitives, shared by every process that uses the
/// same storage.
/// </summary>
/// <remarks>
/// <para>
/// A running task holds a lease for <see cref="StorageStoreOptions.ExecutionTimeout"/>, so a task
/// whose worker stops without recording its completion stops counting as running.
/// </para>
/// <para>
/// Consumers are not tracked; <see cref="GetConsumerCountAsync"/> returns 0.
/// </para>
/// </remarks>
public sealed class QueueMetrics : IQueueMetrics
{
    private const string Waiting = "waiting";
    private const string Processed = "processed";
    private const string Succeeded = "succeeded";
    private const string Failed = "failed";
    private const string DurationTicks = "duration-ticks";

    private readonly ICounterStore _counters;
    private readonly ILeaseStore _leases;
    private readonly IDocumentStore _documents;
    private readonly StorageStoreOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly string _counterPrefix;
    private readonly string _runningPrefix;
    private readonly string _lastEnqueued;
    private readonly string _lastCompleted;

    /// <summary>
    /// Initializes a new instance of the <see cref="QueueMetrics"/> class.
    /// </summary>
    /// <param name="storage">The storage provider.</param>
    /// <param name="options">The store options.</param>
    /// <param name="timeProvider">The clock for activity times.</param>
    public QueueMetrics(
        IStorageProvider storage,
        IOptions<StorageStoreOptions>? options = null,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(storage);
        _counters = storage.Counters;
        _leases = storage.Leases;
        _documents = storage.Documents;
        _options = options?.Value ?? new StorageStoreOptions();
        _timeProvider = timeProvider ?? TimeProvider.System;
        _counterPrefix = _options.Name("queue-metrics/");
        _runningPrefix = _options.Name("queue-running/");
        _lastEnqueued = _options.Name("queue-last-enqueued");
        _lastCompleted = _options.Name("queue-last-completed");
    }

    /// <inheritdoc />
    public ValueTask<long> GetWaitingCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    ) => _counters.GetAsync(CounterKey(queue, Waiting), cancellationToken);

    /// <inheritdoc />
    public async ValueTask<long> GetRunningCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        long count = 0;
        await foreach (
            var _ in _leases
                .ListAsync(RunningPrefix(queue), cancellationToken)
                .ConfigureAwait(false)
        )
        {
            count++;
        }

        return count;
    }

    /// <inheritdoc />
    public ValueTask<long> GetProcessedCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    ) => _counters.GetAsync(CounterKey(queue, Processed), cancellationToken);

    /// <inheritdoc />
    public ValueTask<int> GetConsumerCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    ) => ValueTask.FromResult(0);

    /// <inheritdoc />
    /// <remarks>Queues with recorded enqueues, completions, or running tasks are listed.</remarks>
    public async ValueTask<IReadOnlyList<string>> GetQueuesAsync(
        CancellationToken cancellationToken = default
    )
    {
        var queues = new SortedSet<string>(StringComparer.Ordinal);

        foreach (var collection in new[] { _lastEnqueued, _lastCompleted })
        {
            await foreach (
                var document in _documents
                    .QueryAsync(
                        collection,
                        DocumentFilter.All,
                        cancellationToken: cancellationToken
                    )
                    .ConfigureAwait(false)
            )
            {
                queues.Add(document.Id);
            }
        }

        await foreach (
            var lease in _leases.ListAsync(_runningPrefix, cancellationToken).ConfigureAwait(false)
        )
        {
            var escapedQueue = lease.Key[_runningPrefix.Length..].Split('/')[0];
            queues.Add(Uri.UnescapeDataString(escapedQueue));
        }

        return [.. queues];
    }

    /// <inheritdoc />
    public async ValueTask<QueueMetricsData> GetMetricsAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        var processed = await GetProcessedCountAsync(queue, cancellationToken)
            .ConfigureAwait(false);
        var durationTicks = await _counters
            .GetAsync(CounterKey(queue, DurationTicks), cancellationToken)
            .ConfigureAwait(false);

        return new QueueMetricsData
        {
            Queue = queue,
            WaitingCount = await GetWaitingCountAsync(queue, cancellationToken)
                .ConfigureAwait(false),
            RunningCount = await GetRunningCountAsync(queue, cancellationToken)
                .ConfigureAwait(false),
            ProcessedCount = processed,
            SuccessCount = await _counters
                .GetAsync(CounterKey(queue, Succeeded), cancellationToken)
                .ConfigureAwait(false),
            FailureCount = await _counters
                .GetAsync(CounterKey(queue, Failed), cancellationToken)
                .ConfigureAwait(false),
            AverageDuration = processed > 0 ? TimeSpan.FromTicks(durationTicks / processed) : null,
            LastEnqueuedAt = await GetTimeAsync(_lastEnqueued, queue, cancellationToken)
                .ConfigureAwait(false),
            LastCompletedAt = await GetTimeAsync(_lastCompleted, queue, cancellationToken)
                .ConfigureAwait(false),
        };
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, QueueMetricsData>> GetAllMetricsAsync(
        CancellationToken cancellationToken = default
    )
    {
        var metrics = new Dictionary<string, QueueMetricsData>(StringComparer.Ordinal);
        foreach (var queue in await GetQueuesAsync(cancellationToken).ConfigureAwait(false))
        {
            metrics[queue] = await GetMetricsAsync(queue, cancellationToken).ConfigureAwait(false);
        }

        return metrics;
    }

    /// <inheritdoc />
    public async ValueTask RecordStartedAsync(
        string queue,
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        await _counters
            .IncrementAsync(CounterKey(queue, Waiting), -1, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        await _leases
            .TryAcquireAsync(
                RunningKey(queue, taskId),
                taskId,
                _options.ExecutionTimeout,
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask RecordCompletedAsync(
        string queue,
        string taskId,
        bool success,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    )
    {
        var running = await _leases
            .GetAsync(RunningKey(queue, taskId), cancellationToken)
            .ConfigureAwait(false);
        if (running is not null)
        {
            await _leases.ReleaseAsync(running, cancellationToken).ConfigureAwait(false);
        }

        await _counters
            .IncrementAsync(CounterKey(queue, Processed), cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        await _counters
            .IncrementAsync(
                CounterKey(queue, success ? Succeeded : Failed),
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);
        await _counters
            .IncrementAsync(
                CounterKey(queue, DurationTicks),
                duration.Ticks,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);
        await SetTimeAsync(_lastCompleted, queue, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask RecordEnqueuedAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        await _counters
            .IncrementAsync(CounterKey(queue, Waiting), cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        await SetTimeAsync(_lastEnqueued, queue, cancellationToken).ConfigureAwait(false);
    }

    // Escaping keeps queue names and task IDs that contain '/' unambiguous
    private string CounterKey(string queue, string counter) =>
        $"{_counterPrefix}{Uri.EscapeDataString(queue)}/{counter}";

    private string RunningPrefix(string queue) => $"{_runningPrefix}{Uri.EscapeDataString(queue)}/";

    private string RunningKey(string queue, string taskId) =>
        RunningPrefix(queue) + Uri.EscapeDataString(taskId);

    private async ValueTask<DateTimeOffset?> GetTimeAsync(
        string collection,
        string queue,
        CancellationToken cancellationToken
    ) =>
        (
            await _documents.GetAsync(collection, queue, cancellationToken).ConfigureAwait(false)
        )?.SortKey;

    private async ValueTask SetTimeAsync(
        string collection,
        string queue,
        CancellationToken cancellationToken
    ) =>
        await _documents
            .UpsertAsync(
                collection,
                queue,
                ReadOnlyMemory<byte>.Empty,
                new DocumentWriteOptions { SortKey = _timeProvider.GetUtcNow() },
                cancellationToken
            )
            .ConfigureAwait(false);
}
