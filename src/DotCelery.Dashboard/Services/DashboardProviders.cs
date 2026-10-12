using System.Runtime.CompilerServices;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Dashboard;
using DotCelery.Core.Models;

namespace DotCelery.Dashboard.Services;

/// <summary>
/// Serves the dashboard's task lists and state counts from the task records.
/// </summary>
public sealed class DashboardTaskSummaryProvider : ITaskSummaryProvider
{
    private readonly TaskRecordStore _records;

    /// <summary>
    /// Initializes a new instance of the <see cref="DashboardTaskSummaryProvider"/> class.
    /// </summary>
    /// <param name="records">The task records.</param>
    public DashboardTaskSummaryProvider(TaskRecordStore records)
    {
        _records = records;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<TaskSummary>> GetTasksByStateAsync(
        TaskState state,
        int limit,
        int offset,
        CancellationToken cancellationToken = default
    )
    {
        var tasks = new List<TaskSummary>();

        await foreach (
            var summary in _records
                .QueryByStateAsync(state, limit, offset, cancellationToken)
                .ConfigureAwait(false)
        )
        {
            tasks.Add(summary);
        }

        return tasks;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<TaskSummary>> GetTasksByNameAsync(
        string taskName,
        int limit,
        int offset,
        CancellationToken cancellationToken = default
    )
    {
        var tasks = new List<TaskSummary>();

        await foreach (
            var summary in _records
                .QueryByNameAsync(taskName, limit, offset, cancellationToken)
                .ConfigureAwait(false)
        )
        {
            tasks.Add(summary);
        }

        return tasks;
    }

    /// <inheritdoc />
    public ValueTask<TaskSummary?> GetTaskAsync(
        string taskId,
        CancellationToken cancellationToken = default
    ) => _records.GetAsync(taskId, cancellationToken);

    /// <inheritdoc />
    public ValueTask<IReadOnlyDictionary<TaskState, long>> GetStateCountsAsync(
        CancellationToken cancellationToken = default
    ) => _records.GetStateCountsAsync(cancellationToken);

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetTaskNamesAsync(
        CancellationToken cancellationToken = default
    ) => _records.GetTaskNamesAsync(cancellationToken);
}

/// <summary>
/// Serves the dashboard's queue statistics from the queue metrics store, and the consumers of
/// each queue from the worker registry.
/// </summary>
/// <remarks>
/// A queue's consumers are the online workers whose registered queues contain it; a queue no
/// online worker lists keeps the consumer count the queue metrics store reports. An in-process
/// registry sees only this process's workers, so a dashboard that monitors workers in other
/// processes registers a registry shared with them.
/// </remarks>
public sealed class DashboardQueueStatsProvider : IQueueStatsProvider
{
    private readonly IQueueMetrics? _metrics;
    private readonly IWorkerRegistry? _workers;

    /// <summary>
    /// Initializes a new instance of the <see cref="DashboardQueueStatsProvider"/> class.
    /// </summary>
    /// <param name="metrics">The queue metrics store, when one is registered.</param>
    /// <param name="workers">The worker registry, when one is registered.</param>
    public DashboardQueueStatsProvider(
        IQueueMetrics? metrics = null,
        IWorkerRegistry? workers = null
    )
    {
        _metrics = metrics;
        _workers = workers;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<QueueStats> GetAllQueueStatsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        if (_metrics is null)
        {
            yield break;
        }

        var metrics = await _metrics.GetAllMetricsAsync(cancellationToken).ConfigureAwait(false);
        var consumers = await CountConsumersAsync(cancellationToken).ConfigureAwait(false);

        foreach (var queue in metrics.Values)
        {
            yield return Map(queue, consumers);
        }
    }

    /// <inheritdoc />
    public async ValueTask<QueueStats?> GetQueueStatsAsync(
        string queueName,
        CancellationToken cancellationToken = default
    )
    {
        if (_metrics is null)
        {
            return null;
        }

        var metrics = await _metrics
            .GetMetricsAsync(queueName, cancellationToken)
            .ConfigureAwait(false);
        var consumers = await CountConsumersAsync(cancellationToken).ConfigureAwait(false);

        return Map(metrics, consumers);
    }

    private async ValueTask<Dictionary<string, int>> CountConsumersAsync(
        CancellationToken cancellationToken
    )
    {
        var consumers = new Dictionary<string, int>(StringComparer.Ordinal);

        if (_workers is null)
        {
            return consumers;
        }

        await foreach (
            var worker in _workers.GetActiveWorkersAsync(cancellationToken).ConfigureAwait(false)
        )
        {
            // A worker that missed its heartbeats or stopped is not consuming anything
            if (worker.Status != WorkerStatus.Online)
            {
                continue;
            }

            foreach (var queue in worker.Queues)
            {
                consumers[queue] = consumers.GetValueOrDefault(queue) + 1;
            }
        }

        return consumers;
    }

    private static QueueStats Map(QueueMetricsData metrics, Dictionary<string, int> consumers) =>
        new()
        {
            Name = metrics.Queue,
            MessageCount = metrics.WaitingCount,
            ConsumerCount = consumers.TryGetValue(metrics.Queue, out var count)
                ? count
                : metrics.ConsumerCount,
            InProgressCount = metrics.RunningCount,
            LastMessageAt = metrics.LastEnqueuedAt,
        };
}
