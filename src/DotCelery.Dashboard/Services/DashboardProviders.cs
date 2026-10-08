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
/// Serves the dashboard's queue statistics from the queue metrics store.
/// </summary>
public sealed class DashboardQueueStatsProvider : IQueueStatsProvider
{
    private readonly IQueueMetrics? _metrics;

    /// <summary>
    /// Initializes a new instance of the <see cref="DashboardQueueStatsProvider"/> class.
    /// </summary>
    /// <param name="metrics">The queue metrics store, when one is registered.</param>
    public DashboardQueueStatsProvider(IQueueMetrics? metrics = null)
    {
        _metrics = metrics;
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

        foreach (var queue in metrics.Values)
        {
            yield return Map(queue);
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

        return Map(
            await _metrics.GetMetricsAsync(queueName, cancellationToken).ConfigureAwait(false)
        );
    }

    private static QueueStats Map(QueueMetricsData metrics) =>
        new()
        {
            Name = metrics.Queue,
            MessageCount = metrics.WaitingCount,
            ConsumerCount = metrics.ConsumerCount,
            InProgressCount = metrics.RunningCount,
            LastMessageAt = metrics.LastEnqueuedAt,
        };
}
