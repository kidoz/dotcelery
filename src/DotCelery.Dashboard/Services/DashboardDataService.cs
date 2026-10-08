using DotCelery.Core.Abstractions;
using DotCelery.Core.Dashboard;
using DotCelery.Core.Models;
using Microsoft.Extensions.Options;

namespace DotCelery.Dashboard.Services;

/// <summary>
/// Default implementation of <see cref="IDashboardDataProvider"/>.
/// Aggregates data from the message broker, result backend, and worker registry.
/// </summary>
public sealed class DashboardDataService : IDashboardDataProvider
{
    private readonly IResultBackend _resultBackend;
    private readonly IMessageBroker _broker;
    private readonly IWorkerRegistry? _workerRegistry;
    private readonly ITaskSummaryProvider? _taskSummaries;
    private readonly IQueueStatsProvider? _queueStats;
    private readonly IMetricsProvider? _metrics;
    private readonly DashboardOptions _options;
    private readonly ILogger<DashboardDataService> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="DashboardDataService"/> class.
    /// </summary>
    public DashboardDataService(
        IResultBackend resultBackend,
        IMessageBroker broker,
        IOptions<DashboardOptions> options,
        ILogger<DashboardDataService> logger,
        IWorkerRegistry? workerRegistry = null,
        ITaskSummaryProvider? taskSummaries = null,
        IQueueStatsProvider? queueStats = null,
        IMetricsProvider? metrics = null
    )
    {
        _resultBackend = resultBackend;
        _broker = broker;
        _workerRegistry = workerRegistry;
        _taskSummaries = taskSummaries;
        _queueStats = queueStats;
        _metrics = metrics;
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<QueueStats>> GetQueueStatsAsync(
        CancellationToken cancellationToken = default
    )
    {
        var stats = new List<QueueStats>();

        // Prefer the registered queue stats provider (the queue metrics store), then a broker
        // that can report its own queues
        if (_queueStats is not null)
        {
            await foreach (
                var queueStat in _queueStats
                    .GetAllQueueStatsAsync(cancellationToken)
                    .ConfigureAwait(false)
            )
            {
                stats.Add(queueStat);
            }

            return stats;
        }

        // Get queue stats from broker if supported
        if (_broker is IQueueStatsProvider queueStatsProvider)
        {
            await foreach (
                var queueStat in queueStatsProvider
                    .GetAllQueueStatsAsync(cancellationToken)
                    .ConfigureAwait(false)
            )
            {
                stats.Add(queueStat);
            }
        }

        return stats;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<TaskSummary>> GetTasksByStateAsync(
        TaskState state,
        int limit = 50,
        int offset = 0,
        CancellationToken cancellationToken = default
    )
    {
        if (_taskSummaries is not null)
        {
            return await _taskSummaries
                .GetTasksByStateAsync(state, limit, offset, cancellationToken)
                .ConfigureAwait(false);
        }

        var tasks = new List<TaskSummary>();

        if (_resultBackend is ITaskQueryProvider queryProvider)
        {
            await foreach (
                var result in queryProvider
                    .GetTasksByStateAsync(state, limit, offset, cancellationToken)
                    .ConfigureAwait(false)
            )
            {
                tasks.Add(MapToSummary(result));
            }
        }

        return tasks;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<TaskSummary>> GetTasksByNameAsync(
        string taskName,
        int limit = 50,
        int offset = 0,
        CancellationToken cancellationToken = default
    )
    {
        if (_taskSummaries is not null)
        {
            return await _taskSummaries
                .GetTasksByNameAsync(taskName, limit, offset, cancellationToken)
                .ConfigureAwait(false);
        }

        var tasks = new List<TaskSummary>();

        if (_resultBackend is ITaskQueryProvider queryProvider)
        {
            await foreach (
                var result in queryProvider
                    .GetTasksByNameAsync(taskName, limit, offset, cancellationToken)
                    .ConfigureAwait(false)
            )
            {
                tasks.Add(MapToSummary(result));
            }
        }

        return tasks;
    }

    /// <inheritdoc />
    public async ValueTask<TaskSummary?> GetTaskAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        // A task record also covers tasks that have not produced a result yet, such as a task
        // that is still running
        if (
            _taskSummaries is not null
            && await _taskSummaries.GetTaskAsync(taskId, cancellationToken).ConfigureAwait(false)
                is { } summary
        )
        {
            return summary;
        }

        var result = await _resultBackend
            .GetResultAsync(taskId, cancellationToken)
            .ConfigureAwait(false);
        return result is not null ? MapToSummary(result) : null;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<WorkerInfo>> GetWorkersAsync(
        CancellationToken cancellationToken = default
    )
    {
        if (_workerRegistry is null)
        {
            return [];
        }

        var workers = new List<WorkerInfo>();

        await foreach (
            var worker in _workerRegistry
                .GetActiveWorkersAsync(cancellationToken)
                .ConfigureAwait(false)
        )
        {
            workers.Add(worker);
        }

        return workers;
    }

    /// <inheritdoc />
    public async ValueTask<DashboardMetrics> GetMetricsAsync(
        TimeSpan window,
        CancellationToken cancellationToken = default
    )
    {
        if (_metrics is not null)
        {
            return await _metrics.GetMetricsAsync(window, cancellationToken).ConfigureAwait(false);
        }

        if (_resultBackend is IMetricsProvider metricsProvider)
        {
            return await metricsProvider
                .GetMetricsAsync(window, cancellationToken)
                .ConfigureAwait(false);
        }

        // Return empty metrics if nothing can provide them
        return new DashboardMetrics { Window = window };
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<TaskState, long>> GetStateCountsAsync(
        CancellationToken cancellationToken = default
    )
    {
        if (_taskSummaries is not null)
        {
            return await _taskSummaries
                .GetStateCountsAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        if (_resultBackend is ITaskQueryProvider queryProvider)
        {
            return await queryProvider.GetStateCountsAsync(cancellationToken).ConfigureAwait(false);
        }

        return new Dictionary<TaskState, long>();
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<string>> GetTaskNamesAsync(
        CancellationToken cancellationToken = default
    )
    {
        if (_taskSummaries is not null)
        {
            return await _taskSummaries.GetTaskNamesAsync(cancellationToken).ConfigureAwait(false);
        }

        if (_resultBackend is ITaskQueryProvider queryProvider)
        {
            return await queryProvider.GetTaskNamesAsync(cancellationToken).ConfigureAwait(false);
        }

        return [];
    }

    private TaskSummary MapToSummary(TaskResult result)
    {
        return new TaskSummary
        {
            TaskId = result.TaskId,
            TaskName = result.Metadata?.GetValueOrDefault("task") as string ?? "unknown",
            State = result.State.ToString(),
            Worker = result.Worker,
            CompletedAt = result.CompletedAt,
            Duration = result.Duration,
            Retries = result.Retries,
            ExceptionMessage = TaskExceptionSanitizer.Sanitize(result.Exception, _options),
        };
    }
}

/// <summary>
/// Optional interface for message brokers that can provide queue statistics.
/// </summary>
public interface IQueueStatsProvider
{
    /// <summary>
    /// Gets statistics for all queues.
    /// </summary>
    IAsyncEnumerable<QueueStats> GetAllQueueStatsAsync(
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets statistics for a specific queue.
    /// </summary>
    ValueTask<QueueStats?> GetQueueStatsAsync(
        string queueName,
        CancellationToken cancellationToken = default
    );
}

/// <summary>
/// Optional interface for result backends that support task queries.
/// </summary>
public interface ITaskQueryProvider
{
    /// <summary>
    /// Gets tasks by state.
    /// </summary>
    IAsyncEnumerable<TaskResult> GetTasksByStateAsync(
        TaskState state,
        int limit,
        int offset,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets tasks by name.
    /// </summary>
    IAsyncEnumerable<TaskResult> GetTasksByNameAsync(
        string taskName,
        int limit,
        int offset,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets counts for each state.
    /// </summary>
    ValueTask<IReadOnlyDictionary<TaskState, long>> GetStateCountsAsync(
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets all known task names.
    /// </summary>
    ValueTask<IReadOnlyList<string>> GetTaskNamesAsync(
        CancellationToken cancellationToken = default
    );
}

/// <summary>
/// Optional interface for result backends that support metrics.
/// </summary>
public interface IMetricsProvider
{
    /// <summary>
    /// Gets metrics for a time window.
    /// </summary>
    ValueTask<DashboardMetrics> GetMetricsAsync(
        TimeSpan window,
        CancellationToken cancellationToken = default
    );
}
