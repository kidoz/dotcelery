using DotCelery.Core.Abstractions;
using DotCelery.Core.Dashboard;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;
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
        IWorkerRegistry? workerRegistry = null
    )
    {
        _resultBackend = resultBackend;
        _broker = broker;
        _workerRegistry = workerRegistry;
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<QueueStats>> GetQueueStatsAsync(
        CancellationToken cancellationToken = default
    )
    {
        var stats = new List<QueueStats>();

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
        if (_resultBackend is IMetricsProvider metricsProvider)
        {
            return await metricsProvider
                .GetMetricsAsync(window, cancellationToken)
                .ConfigureAwait(false);
        }

        // Return empty metrics if backend doesn't support metrics
        return new DashboardMetrics { Window = window };
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<TaskState, long>> GetStateCountsAsync(
        CancellationToken cancellationToken = default
    )
    {
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
            ExceptionMessage = SanitizeExceptionMessage(result.Exception),
        };
    }

    /// <summary>
    /// Sanitizes exception information to prevent information disclosure.
    /// </summary>
    private string? SanitizeExceptionMessage(TaskExceptionInfo? exception)
    {
        if (exception is null)
        {
            return null;
        }

        if (_options.ExposeExceptionDetails)
        {
            // Full details requested - truncate if too long
            var message = exception.Message;
            if (message?.Length > _options.MaxExceptionMessageLength)
            {
                return message[.._options.MaxExceptionMessageLength] + "...";
            }
            return message;
        }

        // Sanitized mode - only show exception type without sensitive details
        var exceptionType = exception.Type;
        if (string.IsNullOrEmpty(exceptionType))
        {
            return "An error occurred";
        }

        // Extract just the type name without namespace for privacy
        var typeName = exceptionType.Contains('.')
            ? exceptionType[(exceptionType.LastIndexOf('.') + 1)..]
            : exceptionType;

        return $"{typeName}: Task execution failed";
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
