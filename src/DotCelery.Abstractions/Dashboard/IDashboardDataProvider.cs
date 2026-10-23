using DotCelery.Core.Models;

namespace DotCelery.Core.Dashboard;

/// <summary>
/// Provides data for the dashboard UI.
/// </summary>
public interface IDashboardDataProvider
{
    /// <summary>
    /// Gets statistics for all queues.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Queue statistics.</returns>
    ValueTask<IReadOnlyList<QueueStats>> GetQueueStatsAsync(
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets tasks filtered by state.
    /// </summary>
    /// <param name="state">The task state to filter by.</param>
    /// <param name="limit">Maximum number of tasks to return.</param>
    /// <param name="offset">Number of tasks to skip.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Task summaries.</returns>
    ValueTask<IReadOnlyList<TaskSummary>> GetTasksByStateAsync(
        TaskState state,
        int limit = 50,
        int offset = 0,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets tasks filtered by task name.
    /// </summary>
    /// <param name="taskName">The task name to filter by.</param>
    /// <param name="limit">Maximum number of tasks to return.</param>
    /// <param name="offset">Number of tasks to skip.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Task summaries.</returns>
    ValueTask<IReadOnlyList<TaskSummary>> GetTasksByNameAsync(
        string taskName,
        int limit = 50,
        int offset = 0,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets a specific task by ID.
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The task summary, or null if not found.</returns>
    ValueTask<TaskSummary?> GetTaskAsync(
        string taskId,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets information about all active workers.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Worker information.</returns>
    ValueTask<IReadOnlyList<WorkerInfo>> GetWorkersAsync(
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets dashboard metrics for a time window.
    /// </summary>
    /// <param name="window">The time window.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Dashboard metrics.</returns>
    ValueTask<DashboardMetrics> GetMetricsAsync(
        TimeSpan window,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets counts for each task state.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Dictionary of state to count.</returns>
    ValueTask<IReadOnlyDictionary<TaskState, long>> GetStateCountsAsync(
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets the list of registered task names.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Task names.</returns>
    ValueTask<IReadOnlyList<string>> GetTaskNamesAsync(
        CancellationToken cancellationToken = default
    );
}
