using DotCelery.Core.Dashboard;
using DotCelery.Core.Models;

namespace DotCelery.Dashboard.Services;

/// <summary>
/// Provides task summaries to the dashboard from a task index, such as
/// <see cref="TaskRecordStore"/>. Unlike the optional <c>ITaskQueryProvider</c> on a result
/// backend, a summary provider also knows about tasks that have not produced a result yet.
/// </summary>
public interface ITaskSummaryProvider
{
    /// <summary>
    /// Gets tasks in a state.
    /// </summary>
    /// <param name="state">The state to filter by.</param>
    /// <param name="limit">Maximum number of tasks to return.</param>
    /// <param name="offset">Number of tasks to skip.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Task summaries.</returns>
    ValueTask<IReadOnlyList<TaskSummary>> GetTasksByStateAsync(
        TaskState state,
        int limit,
        int offset,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets tasks with a name.
    /// </summary>
    /// <param name="taskName">The task name to filter by.</param>
    /// <param name="limit">Maximum number of tasks to return.</param>
    /// <param name="offset">Number of tasks to skip.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Task summaries.</returns>
    ValueTask<IReadOnlyList<TaskSummary>> GetTasksByNameAsync(
        string taskName,
        int limit,
        int offset,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets a task by ID.
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The task summary, or <c>null</c> if it is not known.</returns>
    ValueTask<TaskSummary?> GetTaskAsync(
        string taskId,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets the number of tasks in each state.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The count per state.</returns>
    ValueTask<IReadOnlyDictionary<TaskState, long>> GetStateCountsAsync(
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets the names of the tasks that are known.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The task names.</returns>
    ValueTask<IReadOnlyList<string>> GetTaskNamesAsync(
        CancellationToken cancellationToken = default
    );
}
