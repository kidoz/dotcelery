using DotCelery.Core.Abstractions;

namespace DotCelery.Core.Routing;

/// <summary>
/// Routes tasks to appropriate queues based on task name patterns.
/// </summary>
public interface ITaskRouter
{
    /// <summary>
    /// Gets the queue for a task based on configured routes.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <param name="defaultQueue">The default queue if no route matches.</param>
    /// <returns>The queue name.</returns>
    string GetQueue(string taskName, string defaultQueue = "celery");

    /// <summary>
    /// Adds a route for a specific task type.
    /// </summary>
    /// <typeparam name="TTask">The task type.</typeparam>
    /// <param name="queue">The target queue.</param>
    void AddRoute<TTask>(string queue)
        where TTask : ITask;

    /// <summary>
    /// Adds a route for tasks matching a pattern.
    /// </summary>
    /// <param name="pattern">The pattern to match. Use * for single segment, ** for multiple segments.</param>
    /// <param name="queue">The target queue.</param>
    /// <remarks>
    /// Pattern examples:
    /// - "reports.*" matches "reports.daily", "reports.weekly" (single segment)
    /// - "reports.**" matches "reports.email.send", "reports.pdf.generate" (multiple segments)
    /// - "email.send" matches exactly "email.send"
    /// </remarks>
    void AddRoute(string pattern, string queue);

    /// <summary>
    /// Adds a route for a specific task name.
    /// </summary>
    /// <param name="taskName">The exact task name.</param>
    /// <param name="queue">The target queue.</param>
    void AddExactRoute(string taskName, string queue);
}
