using System.Diagnostics;
using System.Diagnostics.Metrics;
using DotCelery.Core.Instrumentation;

namespace DotCelery.Telemetry;

/// <summary>
/// OpenTelemetry instrumentation for DotCelery.
/// </summary>
public static class DotCeleryInstrumentation
{
    /// <summary>
    /// The name of the instrumentation library.
    /// </summary>
    public const string InstrumentationName = DotCeleryDiagnostics.SourceName;

    /// <summary>
    /// The version of the instrumentation library.
    /// </summary>
    public const string InstrumentationVersion = DotCeleryDiagnostics.SourceVersion;

    /// <summary>
    /// Gets the ActivitySource for DotCelery tracing. Emits the producer/consumer spans
    /// created by <c>DotCelery.Core</c> as well as any spans started through this class.
    /// </summary>
    public static ActivitySource ActivitySource => DotCeleryDiagnostics.ActivitySource;

    /// <summary>
    /// Gets the Meter for DotCelery metrics. The client and the worker record through
    /// <see cref="DotCeleryMetrics"/>, which owns the meter, so registering it here is enough.
    /// </summary>
    public static Meter Meter => DotCeleryMetrics.Meter;

    /// <summary>
    /// Records a task being sent.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <param name="queue">The target queue.</param>
    public static void RecordTaskSent(string taskName, string queue) =>
        DotCeleryMetrics.RecordTaskSent(taskName, queue);

    /// <summary>
    /// Records a task being received by a worker.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <param name="queue">The source queue.</param>
    public static void RecordTaskReceived(string taskName, string queue) =>
        DotCeleryMetrics.RecordTaskReceived(taskName, queue);

    /// <summary>
    /// Records a task completion.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <param name="success">Whether the task succeeded.</param>
    /// <param name="duration">The execution duration.</param>
    public static void RecordTaskCompleted(string taskName, bool success, TimeSpan duration) =>
        DotCeleryMetrics.RecordTaskCompleted(taskName, success, duration);

    /// <summary>
    /// Records a task retry.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <param name="retryCount">The current retry count.</param>
    public static void RecordTaskRetry(string taskName, int retryCount) =>
        DotCeleryMetrics.RecordTaskRetry(taskName, retryCount);

    /// <summary>
    /// Records queue time for a task.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <param name="queueTime">Time spent in queue.</param>
    public static void RecordQueueTime(string taskName, TimeSpan queueTime) =>
        DotCeleryMetrics.RecordQueueTime(taskName, queueTime);

    /// <summary>
    /// Increments the in-progress task counter.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    public static void IncrementTasksInProgress(string taskName) =>
        DotCeleryMetrics.IncrementTasksInProgress(taskName);

    /// <summary>
    /// Decrements the in-progress task counter.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    public static void DecrementTasksInProgress(string taskName) =>
        DotCeleryMetrics.DecrementTasksInProgress(taskName);

    /// <summary>
    /// Starts an activity for sending a task.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <param name="taskId">The task ID.</param>
    /// <returns>The started activity, or null if not sampled.</returns>
    public static Activity? StartSendActivity(string taskName, string taskId)
    {
        var activity = ActivitySource.StartActivity($"send {taskName}", ActivityKind.Producer);

        if (activity is not null)
        {
            activity.SetTag("messaging.system", "celery");
            activity.SetTag("messaging.operation", "send");
            activity.SetTag("messaging.destination.name", taskName);
            activity.SetTag("celery.task.name", taskName);
            activity.SetTag("celery.task.id", taskId);
        }

        return activity;
    }

    /// <summary>
    /// Starts an activity for processing a task.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <param name="taskId">The task ID.</param>
    /// <param name="parentContext">Optional parent context for distributed tracing.</param>
    /// <returns>The started activity, or null if not sampled.</returns>
    public static Activity? StartProcessActivity(
        string taskName,
        string taskId,
        ActivityContext? parentContext = null
    )
    {
        var activity = parentContext.HasValue
            ? ActivitySource.StartActivity(
                $"process {taskName}",
                ActivityKind.Consumer,
                parentContext.Value
            )
            : ActivitySource.StartActivity($"process {taskName}", ActivityKind.Consumer);

        if (activity is not null)
        {
            activity.SetTag("messaging.system", "celery");
            activity.SetTag("messaging.operation", "process");
            activity.SetTag("celery.task.name", taskName);
            activity.SetTag("celery.task.id", taskId);
        }

        return activity;
    }

    /// <summary>
    /// Records an exception on an activity.
    /// </summary>
    /// <param name="activity">The activity.</param>
    /// <param name="exception">The exception.</param>
    public static void RecordException(Activity? activity, Exception exception)
    {
        if (activity is null)
        {
            return;
        }

        activity.SetStatus(ActivityStatusCode.Error, exception.Message);
        activity.AddException(exception);
    }
}
