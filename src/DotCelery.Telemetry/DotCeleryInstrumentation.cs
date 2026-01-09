using System.Diagnostics;
using System.Diagnostics.Metrics;
using OpenTelemetry.Trace;

namespace DotCelery.Telemetry;

/// <summary>
/// OpenTelemetry instrumentation for DotCelery.
/// </summary>
public static class DotCeleryInstrumentation
{
    /// <summary>
    /// The name of the instrumentation library.
    /// </summary>
    public const string InstrumentationName = "DotCelery";

    /// <summary>
    /// The version of the instrumentation library.
    /// </summary>
    public const string InstrumentationVersion = "1.0.0";

    /// <summary>
    /// Gets the ActivitySource for DotCelery tracing.
    /// </summary>
    public static ActivitySource ActivitySource { get; } =
        new(InstrumentationName, InstrumentationVersion);

    /// <summary>
    /// Gets the Meter for DotCelery metrics.
    /// </summary>
    public static Meter Meter { get; } = new(InstrumentationName, InstrumentationVersion);

    // Counters
    private static readonly Counter<long> TasksSent = Meter.CreateCounter<long>(
        "dotcelery.tasks.sent",
        description: "Number of tasks sent"
    );

    private static readonly Counter<long> TasksReceived = Meter.CreateCounter<long>(
        "dotcelery.tasks.received",
        description: "Number of tasks received by workers"
    );

    private static readonly Counter<long> TasksSucceeded = Meter.CreateCounter<long>(
        "dotcelery.tasks.succeeded",
        description: "Number of tasks completed successfully"
    );

    private static readonly Counter<long> TasksFailed = Meter.CreateCounter<long>(
        "dotcelery.tasks.failed",
        description: "Number of tasks that failed"
    );

    private static readonly Counter<long> TasksRetried = Meter.CreateCounter<long>(
        "dotcelery.tasks.retried",
        description: "Number of task retries"
    );

    // Histograms
    private static readonly Histogram<double> TaskDuration = Meter.CreateHistogram<double>(
        "dotcelery.tasks.duration",
        unit: "ms",
        description: "Duration of task execution in milliseconds"
    );

    private static readonly Histogram<double> TaskQueueTime = Meter.CreateHistogram<double>(
        "dotcelery.tasks.queue_time",
        unit: "ms",
        description: "Time tasks spend in queue before processing"
    );

    // Gauges (using UpDownCounters as proxies)
    private static readonly UpDownCounter<long> TasksInProgress = Meter.CreateUpDownCounter<long>(
        "dotcelery.tasks.in_progress",
        description: "Number of tasks currently being processed"
    );

    /// <summary>
    /// Records a task being sent.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <param name="queue">The target queue.</param>
    public static void RecordTaskSent(string taskName, string queue)
    {
        TasksSent.Add(
            1,
            new KeyValuePair<string, object?>("task.name", taskName),
            new KeyValuePair<string, object?>("queue", queue)
        );
    }

    /// <summary>
    /// Records a task being received by a worker.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <param name="queue">The source queue.</param>
    public static void RecordTaskReceived(string taskName, string queue)
    {
        TasksReceived.Add(
            1,
            new KeyValuePair<string, object?>("task.name", taskName),
            new KeyValuePair<string, object?>("queue", queue)
        );
    }

    /// <summary>
    /// Records a task completion.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <param name="success">Whether the task succeeded.</param>
    /// <param name="duration">The execution duration.</param>
    public static void RecordTaskCompleted(string taskName, bool success, TimeSpan duration)
    {
        var tags = new KeyValuePair<string, object?>[]
        {
            new("task.name", taskName),
            new("success", success),
        };

        if (success)
        {
            TasksSucceeded.Add(1, tags);
        }
        else
        {
            TasksFailed.Add(1, tags);
        }

        TaskDuration.Record(duration.TotalMilliseconds, tags);
    }

    /// <summary>
    /// Records a task retry.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <param name="retryCount">The current retry count.</param>
    public static void RecordTaskRetry(string taskName, int retryCount)
    {
        TasksRetried.Add(
            1,
            new KeyValuePair<string, object?>("task.name", taskName),
            new KeyValuePair<string, object?>("retry_count", retryCount)
        );
    }

    /// <summary>
    /// Records queue time for a task.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <param name="queueTime">Time spent in queue.</param>
    public static void RecordQueueTime(string taskName, TimeSpan queueTime)
    {
        TaskQueueTime.Record(
            queueTime.TotalMilliseconds,
            new KeyValuePair<string, object?>("task.name", taskName)
        );
    }

    /// <summary>
    /// Increments the in-progress task counter.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    public static void IncrementTasksInProgress(string taskName)
    {
        TasksInProgress.Add(1, new KeyValuePair<string, object?>("task.name", taskName));
    }

    /// <summary>
    /// Decrements the in-progress task counter.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    public static void DecrementTasksInProgress(string taskName)
    {
        TasksInProgress.Add(-1, new KeyValuePair<string, object?>("task.name", taskName));
    }

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
