using System.Diagnostics.Metrics;

namespace DotCelery.Core.Instrumentation;

/// <summary>
/// Metrics recorded by the client and the worker: tasks sent, received, completed, retried,
/// their duration and queue time, and how many are in progress.
/// </summary>
/// <remarks>
/// The instruments live here rather than in <c>DotCelery.Telemetry</c> so the components that
/// record them do not depend on the telemetry package. Register the meter with OpenTelemetry
/// through <c>AddDotCeleryInstrumentation()</c>, which adds the <c>DotCelery</c> meter.
/// </remarks>
public static class DotCeleryMetrics
{
    /// <summary>
    /// Gets the meter for DotCelery metrics.
    /// </summary>
    public static Meter Meter { get; } =
        new(DotCeleryDiagnostics.SourceName, DotCeleryDiagnostics.SourceVersion);

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
    /// Records a task completion, whether it succeeded or failed.
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
}
