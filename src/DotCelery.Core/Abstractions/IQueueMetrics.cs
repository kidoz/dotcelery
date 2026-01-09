namespace DotCelery.Core.Abstractions;

/// <summary>
/// Provides queue-level metrics for monitoring.
/// </summary>
public interface IQueueMetrics
{
    /// <summary>
    /// Gets the number of messages waiting in a queue.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of waiting messages.</returns>
    ValueTask<long> GetWaitingCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets the number of messages currently being processed from a queue.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of in-progress messages.</returns>
    ValueTask<long> GetRunningCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets the total number of messages processed from a queue.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The total processed count.</returns>
    ValueTask<long> GetProcessedCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets the number of consumers for a queue.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The consumer count.</returns>
    ValueTask<int> GetConsumerCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets all known queue names.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of queue names.</returns>
    ValueTask<IReadOnlyList<string>> GetQueuesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets aggregated metrics for a queue.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The queue metrics.</returns>
    ValueTask<QueueMetricsData> GetMetricsAsync(
        string queue,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets aggregated metrics for all queues.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Dictionary of queue name to metrics.</returns>
    ValueTask<IReadOnlyDictionary<string, QueueMetricsData>> GetAllMetricsAsync(
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Records a message being processed (in-flight).
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="taskId">The task ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask RecordStartedAsync(
        string queue,
        string taskId,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Records a message processing completed.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="taskId">The task ID.</param>
    /// <param name="success">Whether the processing was successful.</param>
    /// <param name="duration">The processing duration.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask RecordCompletedAsync(
        string queue,
        string taskId,
        bool success,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Records a message being enqueued.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask RecordEnqueuedAsync(string queue, CancellationToken cancellationToken = default);
}

/// <summary>
/// Aggregated metrics data for a queue.
/// </summary>
public sealed record QueueMetricsData
{
    /// <summary>
    /// Gets the queue name.
    /// </summary>
    public required string Queue { get; init; }

    /// <summary>
    /// Gets the number of messages waiting in the queue.
    /// </summary>
    public long WaitingCount { get; init; }

    /// <summary>
    /// Gets the number of messages currently being processed.
    /// </summary>
    public long RunningCount { get; init; }

    /// <summary>
    /// Gets the total number of messages processed.
    /// </summary>
    public long ProcessedCount { get; init; }

    /// <summary>
    /// Gets the number of successful completions.
    /// </summary>
    public long SuccessCount { get; init; }

    /// <summary>
    /// Gets the number of failed completions.
    /// </summary>
    public long FailureCount { get; init; }

    /// <summary>
    /// Gets the number of consumers.
    /// </summary>
    public int ConsumerCount { get; init; }

    /// <summary>
    /// Gets the average processing duration.
    /// </summary>
    public TimeSpan? AverageDuration { get; init; }

    /// <summary>
    /// Gets the timestamp of the last enqueued message.
    /// </summary>
    public DateTimeOffset? LastEnqueuedAt { get; init; }

    /// <summary>
    /// Gets the timestamp of the last completed message.
    /// </summary>
    public DateTimeOffset? LastCompletedAt { get; init; }

    /// <summary>
    /// Gets the success rate (0-1).
    /// </summary>
    public double SuccessRate => ProcessedCount > 0 ? (double)SuccessCount / ProcessedCount : 0;
}
