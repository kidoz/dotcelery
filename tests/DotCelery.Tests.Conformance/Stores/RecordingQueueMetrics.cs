using DotCelery.Core.Abstractions;

namespace DotCelery.Tests.Conformance.Stores;

/// <summary>
/// A queue metrics store that records what was enqueued, so what a broker records as it
/// publishes can be asserted.
/// </summary>
public sealed class RecordingQueueMetrics : IQueueMetrics
{
    private readonly List<string> _enqueued = [];

    /// <summary>
    /// Gets or sets whether recording fails, as a store that is down does.
    /// </summary>
    public bool Fail { get; set; }

    /// <summary>
    /// Gets the queues the recorded messages were published to, in order.
    /// </summary>
    public IReadOnlyList<string> Enqueued
    {
        get
        {
            lock (_enqueued)
            {
                return [.. _enqueued];
            }
        }
    }

    /// <summary>
    /// Gets how many messages were recorded as enqueued in a queue.
    /// </summary>
    public int CountOf(string queue) => Enqueued.Count(enqueued => enqueued == queue);

    /// <inheritdoc />
    public ValueTask RecordEnqueuedAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        if (Fail)
        {
            throw new InvalidOperationException("The queue metrics store is unavailable.");
        }

        lock (_enqueued)
        {
            _enqueued.Add(queue);
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<long> GetWaitingCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    ) => ValueTask.FromResult((long)CountOf(queue));

    /// <inheritdoc />
    public ValueTask<long> GetRunningCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    ) => ValueTask.FromResult(0L);

    /// <inheritdoc />
    public ValueTask<long> GetProcessedCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    ) => ValueTask.FromResult(0L);

    /// <inheritdoc />
    public ValueTask<int> GetConsumerCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    ) => ValueTask.FromResult(0);

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetQueuesAsync(
        CancellationToken cancellationToken = default
    ) => ValueTask.FromResult<IReadOnlyList<string>>([.. Enqueued.Distinct()]);

    /// <inheritdoc />
    public ValueTask<QueueMetricsData> GetMetricsAsync(
        string queue,
        CancellationToken cancellationToken = default
    ) =>
        ValueTask.FromResult(new QueueMetricsData { Queue = queue, WaitingCount = CountOf(queue) });

    /// <inheritdoc />
    public ValueTask<IReadOnlyDictionary<string, QueueMetricsData>> GetAllMetricsAsync(
        CancellationToken cancellationToken = default
    ) =>
        ValueTask.FromResult<IReadOnlyDictionary<string, QueueMetricsData>>(
            Enqueued
                .Distinct()
                .ToDictionary(queue => queue, queue => new QueueMetricsData { Queue = queue })
        );

    /// <inheritdoc />
    public ValueTask RecordStartedAsync(
        string queue,
        string taskId,
        CancellationToken cancellationToken = default
    ) => ValueTask.CompletedTask;

    /// <inheritdoc />
    public ValueTask RecordCompletedAsync(
        string queue,
        string taskId,
        bool success,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    ) => ValueTask.CompletedTask;
}
