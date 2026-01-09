using System.Collections.Concurrent;
using DotCelery.Core.Abstractions;

namespace DotCelery.Backend.InMemory.Metrics;

/// <summary>
/// In-memory implementation of <see cref="IQueueMetrics"/>.
/// </summary>
public sealed class InMemoryQueueMetrics : IQueueMetrics
{
    private readonly ConcurrentDictionary<string, QueueMetricsState> _queues = new();
    private readonly ConcurrentDictionary<string, string> _runningTasks = new(); // taskId -> queue

    /// <inheritdoc />
    public ValueTask<long> GetWaitingCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        var state = GetOrCreateState(queue);
        return ValueTask.FromResult(state.WaitingCount);
    }

    /// <inheritdoc />
    public ValueTask<long> GetRunningCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        var state = GetOrCreateState(queue);
        return ValueTask.FromResult(state.RunningCount);
    }

    /// <inheritdoc />
    public ValueTask<long> GetProcessedCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        var state = GetOrCreateState(queue);
        return ValueTask.FromResult(state.ProcessedCount);
    }

    /// <inheritdoc />
    public ValueTask<int> GetConsumerCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        var state = GetOrCreateState(queue);
        return ValueTask.FromResult(state.ConsumerCount);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetQueuesAsync(
        CancellationToken cancellationToken = default
    )
    {
        return ValueTask.FromResult<IReadOnlyList<string>>(_queues.Keys.ToList());
    }

    /// <inheritdoc />
    public ValueTask<QueueMetricsData> GetMetricsAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        var state = GetOrCreateState(queue);
        return ValueTask.FromResult(state.ToData(queue));
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyDictionary<string, QueueMetricsData>> GetAllMetricsAsync(
        CancellationToken cancellationToken = default
    )
    {
        var result = _queues.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.ToData(kvp.Key));

        return ValueTask.FromResult<IReadOnlyDictionary<string, QueueMetricsData>>(result);
    }

    /// <inheritdoc />
    public ValueTask RecordStartedAsync(
        string queue,
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        var state = GetOrCreateState(queue);
        Interlocked.Increment(ref state._runningCount);
        Interlocked.Decrement(ref state._waitingCount);
        _runningTasks[taskId] = queue;
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask RecordCompletedAsync(
        string queue,
        string taskId,
        bool success,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    )
    {
        var state = GetOrCreateState(queue);
        Interlocked.Decrement(ref state._runningCount);
        Interlocked.Increment(ref state._processedCount);

        if (success)
        {
            Interlocked.Increment(ref state._successCount);
        }
        else
        {
            Interlocked.Increment(ref state._failureCount);
        }

        // Update average duration (simple moving average)
        lock (state)
        {
            state._totalDuration += duration;
            state._completedWithDuration++;
            state.LastCompletedAt = DateTimeOffset.UtcNow;
        }

        _runningTasks.TryRemove(taskId, out _);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask RecordEnqueuedAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        var state = GetOrCreateState(queue);
        Interlocked.Increment(ref state._waitingCount);
        state.LastEnqueuedAt = DateTimeOffset.UtcNow;
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Registers a consumer for a queue.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    public void RegisterConsumer(string queue)
    {
        var state = GetOrCreateState(queue);
        Interlocked.Increment(ref state._consumerCount);
    }

    /// <summary>
    /// Unregisters a consumer from a queue.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    public void UnregisterConsumer(string queue)
    {
        if (_queues.TryGetValue(queue, out var state))
        {
            Interlocked.Decrement(ref state._consumerCount);
        }
    }

    /// <summary>
    /// Clears all metrics data.
    /// </summary>
    public void Clear()
    {
        _queues.Clear();
        _runningTasks.Clear();
    }

    private QueueMetricsState GetOrCreateState(string queue)
    {
        return _queues.GetOrAdd(queue, _ => new QueueMetricsState());
    }

    private sealed class QueueMetricsState
    {
        internal long _waitingCount;
        internal long _runningCount;
        internal long _processedCount;
        internal long _successCount;
        internal long _failureCount;
        internal int _consumerCount;
        internal TimeSpan _totalDuration;
        internal long _completedWithDuration;

        public DateTimeOffset? LastEnqueuedAt { get; set; }
        public DateTimeOffset? LastCompletedAt { get; set; }

        public long WaitingCount => Interlocked.Read(ref _waitingCount);
        public long RunningCount => Interlocked.Read(ref _runningCount);
        public long ProcessedCount => Interlocked.Read(ref _processedCount);
        public long SuccessCount => Interlocked.Read(ref _successCount);
        public long FailureCount => Interlocked.Read(ref _failureCount);
        public int ConsumerCount => Interlocked.CompareExchange(ref _consumerCount, 0, 0);

        public QueueMetricsData ToData(string queue)
        {
            TimeSpan? avgDuration = null;
            lock (this)
            {
                if (_completedWithDuration > 0)
                {
                    avgDuration = TimeSpan.FromTicks(_totalDuration.Ticks / _completedWithDuration);
                }
            }

            return new QueueMetricsData
            {
                Queue = queue,
                WaitingCount = WaitingCount,
                RunningCount = RunningCount,
                ProcessedCount = ProcessedCount,
                SuccessCount = SuccessCount,
                FailureCount = FailureCount,
                ConsumerCount = ConsumerCount,
                AverageDuration = avgDuration,
                LastEnqueuedAt = LastEnqueuedAt,
                LastCompletedAt = LastCompletedAt,
            };
        }
    }
}
