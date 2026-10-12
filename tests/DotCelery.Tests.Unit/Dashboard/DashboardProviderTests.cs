using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Dashboard;
using DotCelery.Core.Models;
using DotCelery.Core.Storage.Stores;
using DotCelery.Dashboard;
using DotCelery.Dashboard.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;

namespace DotCelery.Tests.Unit.Dashboard;

/// <summary>
/// Exercises the dashboard's built-in providers: queue statistics from the queue metrics store,
/// metrics computed live from the task records, and metrics from the historical data store.
/// </summary>
public sealed class DashboardProviderTests
{
    private readonly FakeTimeProvider _time = new(
        new DateTimeOffset(2026, 10, 8, 12, 0, 0, TimeSpan.Zero)
    );
    private readonly InMemoryStorageProvider _storage = new();
    private readonly TaskRecordStore _records;

    public DashboardProviderTests()
    {
        _records = new TaskRecordStore(Options.Create(new DashboardOptions()), _storage);
    }

    [Fact]
    public async Task QueueStats_ComeFromTheQueueMetricsStore()
    {
        var metrics = new FakeQueueMetrics
        {
            Data =
            {
                ["celery"] = new QueueMetricsData
                {
                    Queue = "celery",
                    WaitingCount = 3,
                    RunningCount = 1,
                    ConsumerCount = 2,
                },
            },
        };
        var provider = new DashboardQueueStatsProvider(metrics);

        var all = new List<QueueStats>();
        await foreach (var stats in provider.GetAllQueueStatsAsync())
        {
            all.Add(stats);
        }

        var queue = Assert.Single(all);
        Assert.Equal("celery", queue.Name);
        Assert.Equal(3, queue.MessageCount);
        Assert.Equal(1, queue.InProgressCount);
        Assert.Equal(2, queue.ConsumerCount);
    }

    [Fact]
    public async Task QueueStats_CountTheOnlineWorkersThatListTheQueue()
    {
        var metrics = new FakeQueueMetrics
        {
            Data =
            {
                ["celery"] = new QueueMetricsData { Queue = "celery", WaitingCount = 3 },
                ["priority"] = new QueueMetricsData { Queue = "priority", ConsumerCount = 7 },
            },
        };
        var workers = new WorkerRegistry(_storage, timeProvider: _time);
        await workers.RegisterWorkerAsync(CreateWorker("worker-1", "celery", "priority"));
        await workers.RegisterWorkerAsync(CreateWorker("worker-2", "celery"));
        var provider = new DashboardQueueStatsProvider(metrics, workers);

        var all = new List<QueueStats>();
        await foreach (var stats in provider.GetAllQueueStatsAsync())
        {
            all.Add(stats);
        }

        // Two workers consume celery, one of them also consumes priority; a queue no online
        // worker lists keeps the count the queue metrics store reports
        Assert.Equal(2, all.Single(queue => queue.Name == "celery").ConsumerCount);
        Assert.Equal(1, all.Single(queue => queue.Name == "priority").ConsumerCount);
    }

    [Fact]
    public async Task QueueStats_DoNotCountWorkersThatMissedTheirHeartbeats()
    {
        var metrics = new FakeQueueMetrics
        {
            Data = { ["celery"] = new QueueMetricsData { Queue = "celery" } },
        };
        var workers = new WorkerRegistry(_storage, timeProvider: _time);
        await workers.RegisterWorkerAsync(CreateWorker("worker-1", "celery"));
        _time.Advance(TimeSpan.FromSeconds(1));
        await workers.CleanupStaleWorkersAsync(TimeSpan.Zero);
        var provider = new DashboardQueueStatsProvider(metrics, workers);

        var all = new List<QueueStats>();
        await foreach (var stats in provider.GetAllQueueStatsAsync())
        {
            all.Add(stats);
        }

        Assert.Equal(0, Assert.Single(all).ConsumerCount);
    }

    [Fact]
    public async Task Metrics_AreComputedFromTheTaskRecordsInTheWindow()
    {
        var now = _time.GetUtcNow();
        await _records.RecordAsync(
            CreateCompleted("t1", "tests.alpha", TaskState.Success, now.AddMinutes(-10), 100)
        );
        await _records.RecordAsync(
            CreateCompleted("t2", "tests.alpha", TaskState.Success, now.AddMinutes(-5), 300)
        );
        await _records.RecordAsync(
            CreateCompleted("t3", "tests.beta", TaskState.Failure, now.AddMinutes(-1), null)
        );

        // Outside the window
        await _records.RecordAsync(
            CreateCompleted("old", "tests.alpha", TaskState.Success, now.AddHours(-2), 500)
        );

        var provider = new DashboardMetricsProvider(_records, timeProvider: _time);
        var metrics = await provider.GetMetricsAsync(TimeSpan.FromHours(1));

        Assert.Equal(3, metrics.TotalProcessed);
        Assert.Equal(2, metrics.SuccessCount);
        Assert.Equal(1, metrics.FailureCount);
        Assert.Equal(TimeSpan.FromMilliseconds(200), metrics.AverageExecutionTime);
        Assert.True(metrics.TasksPerSecond > 0);

        Assert.NotNull(metrics.ByTaskName);
        var alpha = metrics.ByTaskName["tests.alpha"];
        Assert.Equal(2, alpha.TotalCount);
        Assert.Equal(2, alpha.SuccessCount);
        Assert.Equal(TimeSpan.FromMilliseconds(200), alpha.AverageExecutionTime);

        var beta = metrics.ByTaskName["tests.beta"];
        Assert.Equal(1, beta.TotalCount);
        Assert.Equal(1, beta.FailureCount);
    }

    [Fact]
    public async Task Metrics_ComeFromTheHistoricalStoreWhenOneIsRegistered()
    {
        var from = _time.GetUtcNow() - TimeSpan.FromHours(1);
        var history = new FakeHistoricalDataStore
        {
            Aggregated = new AggregatedMetrics
            {
                From = from,
                To = _time.GetUtcNow(),
                Granularity = MetricsGranularity.Minute,
                TotalProcessed = 10,
                SuccessCount = 9,
                FailureCount = 1,
            },
            ByTaskName =
            {
                ["tests.alpha"] = new TaskMetricsSummary
                {
                    TaskName = "tests.alpha",
                    TotalCount = 10,
                    SuccessCount = 9,
                    FailureCount = 1,
                },
            },
        };

        var provider = new DashboardMetricsProvider(_records, history, _time);
        var metrics = await provider.GetMetricsAsync(TimeSpan.FromHours(1));

        Assert.Equal(10, metrics.TotalProcessed);
        Assert.Equal(9, metrics.SuccessCount);
        Assert.Equal(1, metrics.FailureCount);
        Assert.Equal(9, metrics.ByTaskName!["tests.alpha"].SuccessCount);
    }

    [Fact]
    public async Task TheCollector_RecordsDeltasAsSnapshots()
    {
        var metrics = new FakeQueueMetrics
        {
            Data =
            {
                ["celery"] = new QueueMetricsData
                {
                    Queue = "celery",
                    SuccessCount = 5,
                    FailureCount = 1,
                },
            },
        };
        var history = new FakeHistoricalDataStore();

        var collector = new DashboardMetricsCollector(
            Options.Create(
                new DashboardOptions { MetricsSnapshotInterval = TimeSpan.FromMinutes(1) }
            ),
            NullLogger<DashboardMetricsCollector>.Instance,
            history,
            metrics,
            _time
        );

        await collector.StartAsync(CancellationToken.None);

        // The first poll only establishes the baseline
        await WaitForTickAsync(() => metrics.Reads > 0);
        Assert.Empty(history.Snapshots);

        metrics.Data["celery"] = metrics.Data["celery"] with { SuccessCount = 8, FailureCount = 2 };
        await WaitForTickAsync(() => history.Snapshots.Count == 1);

        var snapshot = Assert.Single(history.Snapshots);
        Assert.Equal("celery", snapshot.Queue);
        Assert.Equal(3, snapshot.SuccessCount);
        Assert.Equal(1, snapshot.FailureCount);

        // Nothing changed, so no further snapshot is recorded
        var reads = metrics.Reads;
        await WaitForTickAsync(() => metrics.Reads > reads);
        Assert.Single(history.Snapshots);

        // Retention runs less often than snapshots
        Assert.Equal(0, history.RetentionApplied);
        metrics.Data["celery"] = metrics.Data["celery"] with { SuccessCount = 9 };
        _time.Advance(TimeSpan.FromHours(1));
        await WaitForAsync(() => history.RetentionApplied >= 1);

        await collector.StopAsync(CancellationToken.None);
    }

    /// <summary>
    /// Waits for a condition while advancing the fake clock, so the collector's timer ticks
    /// without real waiting.
    /// </summary>
    private async Task WaitForTickAsync(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!condition())
        {
            _time.Advance(TimeSpan.FromSeconds(30));
            await Task.Delay(10, timeout.Token);
        }
    }

    private static TaskSummary CreateCompleted(
        string taskId,
        string taskName,
        TaskState state,
        DateTimeOffset completedAt,
        int? durationMilliseconds
    ) =>
        new()
        {
            TaskId = taskId,
            TaskName = taskName,
            State = state.ToString(),
            StartedAt = completedAt,
            CompletedAt = completedAt,
            Duration = durationMilliseconds is { } ms ? TimeSpan.FromMilliseconds(ms) : null,
        };

    private static async Task WaitForAsync(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!condition())
        {
            await Task.Delay(10, timeout.Token);
        }
    }

    private WorkerInfo CreateWorker(string workerId, params string[] queues) =>
        new()
        {
            WorkerId = workerId,
            Hostname = "test-host",
            Queues = queues,
            StartedAt = _time.GetUtcNow(),
            LastHeartbeat = _time.GetUtcNow(),
        };

    private sealed class FakeQueueMetrics : IQueueMetrics
    {
        private int _reads;

        public Dictionary<string, QueueMetricsData> Data { get; } = new(StringComparer.Ordinal);

        public int Reads => _reads;

        public ValueTask<IReadOnlyDictionary<string, QueueMetricsData>> GetAllMetricsAsync(
            CancellationToken cancellationToken = default
        )
        {
            Interlocked.Increment(ref _reads);
            return ValueTask.FromResult<IReadOnlyDictionary<string, QueueMetricsData>>(
                new Dictionary<string, QueueMetricsData>(Data, StringComparer.Ordinal)
            );
        }

        public ValueTask<QueueMetricsData> GetMetricsAsync(
            string queue,
            CancellationToken cancellationToken = default
        ) =>
            ValueTask.FromResult(
                Data.TryGetValue(queue, out var data)
                    ? data
                    : new QueueMetricsData { Queue = queue }
            );

        public ValueTask<long> GetWaitingCountAsync(
            string queue,
            CancellationToken cancellationToken = default
        ) => ValueTask.FromResult(Data.TryGetValue(queue, out var data) ? data.WaitingCount : 0);

        public ValueTask<long> GetRunningCountAsync(
            string queue,
            CancellationToken cancellationToken = default
        ) => ValueTask.FromResult(Data.TryGetValue(queue, out var data) ? data.RunningCount : 0);

        public ValueTask<long> GetProcessedCountAsync(
            string queue,
            CancellationToken cancellationToken = default
        ) => ValueTask.FromResult(Data.TryGetValue(queue, out var data) ? data.ProcessedCount : 0);

        public ValueTask<int> GetConsumerCountAsync(
            string queue,
            CancellationToken cancellationToken = default
        ) => ValueTask.FromResult(0);

        public ValueTask<IReadOnlyList<string>> GetQueuesAsync(
            CancellationToken cancellationToken = default
        ) => ValueTask.FromResult<IReadOnlyList<string>>([.. Data.Keys]);

        public ValueTask RecordStartedAsync(
            string queue,
            string taskId,
            CancellationToken cancellationToken = default
        ) => ValueTask.CompletedTask;

        public ValueTask RecordCompletedAsync(
            string queue,
            string taskId,
            bool success,
            TimeSpan duration,
            CancellationToken cancellationToken = default
        ) => ValueTask.CompletedTask;

        public ValueTask RecordEnqueuedAsync(
            string queue,
            CancellationToken cancellationToken = default
        ) => ValueTask.CompletedTask;

#pragma warning disable CA1822 // An interface implementation cannot be static
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
#pragma warning restore CA1822
    }

    private sealed class FakeHistoricalDataStore : IHistoricalDataStore
    {
        public AggregatedMetrics Aggregated { get; set; } =
            new()
            {
                From = DateTimeOffset.MinValue,
                To = DateTimeOffset.MinValue,
                Granularity = MetricsGranularity.Minute,
            };

        public Dictionary<string, TaskMetricsSummary> ByTaskName { get; } =
            new(StringComparer.Ordinal);

        public List<MetricsSnapshot> Snapshots { get; } = [];

        public int RetentionApplied { get; private set; }

        public ValueTask RecordMetricsAsync(
            MetricsSnapshot snapshot,
            CancellationToken cancellationToken = default
        )
        {
            Snapshots.Add(snapshot);
            return ValueTask.CompletedTask;
        }

        public ValueTask<AggregatedMetrics> GetMetricsAsync(
            DateTimeOffset from,
            DateTimeOffset until,
            MetricsGranularity granularity = MetricsGranularity.Hour,
            CancellationToken cancellationToken = default
        ) => ValueTask.FromResult(Aggregated);

        public async IAsyncEnumerable<MetricsDataPoint> GetTimeSeriesAsync(
            DateTimeOffset from,
            DateTimeOffset until,
            MetricsGranularity granularity = MetricsGranularity.Hour,
            [System.Runtime.CompilerServices.EnumeratorCancellation]
                CancellationToken cancellationToken = default
        )
        {
            await Task.CompletedTask;
            yield break;
        }

        public ValueTask<IReadOnlyDictionary<string, TaskMetricsSummary>> GetMetricsByTaskNameAsync(
            DateTimeOffset from,
            DateTimeOffset until,
            CancellationToken cancellationToken = default
        ) =>
            ValueTask.FromResult<IReadOnlyDictionary<string, TaskMetricsSummary>>(
                new Dictionary<string, TaskMetricsSummary>(ByTaskName, StringComparer.Ordinal)
            );

        public ValueTask<long> ApplyRetentionAsync(CancellationToken cancellationToken = default)
        {
            RetentionApplied++;
            return ValueTask.FromResult(0L);
        }

        public ValueTask<long> GetSnapshotCountAsync(
            CancellationToken cancellationToken = default
        ) => ValueTask.FromResult((long)Snapshots.Count);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
