using DotCelery.Core.Storage.Stores;

namespace DotCelery.Tests.Conformance.Stores;

/// <summary>
/// Conformance tests for <see cref="QueueMetrics"/>.
/// </summary>
public abstract class QueueMetricsConformanceTests : StoreConformanceTests
{
    // Each instance stands for a worker in its own process
    private QueueMetrics CreateMetrics() => new(Provider, CreateOptions(), Time);

    [Fact]
    public async Task RecordEnqueuedAndStarted_TrackWaitingAndRunningTasks()
    {
        var metrics = CreateMetrics();
        await metrics.RecordEnqueuedAsync("queue-1");
        await CreateMetrics().RecordEnqueuedAsync("queue-1");

        await metrics.RecordStartedAsync("queue-1", "task-1");

        Assert.Equal(1, await CreateMetrics().GetWaitingCountAsync("queue-1"));
        Assert.Equal(1, await CreateMetrics().GetRunningCountAsync("queue-1"));
    }

    [Fact]
    public async Task RecordCompletedAsync_CountsSuccessesAndFailures()
    {
        var metrics = CreateMetrics();
        for (var i = 0; i < 4; i++)
        {
            await metrics.RecordEnqueuedAsync("queue-1");
            await metrics.RecordStartedAsync("queue-1", $"task-{i}");
        }

        await metrics.RecordCompletedAsync("queue-1", "task-0", true, TimeSpan.FromSeconds(2));
        await metrics.RecordCompletedAsync("queue-1", "task-1", true, TimeSpan.FromSeconds(4));
        await metrics.RecordCompletedAsync("queue-1", "task-2", true, TimeSpan.FromSeconds(3));
        await metrics.RecordCompletedAsync("queue-1", "task-3", false, TimeSpan.FromSeconds(3));

        var data = await metrics.GetMetricsAsync("queue-1");
        Assert.Equal(0, data.WaitingCount);
        Assert.Equal(0, data.RunningCount);
        Assert.Equal(4, data.ProcessedCount);
        Assert.Equal(3, data.SuccessCount);
        Assert.Equal(1, data.FailureCount);
        Assert.Equal(0.75, data.SuccessRate, 0.01);
        Assert.Equal(TimeSpan.FromSeconds(3), data.AverageDuration);
        Assert.Equal(4, await metrics.GetProcessedCountAsync("queue-1"));
    }

    [Fact]
    public async Task GetMetricsAsync_RecordsTheLastActivityTimes()
    {
        var metrics = CreateMetrics();
        await metrics.RecordEnqueuedAsync("queue-1");
        Time.Advance(TimeSpan.FromSeconds(10));
        await metrics.RecordStartedAsync("queue-1", "task-1");
        await metrics.RecordCompletedAsync("queue-1", "task-1", true, TimeSpan.FromSeconds(10));

        var data = await metrics.GetMetricsAsync("queue-1");

        Assert.Equal(Start, data.LastEnqueuedAt);
        Assert.Equal(Start.AddSeconds(10), data.LastCompletedAt);
    }

    [Fact]
    public async Task GetMetricsAsync_UnknownQueue_ReturnsEmptyMetrics()
    {
        var data = await CreateMetrics().GetMetricsAsync("unknown");

        Assert.Equal("unknown", data.Queue);
        Assert.Equal(0, data.ProcessedCount);
        Assert.Null(data.AverageDuration);
        Assert.Null(data.LastEnqueuedAt);
    }

    [Fact]
    public async Task RunningTask_StopsCountingAfterTheExecutionTimeout()
    {
        var metrics = CreateMetrics();
        await metrics.RecordStartedAsync("queue-1", "task-1");

        // The worker stopped without recording the completion
        Time.Advance(StoreOptions.ExecutionTimeout);

        Assert.Equal(0, await metrics.GetRunningCountAsync("queue-1"));
    }

    [Fact]
    public async Task GetQueuesAsync_ListsQueuesWithActivity()
    {
        var metrics = CreateMetrics();
        await metrics.RecordEnqueuedAsync("queue-1");
        await metrics.RecordStartedAsync("queue/2", "task-1");
        await metrics.RecordEnqueuedAsync("queue-3");
        await metrics.RecordStartedAsync("queue-3", "task-2");
        await metrics.RecordCompletedAsync("queue-3", "task-2", true, TimeSpan.FromSeconds(1));

        var queues = await CreateMetrics().GetQueuesAsync();
        var all = await metrics.GetAllMetricsAsync();

        Assert.Equal(["queue-1", "queue-3", "queue/2"], queues);
        Assert.Equal(queues, all.Keys.Order(StringComparer.Ordinal));
        Assert.Equal(1, all["queue/2"].RunningCount);
        Assert.Equal(1, all["queue-3"].ProcessedCount);
    }

    [Fact]
    public async Task Queues_AreIndependent()
    {
        var metrics = CreateMetrics();
        await metrics.RecordEnqueuedAsync("queue-1");
        await metrics.RecordEnqueuedAsync("queue-1");
        await metrics.RecordEnqueuedAsync("queue-2");

        Assert.Equal(2, await metrics.GetWaitingCountAsync("queue-1"));
        Assert.Equal(1, await metrics.GetWaitingCountAsync("queue-2"));
        Assert.Equal(0, await metrics.GetConsumerCountAsync("queue-1"));
    }

    [Fact]
    public async Task RecordCompletedAsync_ConcurrentWorkers_CountEveryTask()
    {
        await RunConcurrentlyAsync(
            10,
            async i =>
            {
                var metrics = CreateMetrics();
                await metrics.RecordStartedAsync("queue-1", $"task-{i}");
                await metrics.RecordCompletedAsync(
                    "queue-1",
                    $"task-{i}",
                    true,
                    TimeSpan.FromSeconds(1)
                );
                return i;
            }
        );

        var data = await CreateMetrics().GetMetricsAsync("queue-1");
        Assert.Equal(10, data.ProcessedCount);
        Assert.Equal(0, data.RunningCount);
        Assert.Equal(TimeSpan.FromSeconds(1), data.AverageDuration);
    }
}
