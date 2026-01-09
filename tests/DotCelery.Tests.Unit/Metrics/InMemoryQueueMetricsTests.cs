using DotCelery.Backend.InMemory.Metrics;

namespace DotCelery.Tests.Unit.Metrics;

/// <summary>
/// Tests for <see cref="InMemoryQueueMetrics"/>.
/// </summary>
public sealed class InMemoryQueueMetricsTests
{
    private readonly InMemoryQueueMetrics _metrics = new();

    [Fact]
    public async Task RecordEnqueuedAsync_IncrementsWaitingCount()
    {
        // Act
        await _metrics.RecordEnqueuedAsync("queue-1");
        await _metrics.RecordEnqueuedAsync("queue-1");

        var count = await _metrics.GetWaitingCountAsync("queue-1");

        // Assert
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task RecordStartedAsync_DecrementsWaitingAndIncrementsRunning()
    {
        // Arrange
        await _metrics.RecordEnqueuedAsync("queue-1");
        await _metrics.RecordEnqueuedAsync("queue-1");

        // Act
        await _metrics.RecordStartedAsync("queue-1", "task-1");

        var waiting = await _metrics.GetWaitingCountAsync("queue-1");
        var running = await _metrics.GetRunningCountAsync("queue-1");

        // Assert
        Assert.Equal(1, waiting);
        Assert.Equal(1, running);
    }

    [Fact]
    public async Task RecordCompletedAsync_Success_IncrementsSuccessAndProcessed()
    {
        // Arrange
        await _metrics.RecordEnqueuedAsync("queue-1");
        await _metrics.RecordStartedAsync("queue-1", "task-1");

        // Act
        await _metrics.RecordCompletedAsync(
            "queue-1",
            "task-1",
            success: true,
            TimeSpan.FromSeconds(1)
        );

        var running = await _metrics.GetRunningCountAsync("queue-1");
        var processed = await _metrics.GetProcessedCountAsync("queue-1");
        var data = await _metrics.GetMetricsAsync("queue-1");

        // Assert
        Assert.Equal(0, running);
        Assert.Equal(1, processed);
        Assert.Equal(1, data.SuccessCount);
        Assert.Equal(0, data.FailureCount);
    }

    [Fact]
    public async Task RecordCompletedAsync_Failure_IncrementsFailureAndProcessed()
    {
        // Arrange
        await _metrics.RecordEnqueuedAsync("queue-1");
        await _metrics.RecordStartedAsync("queue-1", "task-1");

        // Act
        await _metrics.RecordCompletedAsync(
            "queue-1",
            "task-1",
            success: false,
            TimeSpan.FromSeconds(1)
        );

        var data = await _metrics.GetMetricsAsync("queue-1");

        // Assert
        Assert.Equal(1, data.ProcessedCount);
        Assert.Equal(0, data.SuccessCount);
        Assert.Equal(1, data.FailureCount);
    }

    [Fact]
    public async Task GetConsumerCountAsync_ReturnsRegisteredConsumers()
    {
        // Arrange
        _metrics.RegisterConsumer("queue-1");
        _metrics.RegisterConsumer("queue-1");

        // Act
        var count = await _metrics.GetConsumerCountAsync("queue-1");

        // Assert
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task UnregisterConsumer_DecrementsCount()
    {
        // Arrange
        _metrics.RegisterConsumer("queue-1");
        _metrics.RegisterConsumer("queue-1");

        // Act
        _metrics.UnregisterConsumer("queue-1");
        var count = await _metrics.GetConsumerCountAsync("queue-1");

        // Assert
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task GetQueuesAsync_ReturnsAllKnownQueues()
    {
        // Arrange
        await _metrics.RecordEnqueuedAsync("queue-1");
        await _metrics.RecordEnqueuedAsync("queue-2");
        await _metrics.RecordEnqueuedAsync("queue-3");

        // Act
        var queues = await _metrics.GetQueuesAsync();

        // Assert
        Assert.Equal(3, queues.Count);
        Assert.Contains("queue-1", queues);
        Assert.Contains("queue-2", queues);
        Assert.Contains("queue-3", queues);
    }

    [Fact]
    public async Task GetAllMetricsAsync_ReturnsMetricsForAllQueues()
    {
        // Arrange
        await _metrics.RecordEnqueuedAsync("queue-1");
        await _metrics.RecordEnqueuedAsync("queue-2");

        // Act
        var allMetrics = await _metrics.GetAllMetricsAsync();

        // Assert
        Assert.Equal(2, allMetrics.Count);
        Assert.Contains("queue-1", allMetrics.Keys);
        Assert.Contains("queue-2", allMetrics.Keys);
    }

    [Fact]
    public async Task GetMetricsAsync_CalculatesSuccessRate()
    {
        // Arrange
        await _metrics.RecordEnqueuedAsync("queue-1");
        await _metrics.RecordEnqueuedAsync("queue-1");
        await _metrics.RecordEnqueuedAsync("queue-1");
        await _metrics.RecordEnqueuedAsync("queue-1");
        await _metrics.RecordStartedAsync("queue-1", "task-1");
        await _metrics.RecordStartedAsync("queue-1", "task-2");
        await _metrics.RecordStartedAsync("queue-1", "task-3");
        await _metrics.RecordStartedAsync("queue-1", "task-4");
        await _metrics.RecordCompletedAsync("queue-1", "task-1", true, TimeSpan.FromSeconds(1));
        await _metrics.RecordCompletedAsync("queue-1", "task-2", true, TimeSpan.FromSeconds(1));
        await _metrics.RecordCompletedAsync("queue-1", "task-3", true, TimeSpan.FromSeconds(1));
        await _metrics.RecordCompletedAsync("queue-1", "task-4", false, TimeSpan.FromSeconds(1));

        // Act
        var data = await _metrics.GetMetricsAsync("queue-1");

        // Assert
        Assert.Equal(4, data.ProcessedCount);
        Assert.Equal(3, data.SuccessCount);
        Assert.Equal(1, data.FailureCount);
        Assert.Equal(0.75, data.SuccessRate, 0.01);
    }

    [Fact]
    public async Task GetMetricsAsync_CalculatesAverageDuration()
    {
        // Arrange
        await _metrics.RecordEnqueuedAsync("queue-1");
        await _metrics.RecordEnqueuedAsync("queue-1");
        await _metrics.RecordStartedAsync("queue-1", "task-1");
        await _metrics.RecordStartedAsync("queue-1", "task-2");
        await _metrics.RecordCompletedAsync("queue-1", "task-1", true, TimeSpan.FromSeconds(2));
        await _metrics.RecordCompletedAsync("queue-1", "task-2", true, TimeSpan.FromSeconds(4));

        // Act
        var data = await _metrics.GetMetricsAsync("queue-1");

        // Assert
        Assert.NotNull(data.AverageDuration);
        Assert.Equal(TimeSpan.FromSeconds(3), data.AverageDuration.Value);
    }

    [Fact]
    public async Task GetMetricsAsync_RecordsLastEnqueuedAt()
    {
        // Act
        await _metrics.RecordEnqueuedAsync("queue-1");
        var data = await _metrics.GetMetricsAsync("queue-1");

        // Assert
        Assert.NotNull(data.LastEnqueuedAt);
        Assert.True(data.LastEnqueuedAt > DateTimeOffset.UtcNow.AddMinutes(-1));
    }

    [Fact]
    public async Task GetMetricsAsync_RecordsLastCompletedAt()
    {
        // Arrange
        await _metrics.RecordEnqueuedAsync("queue-1");
        await _metrics.RecordStartedAsync("queue-1", "task-1");

        // Act
        await _metrics.RecordCompletedAsync("queue-1", "task-1", true, TimeSpan.FromSeconds(1));
        var data = await _metrics.GetMetricsAsync("queue-1");

        // Assert
        Assert.NotNull(data.LastCompletedAt);
        Assert.True(data.LastCompletedAt > DateTimeOffset.UtcNow.AddMinutes(-1));
    }

    [Fact]
    public async Task Clear_ResetsAllMetrics()
    {
        // Arrange
        _metrics.RegisterConsumer("queue-1");
        await _metrics.RecordEnqueuedAsync("queue-1");

        // Act
        _metrics.Clear();
        var queues = await _metrics.GetQueuesAsync();

        // Assert
        Assert.Empty(queues);
    }

    [Fact]
    public async Task MultipleQueues_AreIndependent()
    {
        // Arrange
        await _metrics.RecordEnqueuedAsync("queue-1");
        await _metrics.RecordEnqueuedAsync("queue-1");
        await _metrics.RecordEnqueuedAsync("queue-2");

        // Act
        var waiting1 = await _metrics.GetWaitingCountAsync("queue-1");
        var waiting2 = await _metrics.GetWaitingCountAsync("queue-2");

        // Assert
        Assert.Equal(2, waiting1);
        Assert.Equal(1, waiting2);
    }
}
