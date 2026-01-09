using DotCelery.Backend.InMemory.Historical;
using DotCelery.Core.Dashboard;

namespace DotCelery.Tests.Unit.Historical;

/// <summary>
/// Tests for <see cref="InMemoryHistoricalDataStore"/>.
/// </summary>
public sealed class InMemoryHistoricalDataStoreTests : IAsyncDisposable
{
    private readonly InMemoryHistoricalDataStore _store = new();

    public async ValueTask DisposeAsync()
    {
        await _store.DisposeAsync();
    }

    [Fact]
    public async Task RecordMetricsAsync_StoresSnapshot()
    {
        // Arrange
        var snapshot = CreateSnapshot(DateTimeOffset.UtcNow, "test.task", 10, 2, 1);

        // Act
        await _store.RecordMetricsAsync(snapshot);
        var count = await _store.GetSnapshotCountAsync();

        // Assert
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task GetMetricsAsync_ReturnsAggregatedMetrics()
    {
        // Arrange
        var now = DateTimeOffset.UtcNow;
        await _store.RecordMetricsAsync(CreateSnapshot(now.AddMinutes(-30), "task1", 10, 2, 1));
        await _store.RecordMetricsAsync(CreateSnapshot(now.AddMinutes(-15), "task1", 15, 3, 2));
        await _store.RecordMetricsAsync(CreateSnapshot(now.AddMinutes(-5), "task2", 5, 0, 0));

        // Act
        var metrics = await _store.GetMetricsAsync(now.AddHours(-1), now, MetricsGranularity.Hour);

        // Assert
        Assert.Equal(30, metrics.SuccessCount);
        Assert.Equal(5, metrics.FailureCount);
        Assert.Equal(3, metrics.RetryCount);
        Assert.Equal(35, metrics.TotalProcessed);
    }

    [Fact]
    public async Task GetMetricsAsync_FiltersOutOfRangeSnapshots()
    {
        // Arrange
        var now = DateTimeOffset.UtcNow;
        await _store.RecordMetricsAsync(CreateSnapshot(now.AddHours(-3), "task1", 100, 10, 0));
        await _store.RecordMetricsAsync(CreateSnapshot(now.AddMinutes(-30), "task1", 10, 2, 1));

        // Act
        var metrics = await _store.GetMetricsAsync(now.AddHours(-1), now, MetricsGranularity.Hour);

        // Assert
        Assert.Equal(10, metrics.SuccessCount);
        Assert.Equal(2, metrics.FailureCount);
    }

    [Fact]
    public async Task GetTimeSeriesAsync_ReturnsDataPoints()
    {
        // Arrange
        var baseTime = DateTimeOffset.UtcNow.AddHours(-2);
        await _store.RecordMetricsAsync(CreateSnapshot(baseTime, "task1", 10, 2, 1));
        await _store.RecordMetricsAsync(CreateSnapshot(baseTime.AddMinutes(30), "task1", 15, 1, 0));
        await _store.RecordMetricsAsync(CreateSnapshot(baseTime.AddMinutes(90), "task2", 5, 0, 0));

        // Act
        var dataPoints = new List<MetricsDataPoint>();
        await foreach (
            var point in _store.GetTimeSeriesAsync(baseTime.AddMinutes(-10), baseTime.AddHours(3))
        )
        {
            dataPoints.Add(point);
        }

        // Assert
        Assert.True(dataPoints.Count >= 1);
    }

    [Fact]
    public async Task GetTimeSeriesAsync_AggregatesByGranularity()
    {
        // Arrange
        var baseTime = new DateTimeOffset(2024, 1, 1, 12, 0, 0, TimeSpan.Zero);
        await _store.RecordMetricsAsync(CreateSnapshot(baseTime.AddMinutes(10), "task1", 5, 1, 0));
        await _store.RecordMetricsAsync(CreateSnapshot(baseTime.AddMinutes(20), "task1", 5, 1, 0));
        await _store.RecordMetricsAsync(CreateSnapshot(baseTime.AddMinutes(70), "task1", 10, 0, 0));

        // Act
        var dataPoints = new List<MetricsDataPoint>();
        await foreach (
            var point in _store.GetTimeSeriesAsync(
                baseTime,
                baseTime.AddHours(2),
                MetricsGranularity.Hour
            )
        )
        {
            dataPoints.Add(point);
        }

        // Assert
        Assert.Equal(2, dataPoints.Count);
        var firstHour = dataPoints.First();
        Assert.Equal(10, firstHour.SuccessCount);
        Assert.Equal(2, firstHour.FailureCount);
    }

    [Fact]
    public async Task GetMetricsByTaskNameAsync_GroupsByTaskName()
    {
        // Arrange
        var now = DateTimeOffset.UtcNow;
        await _store.RecordMetricsAsync(CreateSnapshot(now.AddMinutes(-30), "task1", 10, 2, 1));
        await _store.RecordMetricsAsync(CreateSnapshot(now.AddMinutes(-20), "task1", 5, 1, 0));
        await _store.RecordMetricsAsync(CreateSnapshot(now.AddMinutes(-10), "task2", 20, 0, 0));

        // Act
        var metrics = await _store.GetMetricsByTaskNameAsync(now.AddHours(-1), now);

        // Assert
        Assert.Equal(2, metrics.Count);
        Assert.True(metrics.ContainsKey("task1"));
        Assert.True(metrics.ContainsKey("task2"));

        var task1Metrics = metrics["task1"];
        Assert.Equal(15, task1Metrics.SuccessCount);
        Assert.Equal(3, task1Metrics.FailureCount);

        var task2Metrics = metrics["task2"];
        Assert.Equal(20, task2Metrics.SuccessCount);
        Assert.Equal(0, task2Metrics.FailureCount);
    }

    [Fact]
    public async Task GetMetricsByTaskNameAsync_ExcludesNullTaskNames()
    {
        // Arrange
        var now = DateTimeOffset.UtcNow;
        await _store.RecordMetricsAsync(CreateSnapshot(now.AddMinutes(-30), null, 100, 10, 0));
        await _store.RecordMetricsAsync(CreateSnapshot(now.AddMinutes(-10), "task1", 10, 2, 0));

        // Act
        var metrics = await _store.GetMetricsByTaskNameAsync(now.AddHours(-1), now);

        // Assert
        Assert.Single(metrics);
        Assert.True(metrics.ContainsKey("task1"));
    }

    [Fact]
    public async Task ApplyRetentionAsync_RemovesOldSnapshots()
    {
        // Arrange
        var options = new HistoricalDataOptions { RetentionPeriod = TimeSpan.FromHours(1) };
        await using var store = new InMemoryHistoricalDataStore(options);

        var now = DateTimeOffset.UtcNow;
        await store.RecordMetricsAsync(CreateSnapshot(now.AddHours(-3), "task1", 10, 0, 0));
        await store.RecordMetricsAsync(CreateSnapshot(now.AddMinutes(-30), "task1", 10, 0, 0));
        await store.RecordMetricsAsync(CreateSnapshot(now.AddMinutes(-10), "task1", 10, 0, 0));

        // Act
        var removed = await store.ApplyRetentionAsync();
        var remaining = await store.GetSnapshotCountAsync();

        // Assert
        Assert.Equal(1, removed);
        Assert.Equal(2, remaining);
    }

    [Fact]
    public async Task GetMetricsAsync_CalculatesTasksPerSecond()
    {
        // Arrange
        var now = DateTimeOffset.UtcNow;
        await _store.RecordMetricsAsync(CreateSnapshot(now.AddMinutes(-30), "task1", 1800, 0, 0));

        // Act
        var metrics = await _store.GetMetricsAsync(now.AddHours(-1), now, MetricsGranularity.Hour);

        // Assert
        Assert.True(metrics.TasksPerSecond > 0);
    }

    [Fact]
    public async Task GetMetricsAsync_CalculatesSuccessRate()
    {
        // Arrange
        var now = DateTimeOffset.UtcNow;
        await _store.RecordMetricsAsync(CreateSnapshot(now.AddMinutes(-30), "task1", 80, 20, 0));

        // Act
        var metrics = await _store.GetMetricsAsync(now.AddHours(-1), now, MetricsGranularity.Hour);

        // Assert
        Assert.Equal(0.8, metrics.SuccessRate, 0.01);
    }

    [Fact]
    public async Task GetMetricsAsync_EmptyStore_ReturnsZeroValues()
    {
        // Act
        var metrics = await _store.GetMetricsAsync(
            DateTimeOffset.UtcNow.AddHours(-1),
            DateTimeOffset.UtcNow,
            MetricsGranularity.Hour
        );

        // Assert
        Assert.Equal(0, metrics.TotalProcessed);
        Assert.Equal(0, metrics.SuccessCount);
        Assert.Equal(0, metrics.FailureCount);
    }

    [Fact]
    public async Task GetTimeSeriesAsync_EmptyStore_ReturnsEmpty()
    {
        // Act
        var dataPoints = new List<MetricsDataPoint>();
        await foreach (
            var point in _store.GetTimeSeriesAsync(
                DateTimeOffset.UtcNow.AddHours(-1),
                DateTimeOffset.UtcNow
            )
        )
        {
            dataPoints.Add(point);
        }

        // Assert
        Assert.Empty(dataPoints);
    }

    [Fact]
    public async Task MultipleSnapshots_SameTimestamp_LastOneWins()
    {
        // The implementation uses timestamp as dictionary key, so same timestamps overwrite
        // Arrange
        var timestamp = DateTimeOffset.UtcNow;
        await _store.RecordMetricsAsync(CreateSnapshot(timestamp, "task1", 10, 0, 0));
        await _store.RecordMetricsAsync(CreateSnapshot(timestamp, "task2", 20, 0, 0));

        // Act
        var count = await _store.GetSnapshotCountAsync();
        var metrics = await _store.GetMetricsByTaskNameAsync(
            timestamp.AddHours(-1),
            timestamp.AddHours(1)
        );

        // Assert - only one snapshot remains (last one wins)
        Assert.Equal(1, count);
        Assert.Single(metrics);
        Assert.True(metrics.ContainsKey("task2"));
    }

    [Fact]
    public async Task MultipleSnapshots_DifferentTimestamps_AreAllStored()
    {
        // Arrange
        var baseTime = DateTimeOffset.UtcNow;
        await _store.RecordMetricsAsync(CreateSnapshot(baseTime, "task1", 10, 0, 0));
        await _store.RecordMetricsAsync(
            CreateSnapshot(baseTime.AddMilliseconds(1), "task2", 20, 0, 0)
        );

        // Act
        var count = await _store.GetSnapshotCountAsync();

        // Assert
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task GetMetricsAsync_CalculatesAverageExecutionTime()
    {
        // Arrange
        var now = DateTimeOffset.UtcNow;
        await _store.RecordMetricsAsync(
            CreateSnapshotWithDuration(now.AddMinutes(-30), "task1", 10, 0, TimeSpan.FromSeconds(5))
        );
        await _store.RecordMetricsAsync(
            CreateSnapshotWithDuration(
                now.AddMinutes(-15),
                "task1",
                10,
                0,
                TimeSpan.FromSeconds(15)
            )
        );

        // Act
        var metrics = await _store.GetMetricsAsync(now.AddHours(-1), now, MetricsGranularity.Hour);

        // Assert
        Assert.NotNull(metrics.AverageExecutionTime);
        Assert.Equal(TimeSpan.FromSeconds(10), metrics.AverageExecutionTime);
    }

    private static MetricsSnapshot CreateSnapshot(
        DateTimeOffset timestamp,
        string? taskName,
        long success,
        long failure,
        long retry
    ) =>
        new()
        {
            Timestamp = timestamp,
            TaskName = taskName,
            SuccessCount = success,
            FailureCount = failure,
            RetryCount = retry,
        };

    private static MetricsSnapshot CreateSnapshotWithDuration(
        DateTimeOffset timestamp,
        string? taskName,
        long success,
        long failure,
        TimeSpan avgDuration
    ) =>
        new()
        {
            Timestamp = timestamp,
            TaskName = taskName,
            SuccessCount = success,
            FailureCount = failure,
            AverageExecutionTime = avgDuration,
        };
}
