using DotCelery.Core.Dashboard;
using DotCelery.Core.Storage.Stores;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Conformance.Stores;

/// <summary>
/// Conformance tests for <see cref="HistoricalDataStore"/>.
/// </summary>
public abstract class HistoricalDataStoreConformanceTests : StoreConformanceTests
{
    private HistoricalDataStore CreateStore(HistoricalDataOptions? options = null) =>
        new(
            Provider,
            Options.Create(options ?? new HistoricalDataOptions()),
            CreateOptions(),
            Time
        );

    [Fact]
    public async Task GetMetricsAsync_AggregatesSnapshotsInTheRange()
    {
        var store = CreateStore();
        await store.RecordMetricsAsync(Snapshot(Start.AddHours(-3), "task1", 100, 10, 0));
        await store.RecordMetricsAsync(Snapshot(Start.AddMinutes(-30), "task1", 10, 2, 1));
        await store.RecordMetricsAsync(Snapshot(Start.AddMinutes(-15), "task1", 15, 3, 2));
        await store.RecordMetricsAsync(Snapshot(Start, "task2", 5, 0, 0));

        var metrics = await CreateStore().GetMetricsAsync(Start.AddHours(-1), Start);

        Assert.Equal(30, metrics.SuccessCount);
        Assert.Equal(5, metrics.FailureCount);
        Assert.Equal(3, metrics.RetryCount);
        Assert.Equal(35, metrics.TotalProcessed);
        Assert.Equal(35 / 3600.0, metrics.TasksPerSecond, 6);
        Assert.Equal(30 / 35.0, metrics.SuccessRate, 6);
    }

    [Fact]
    public async Task GetMetricsAsync_AveragesExecutionTimes()
    {
        var store = CreateStore();
        await store.RecordMetricsAsync(
            Snapshot(Start.AddMinutes(-30), "task1", 10, 0, 0) with
            {
                AverageExecutionTime = TimeSpan.FromSeconds(5),
            }
        );
        await store.RecordMetricsAsync(
            Snapshot(Start.AddMinutes(-15), "task1", 10, 0, 0) with
            {
                AverageExecutionTime = TimeSpan.FromSeconds(15),
            }
        );

        var metrics = await store.GetMetricsAsync(Start.AddHours(-1), Start);

        Assert.Equal(TimeSpan.FromSeconds(10), metrics.AverageExecutionTime);
    }

    [Fact]
    public async Task GetMetricsAsync_NoSnapshots_ReturnsZeros()
    {
        var metrics = await CreateStore().GetMetricsAsync(Start.AddHours(-1), Start);

        Assert.Equal(0, metrics.TotalProcessed);
        Assert.Null(metrics.AverageExecutionTime);
        Assert.Equal(0, metrics.TasksPerSecond);
    }

    [Fact]
    public async Task GetTimeSeriesAsync_AggregatesByGranularity()
    {
        var store = CreateStore();
        var hour = Start.AddHours(-3);
        await store.RecordMetricsAsync(Snapshot(hour.AddMinutes(10), "task1", 5, 1, 0));
        await store.RecordMetricsAsync(Snapshot(hour.AddMinutes(20), "task1", 5, 1, 0));
        await store.RecordMetricsAsync(Snapshot(hour.AddMinutes(70), "task1", 10, 0, 0));

        var points = await ToListAsync(store.GetTimeSeriesAsync(hour, hour.AddHours(2)));

        Assert.Equal([hour, hour.AddHours(1)], points.Select(p => p.Timestamp));
        Assert.Equal(10, points[0].SuccessCount);
        Assert.Equal(2, points[0].FailureCount);
        Assert.Equal(10, points[1].SuccessCount);
    }

    [Fact]
    public async Task GetTimeSeriesAsync_ReturnsAtMostMaxDataPoints()
    {
        var store = CreateStore(new HistoricalDataOptions { MaxDataPoints = 2 });
        for (var i = 1; i <= 3; i++)
        {
            await store.RecordMetricsAsync(Snapshot(Start.AddMinutes(-i), "task1", 1, 0, 0));
        }

        var points = await ToListAsync(
            store.GetTimeSeriesAsync(Start.AddHours(-1), Start, MetricsGranularity.Minute)
        );

        Assert.Equal([Start.AddMinutes(-3), Start.AddMinutes(-2)], points.Select(p => p.Timestamp));
    }

    [Fact]
    public async Task GetMetricsByTaskNameAsync_GroupsNamedSnapshots()
    {
        var store = CreateStore();
        await store.RecordMetricsAsync(
            Snapshot(Start.AddMinutes(-30), "task1", 10, 2, 1) with
            {
                AverageExecutionTime = TimeSpan.FromSeconds(1),
            }
        );
        await store.RecordMetricsAsync(
            Snapshot(Start.AddMinutes(-20), "task1", 5, 1, 0) with
            {
                AverageExecutionTime = TimeSpan.FromSeconds(3),
            }
        );
        await store.RecordMetricsAsync(Snapshot(Start.AddMinutes(-10), "task2", 20, 0, 0));
        await store.RecordMetricsAsync(Snapshot(Start.AddMinutes(-5), null, 100, 10, 0));

        var byTask = await store.GetMetricsByTaskNameAsync(Start.AddHours(-1), Start);

        Assert.Equal(["task1", "task2"], byTask.Keys.Order());
        Assert.Equal(18, byTask["task1"].TotalCount);
        Assert.Equal(15, byTask["task1"].SuccessCount);
        Assert.Equal(TimeSpan.FromSeconds(1), byTask["task1"].MinExecutionTime);
        Assert.Equal(TimeSpan.FromSeconds(3), byTask["task1"].MaxExecutionTime);
        Assert.Equal(TimeSpan.FromSeconds(2), byTask["task1"].AverageExecutionTime);
        Assert.Equal(20, byTask["task2"].SuccessCount);
    }

    [Fact]
    public async Task RecordMetricsAsync_SameTimestampAndTask_ReplacesTheSnapshot()
    {
        var store = CreateStore();
        await store.RecordMetricsAsync(Snapshot(Start, "task1", 10, 0, 0));
        await store.RecordMetricsAsync(Snapshot(Start, "task1", 20, 0, 0));
        await store.RecordMetricsAsync(Snapshot(Start, "task2", 5, 0, 0));
        await store.RecordMetricsAsync(Snapshot(Start.AddMilliseconds(1), "task1", 1, 0, 0));

        var byTask = await store.GetMetricsByTaskNameAsync(Start, Start.AddSeconds(1));

        Assert.Equal(3, await store.GetSnapshotCountAsync());
        Assert.Equal(21, byTask["task1"].SuccessCount);
        Assert.Equal(5, byTask["task2"].SuccessCount);
    }

    [Fact]
    public async Task Snapshots_ExpireAfterTheRetentionPeriod()
    {
        var store = CreateStore(
            new HistoricalDataOptions { RetentionPeriod = TimeSpan.FromHours(1) }
        );
        await store.RecordMetricsAsync(Snapshot(Start.AddHours(-2), "old", 1, 0, 0));
        await store.RecordMetricsAsync(Snapshot(Start.AddMinutes(-30), "recent", 1, 0, 0));
        await store.RecordMetricsAsync(Snapshot(Start, "new", 1, 0, 0));

        Assert.Equal(2, await store.GetSnapshotCountAsync());
        Assert.Equal(0, await store.ApplyRetentionAsync());

        Time.Advance(TimeSpan.FromMinutes(30));
        Assert.Equal(1, await store.GetSnapshotCountAsync());
    }

    private static MetricsSnapshot Snapshot(
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

    private static async Task<List<T>> ToListAsync<T>(IAsyncEnumerable<T> items)
    {
        var list = new List<T>();
        await foreach (var item in items)
        {
            list.Add(item);
        }

        return list;
    }
}
