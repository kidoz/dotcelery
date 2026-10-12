using DotCelery.Core.Dashboard;

namespace DotCelery.Tests.Conformance.Stores;

/// <summary>
/// Conformance tests for <see cref="IWorkerRegistry"/>, so every implementation registers,
/// heartbeats, cleans up, and removes workers the same way.
/// </summary>
public abstract class WorkerRegistryConformanceTests : StoreConformanceTests
{
    /// <summary>
    /// The heartbeat budget the cleanup tests use.
    /// </summary>
    protected static readonly TimeSpan HeartbeatTimeout = TimeSpan.FromSeconds(90);

    protected abstract IWorkerRegistry CreateRegistry();

    protected WorkerInfo CreateWorker(string workerId, params string[] queues) =>
        new()
        {
            WorkerId = workerId,
            Hostname = "test-host",
            ProcessId = 1,
            Queues = queues,
            Concurrency = 4,
            StartedAt = Start,
            LastHeartbeat = Time.GetUtcNow(),
        };

    protected static async Task<List<WorkerInfo>> ListAsync(IWorkerRegistry registry)
    {
        var workers = new List<WorkerInfo>();

        await foreach (var worker in registry.GetActiveWorkersAsync())
        {
            workers.Add(worker);
        }

        return workers;
    }

    [Fact]
    public async Task Register_ThenGetAndList_ReturnsTheWorker()
    {
        var registry = CreateRegistry();
        var worker = CreateWorker("worker-1", "celery", "priority");

        await registry.RegisterWorkerAsync(worker);

        var fetched = await registry.GetWorkerAsync("worker-1");
        Assert.NotNull(fetched);
        Assert.Equal("worker-1", fetched.WorkerId);
        Assert.Equal("test-host", fetched.Hostname);
        Assert.Equal(4, fetched.Concurrency);
        Assert.Equal(["celery", "priority"], fetched.Queues);
        Assert.Equal(Start, fetched.StartedAt);
        Assert.Equal(WorkerStatus.Online, fetched.Status);

        Assert.Equal("worker-1", Assert.Single(await ListAsync(registry)).WorkerId);
    }

    [Fact]
    public async Task GetWorker_ForAnUnknownWorker_ReturnsNull()
    {
        var registry = CreateRegistry();

        Assert.Null(await registry.GetWorkerAsync("no-such-worker"));
    }

    [Fact]
    public async Task Register_TheSameWorkerTwice_KeepsOneRecord()
    {
        var registry = CreateRegistry();

        await registry.RegisterWorkerAsync(CreateWorker("worker-1", "celery"));
        Time.Advance(TimeSpan.FromSeconds(30));
        await registry.RegisterWorkerAsync(CreateWorker("worker-1", "priority"));

        var workers = await ListAsync(registry);
        Assert.Equal("priority", Assert.Single(workers).Queues.Single());
    }

    [Fact]
    public async Task Heartbeat_UpdatesTheCountsAndTheLastHeartbeat()
    {
        var registry = CreateRegistry();
        await registry.RegisterWorkerAsync(CreateWorker("worker-1", "celery"));
        Time.Advance(TimeSpan.FromSeconds(30));

        await registry.HeartbeatAsync("worker-1", activeTasks: 3, processedCount: 42);

        var worker = await registry.GetWorkerAsync("worker-1");
        Assert.NotNull(worker);
        Assert.Equal(3, worker.ActiveTasks);
        Assert.Equal(42, worker.ProcessedCount);
        Assert.Equal(Time.GetUtcNow(), worker.LastHeartbeat);
        Assert.Equal(WorkerStatus.Online, worker.Status);
    }

    [Fact]
    public async Task Heartbeat_OfAnUnknownWorker_RegistersNothing()
    {
        var registry = CreateRegistry();

        await registry.HeartbeatAsync("no-such-worker", activeTasks: 1);

        Assert.Null(await registry.GetWorkerAsync("no-such-worker"));
        Assert.Empty(await ListAsync(registry));
    }

    [Fact]
    public async Task Heartbeat_RevivesAWorkerMarkedUnresponsive()
    {
        var registry = CreateRegistry();
        await registry.RegisterWorkerAsync(CreateWorker("worker-1", "celery"));
        Time.Advance(HeartbeatTimeout + TimeSpan.FromSeconds(1));
        await registry.CleanupStaleWorkersAsync(HeartbeatTimeout);
        Assert.Equal(
            WorkerStatus.Unresponsive,
            (await registry.GetWorkerAsync("worker-1"))!.Status
        );

        await registry.HeartbeatAsync("worker-1", activeTasks: 1);

        Assert.Equal(WorkerStatus.Online, (await registry.GetWorkerAsync("worker-1"))!.Status);
    }

    [Fact]
    public async Task Unregister_RemovesTheWorker()
    {
        var registry = CreateRegistry();
        await registry.RegisterWorkerAsync(CreateWorker("worker-1", "celery"));
        await registry.RegisterWorkerAsync(CreateWorker("worker-2", "celery"));

        await registry.UnregisterWorkerAsync("worker-1");

        Assert.Null(await registry.GetWorkerAsync("worker-1"));
        Assert.Equal("worker-2", Assert.Single(await ListAsync(registry)).WorkerId);
    }

    [Fact]
    public async Task Cleanup_LeavesAWorkerThatHeartedRecently()
    {
        var registry = CreateRegistry();
        await registry.RegisterWorkerAsync(CreateWorker("worker-1", "celery"));

        Time.Advance(HeartbeatTimeout - TimeSpan.FromSeconds(10));
        await registry.HeartbeatAsync("worker-1");
        Time.Advance(TimeSpan.FromSeconds(10));

        Assert.Equal(0, await registry.CleanupStaleWorkersAsync(HeartbeatTimeout));
        Assert.Equal(WorkerStatus.Online, (await registry.GetWorkerAsync("worker-1"))!.Status);
    }

    [Fact]
    public async Task Cleanup_MarksAStaleWorkerUnresponsive_ThenRemovesIt()
    {
        var registry = CreateRegistry();
        await registry.RegisterWorkerAsync(CreateWorker("worker-1", "celery"));

        // The first cleanup past the budget marks it, and reports no removal
        Time.Advance(HeartbeatTimeout + TimeSpan.FromSeconds(1));
        Assert.Equal(0, await registry.CleanupStaleWorkersAsync(HeartbeatTimeout));
        Assert.Equal(
            WorkerStatus.Unresponsive,
            (await registry.GetWorkerAsync("worker-1"))!.Status
        );

        // The next one removes it and says so
        Assert.Equal(1, await registry.CleanupStaleWorkersAsync(HeartbeatTimeout));
        Assert.Null(await registry.GetWorkerAsync("worker-1"));
        Assert.Empty(await ListAsync(registry));
    }

    [Fact]
    public async Task Cleanup_CountsEachRemovedWorkerOnce()
    {
        var registry = CreateRegistry();
        await registry.RegisterWorkerAsync(CreateWorker("worker-1", "celery"));
        await registry.RegisterWorkerAsync(CreateWorker("worker-2", "celery"));

        Time.Advance(HeartbeatTimeout + TimeSpan.FromSeconds(1));
        Assert.Equal(0, await registry.CleanupStaleWorkersAsync(HeartbeatTimeout));
        Assert.Equal(2, (await ListAsync(registry)).Count);

        // Every stale worker is removed, and the ones already removed are not counted again
        Assert.Equal(2, await registry.CleanupStaleWorkersAsync(HeartbeatTimeout));
        Assert.Equal(0, await registry.CleanupStaleWorkersAsync(HeartbeatTimeout));
        Assert.Empty(await ListAsync(registry));
    }
}

/// <summary>
/// Adds what a registry backed by storage does beyond the contract: a worker whose record is not
/// refreshed stops being listed even when nothing cleans the registry up.
/// </summary>
public abstract class ExpiringWorkerRegistryConformanceTests : WorkerRegistryConformanceTests
{
    [Fact]
    public async Task AWorkerThatStoppedHeartbeating_IsNotListedAfterTheRetention()
    {
        var registry = CreateRegistry();
        await registry.RegisterWorkerAsync(CreateWorker("worker-1", "celery"));

        // A heartbeat near the end of the retention keeps the record, and the record outlives
        // the first expiry
        Time.Advance(StoreOptions.WorkerRetention - TimeSpan.FromMinutes(1));
        await registry.HeartbeatAsync("worker-1");
        Time.Advance(TimeSpan.FromMinutes(1) + TimeSpan.FromSeconds(1));
        Assert.Single(await ListAsync(registry));

        // Without another heartbeat it expires, without any cleanup
        Time.Advance(StoreOptions.WorkerRetention);
        Assert.Null(await registry.GetWorkerAsync("worker-1"));
        Assert.Empty(await ListAsync(registry));
    }
}
