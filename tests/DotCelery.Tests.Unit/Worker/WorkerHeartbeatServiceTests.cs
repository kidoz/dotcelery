using System.Runtime.CompilerServices;
using DotCelery.Core.Dashboard;
using DotCelery.Worker;
using DotCelery.Worker.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;

namespace DotCelery.Tests.Unit.Worker;

/// <summary>
/// Runs the worker heartbeat against a registry, so registering on start, heartbeating the live
/// activity counts, and unregistering on stop are proven.
/// </summary>
public sealed class WorkerHeartbeatServiceTests
{
    private readonly FakeTimeProvider _time = new(
        new DateTimeOffset(2026, 10, 8, 12, 0, 0, TimeSpan.Zero)
    );
    private readonly FakeWorkerRegistry _registry = new();
    private readonly WorkerActivity _activity = new();

    [Fact]
    public async Task AWorker_RegistersHeartbeatsItsActivityAndUnregisters()
    {
        var service = CreateService();
        await service.StartAsync(CancellationToken.None);

        await WaitForTickAsync(() => _registry.Registered.Count == 1);

        var registered = Assert.Single(_registry.Registered);
        Assert.Equal("worker-test", registered.WorkerId);
        Assert.Equal("celery", Assert.Single(registered.Queues));
        Assert.Equal(WorkerStatus.Online, registered.Status);

        _activity.IncrementActiveTasks();
        _activity.IncrementProcessedTasks();
        _activity.IncrementProcessedTasks();

        await WaitForTickAsync(() => _registry.Heartbeats.Count >= 1);

        var (workerId, activeTasks, processedCount) = _registry.Heartbeats[0];
        Assert.Equal("worker-test", workerId);
        Assert.Equal(1, activeTasks);
        Assert.Equal(2, processedCount);

        await service.StopAsync(CancellationToken.None);

        Assert.Equal("worker-test", Assert.Single(_registry.Unregistered));
    }

    [Fact]
    public async Task WithoutARegistry_TheHeartbeatIsIdle()
    {
        var service = new WorkerHeartbeatService(
            Options.Create(new WorkerOptions { WorkerName = "worker-test" }),
            NullLogger<WorkerHeartbeatService>.Instance,
            _activity,
            registry: null,
            _time
        );

        await service.StartAsync(CancellationToken.None);
        await service.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RegistryFailure_RetriesAndResumesHeartbeats(bool registrationFails)
    {
        _registry.FailRegistration = registrationFails;
        _registry.FailHeartbeat = !registrationFails;
        using var service = CreateService();
        await service.StartAsync(CancellationToken.None);
        await WaitForTickAsync(() => _registry.Heartbeats.Count > 0);
        Assert.False(service.ExecuteTask!.IsCompleted);
        await service.StopAsync(CancellationToken.None);
        Assert.Single(_registry.Unregistered);
    }

    [Fact]
    public async Task UnregisterFailure_DoesNotFaultShutdown()
    {
        _registry.FailUnregister = true;
        using var service = CreateService();
        await service.StartAsync(CancellationToken.None);
        await WaitForTickAsync(() => _registry.Registered.Count > 0);
        await service.StopAsync(CancellationToken.None);
        await service.ExecuteTask!;
    }

    private WorkerHeartbeatService CreateService() =>
        new(
            Options.Create(
                new WorkerOptions
                {
                    WorkerName = "worker-test",
                    Queues = ["celery"],
                    WorkerHeartbeatInterval = TimeSpan.FromSeconds(1),
                }
            ),
            NullLogger<WorkerHeartbeatService>.Instance,
            _activity,
            _registry,
            _time
        );

    private async Task WaitForTickAsync(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!condition())
        {
            _time.Advance(TimeSpan.FromSeconds(1));
            await Task.Delay(10, timeout.Token);
        }
    }

    private sealed class FakeWorkerRegistry : IWorkerRegistry
    {
        public bool FailRegistration { get; set; }
        public bool FailHeartbeat { get; set; }
        public bool FailUnregister { get; set; }

        public List<WorkerInfo> Registered { get; } = [];

        public List<(string WorkerId, int ActiveTasks, long ProcessedCount)> Heartbeats { get; } =
        [];

        public List<string> Unregistered { get; } = [];

        public ValueTask RegisterWorkerAsync(
            WorkerInfo worker,
            CancellationToken cancellationToken = default
        )
        {
            if (FailRegistration)
            {
                FailRegistration = false;
                throw new IOException("registration unavailable");
            }
            Registered.Add(worker);
            return ValueTask.CompletedTask;
        }

        public ValueTask HeartbeatAsync(
            string workerId,
            int activeTasks = 0,
            long processedCount = 0,
            CancellationToken cancellationToken = default
        )
        {
            if (FailHeartbeat)
            {
                FailHeartbeat = false;
                throw new IOException("heartbeat unavailable");
            }
            Heartbeats.Add((workerId, activeTasks, processedCount));
            return ValueTask.CompletedTask;
        }

        public ValueTask UnregisterWorkerAsync(
            string workerId,
            CancellationToken cancellationToken = default
        )
        {
            if (FailUnregister)
                throw new IOException("registry unavailable");
            Unregistered.Add(workerId);
            return ValueTask.CompletedTask;
        }

        public async IAsyncEnumerable<WorkerInfo> GetActiveWorkersAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default
        )
        {
            await Task.CompletedTask;
            yield break;
        }

        public ValueTask<WorkerInfo?> GetWorkerAsync(
            string workerId,
            CancellationToken cancellationToken = default
        ) => ValueTask.FromResult<WorkerInfo?>(null);

        public ValueTask<int> CleanupStaleWorkersAsync(
            TimeSpan timeout,
            CancellationToken cancellationToken = default
        ) => ValueTask.FromResult(0);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
