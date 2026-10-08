using System.Runtime.CompilerServices;
using DotCelery.Core.Dashboard;
using DotCelery.Dashboard;
using DotCelery.Dashboard.Hubs;
using DotCelery.Dashboard.Services;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;

namespace DotCelery.Tests.Unit.Dashboard;

/// <summary>
/// Runs the dashboard's worker monitor against a registry, so announcements of workers coming
/// and their heartbeat changes are proven, together with the stale-worker cleanup.
/// </summary>
public sealed class DashboardWorkerMonitorTests
{
    private readonly FakeTimeProvider _time = new(
        new DateTimeOffset(2026, 10, 8, 12, 0, 0, TimeSpan.Zero)
    );
    private readonly FakeWorkerRegistry _registry = new();
    private readonly IClientProxy _proxy = Substitute.For<IClientProxy>();

    [Fact]
    public async Task Workers_AreAnnouncedWhenTheyAppearAndChange()
    {
        _registry.Workers["worker-1"] = CreateWorker("worker-1", activeTasks: 0);

        var monitor = CreateMonitor();
        await monitor.StartAsync(CancellationToken.None);

        await WaitForTickAsync(() => _proxy.ReceivedCalls().Any());

        // The same worker with the same heartbeat is not announced again
        var announcements = _proxy.ReceivedCalls().Count();
        var polls = _registry.CleanupCalls;
        await WaitForTickAsync(() => _registry.CleanupCalls > polls);
        Assert.Equal(announcements, _proxy.ReceivedCalls().Count());

        // A changed worker is
        _registry.Workers["worker-1"] = _registry.Workers["worker-1"] with
        {
            ActiveTasks = 2,
        };
        await WaitForTickAsync(() => _proxy.ReceivedCalls().Count() > announcements);
        Assert.True(_registry.CleanupCalls > 0);

        await monitor.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task WithoutARegistry_TheMonitorIsIdle()
    {
        var monitor = new DashboardWorkerMonitor(
            Options.Create(new DashboardOptions()),
            NullLogger<DashboardWorkerMonitor>.Instance,
            registry: null,
            notifications: null,
            _time
        );

        await monitor.StartAsync(CancellationToken.None);
        await monitor.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(5));
    }

    private DashboardWorkerMonitor CreateMonitor()
    {
        var clients = Substitute.For<IHubClients>();
        clients.All.Returns(_proxy);
        var hubContext = Substitute.For<IHubContext<DashboardHub>>();
        hubContext.Clients.Returns(clients);

        return new DashboardWorkerMonitor(
            Options.Create(new DashboardOptions { RefreshIntervalSeconds = 1 }),
            NullLogger<DashboardWorkerMonitor>.Instance,
            _registry,
            new DashboardNotificationService(hubContext),
            _time
        );
    }

    private static WorkerInfo CreateWorker(string workerId, int activeTasks) =>
        new()
        {
            WorkerId = workerId,
            Hostname = "test-host",
            ProcessId = 42,
            Queues = ["celery"],
            ActiveTasks = activeTasks,
            StartedAt = DateTimeOffset.UtcNow,
            LastHeartbeat = new DateTimeOffset(2026, 10, 8, 12, 0, 0, TimeSpan.Zero),
            Status = WorkerStatus.Online,
        };

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
        private int _cleanupCalls;

        public Dictionary<string, WorkerInfo> Workers { get; } = new(StringComparer.Ordinal);

        public int CleanupCalls => _cleanupCalls;

        public ValueTask RegisterWorkerAsync(
            WorkerInfo worker,
            CancellationToken cancellationToken = default
        )
        {
            Workers[worker.WorkerId] = worker;
            return ValueTask.CompletedTask;
        }

        public ValueTask HeartbeatAsync(
            string workerId,
            int activeTasks = 0,
            long processedCount = 0,
            CancellationToken cancellationToken = default
        ) => ValueTask.CompletedTask;

        public ValueTask UnregisterWorkerAsync(
            string workerId,
            CancellationToken cancellationToken = default
        )
        {
            Workers.Remove(workerId);
            return ValueTask.CompletedTask;
        }

        public async IAsyncEnumerable<WorkerInfo> GetActiveWorkersAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default
        )
        {
            foreach (var worker in Workers.Values.ToList())
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return worker;
                await Task.Yield();
            }
        }

        public ValueTask<WorkerInfo?> GetWorkerAsync(
            string workerId,
            CancellationToken cancellationToken = default
        ) => ValueTask.FromResult(Workers.TryGetValue(workerId, out var worker) ? worker : null);

        public ValueTask<int> CleanupStaleWorkersAsync(
            TimeSpan timeout,
            CancellationToken cancellationToken = default
        )
        {
            Interlocked.Increment(ref _cleanupCalls);
            return ValueTask.FromResult(0);
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
