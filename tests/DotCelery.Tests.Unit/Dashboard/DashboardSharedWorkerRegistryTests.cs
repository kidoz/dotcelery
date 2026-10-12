using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Dashboard;
using DotCelery.Core.Storage.Stores;
using DotCelery.Dashboard.Services;
using DotCelery.Worker;
using DotCelery.Worker.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Unit.Dashboard;

/// <summary>
/// A worker and a dashboard in different hosts share one registry through storage, so the
/// dashboard sees the workers of other processes and the queues they consume.
/// </summary>
public sealed class DashboardSharedWorkerRegistryTests
{
    [Fact]
    public async Task AWorkerInAnotherHost_ShowsUpAsAConsumerOfItsQueues()
    {
        // The storage both hosts are configured with
        var storage = new InMemoryStorageProvider();
        var metrics = new QueueMetrics(storage);
        await metrics.RecordEnqueuedAsync("celery");

        // One host runs the worker, which registers itself and heartbeats
        var heartbeat = new WorkerHeartbeatService(
            Options.Create(new WorkerOptions { WorkerName = "worker-a", Queues = ["celery"] }),
            NullLogger<WorkerHeartbeatService>.Instance,
            new WorkerActivity(),
            new WorkerRegistry(storage)
        );
        await heartbeat.StartAsync(CancellationToken.None);

        try
        {
            var registry = new WorkerRegistry(storage);
            await WaitForWorkerAsync(registry);
            Assert.Equal("worker-a", (await registry.GetWorkerAsync("worker-a"))!.WorkerId);

            // The other host runs the dashboard, whose queue panel reports the consumer
            var provider = new DashboardQueueStatsProvider(metrics, registry);
            var stats = await provider.GetQueueStatsAsync("celery");

            Assert.NotNull(stats);
            Assert.Equal(1, stats.ConsumerCount);
            Assert.Equal(1, stats.MessageCount);
        }
        finally
        {
            await heartbeat.StopAsync(CancellationToken.None);
            heartbeat.Dispose();
        }

        // The worker's record is gone once it stops
        Assert.Null(await new WorkerRegistry(storage).GetWorkerAsync("worker-a"));
    }

    private static async Task WaitForWorkerAsync(WorkerRegistry registry)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        while (await registry.GetWorkerAsync("worker-a") is null)
        {
            timeout.Token.ThrowIfCancellationRequested();
            await Task.Delay(10, timeout.Token);
        }
    }
}
