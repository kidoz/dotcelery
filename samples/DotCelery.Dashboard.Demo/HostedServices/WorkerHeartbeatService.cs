using System.Diagnostics;
using DotCelery.Core.Dashboard;
using DotCelery.Worker;
using Microsoft.Extensions.Options;

namespace DotCelery.Dashboard.Demo.HostedServices;

/// <summary>
/// Registers the local worker with the dashboard's <see cref="IWorkerRegistry"/>
/// and emits a heartbeat on a fixed interval so it appears on the Workers panel.
/// The worker process itself does not auto-register; the dashboard registry is
/// designed to be populated by whatever orchestrator owns the worker lifecycle.
/// </summary>
public sealed class WorkerHeartbeatService(
    IWorkerRegistry registry,
    IOptions<WorkerOptions> workerOptions
) : BackgroundService
{
    private readonly string _workerId =
        workerOptions.Value.WorkerName
        ?? $"worker-{Environment.MachineName}-{Environment.ProcessId}";

    private long _processed;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = workerOptions.Value;

        await registry.RegisterWorkerAsync(
            new WorkerInfo
            {
                WorkerId = _workerId,
                Hostname = Environment.MachineName,
                ProcessId = Environment.ProcessId,
                Queues = options.Queues,
                Concurrency = options.Concurrency,
                StartedAt = DateTimeOffset.UtcNow,
                LastHeartbeat = DateTimeOffset.UtcNow,
                Status = WorkerStatus.Online,
                Version = typeof(WorkerHeartbeatService).Assembly.GetName().Version?.ToString(),
            },
            stoppingToken
        );

        var ticker = new PeriodicTimer(TimeSpan.FromSeconds(5));
        try
        {
            while (await ticker.WaitForNextTickAsync(stoppingToken))
            {
                // For a real worker, derive these counters from the actual executor.
                // Here we just simulate steady throughput so the panel stays alive.
                Interlocked.Add(ref _processed, Random.Shared.Next(0, 4));

                await registry.HeartbeatAsync(
                    _workerId,
                    activeTasks: Random.Shared.Next(0, options.Concurrency + 1),
                    processedCount: Interlocked.Read(ref _processed),
                    cancellationToken: stoppingToken
                );
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            try
            {
                await registry.UnregisterWorkerAsync(_workerId, cleanup.Token);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Worker unregister failed: {ex.Message}");
            }
        }
    }
}
