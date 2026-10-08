using DotCelery.Core.Dashboard;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Services;

/// <summary>
/// Registers the worker with an <see cref="IWorkerRegistry"/> and sends it a heartbeat, so a
/// dashboard shows the worker as online with its live active and processed task counts.
/// </summary>
/// <remarks>
/// The service does nothing when no registry is registered, so a worker without a dashboard
/// does not carry the background loop.
/// </remarks>
public sealed class WorkerHeartbeatService : BackgroundService
{
    private readonly IWorkerRegistry? _registry;
    private readonly WorkerActivity _activity;
    private readonly WorkerOptions _options;
    private readonly ILogger<WorkerHeartbeatService> _logger;
    private readonly TimeProvider _timeProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="WorkerHeartbeatService"/> class.
    /// </summary>
    public WorkerHeartbeatService(
        IOptions<WorkerOptions> options,
        ILogger<WorkerHeartbeatService> logger,
        WorkerActivity activity,
        IWorkerRegistry? registry = null,
        TimeProvider? timeProvider = null
    )
    {
        _options = options.Value;
        _logger = logger;
        _activity = activity;
        _registry = registry;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_registry is null)
        {
            _logger.LogDebug("No worker registry is registered; worker heartbeats are disabled");
            return;
        }

        var workerId = WorkerIdentity.Resolve(_options);
        var startedAt = _timeProvider.GetUtcNow();

        await _registry
            .RegisterWorkerAsync(
                new WorkerInfo
                {
                    WorkerId = workerId,
                    Hostname = Environment.MachineName,
                    ProcessId = Environment.ProcessId,
                    Queues = _options.Queues,
                    Concurrency = _options.Concurrency,
                    StartedAt = startedAt,
                    LastHeartbeat = startedAt,
                    Status = WorkerStatus.Online,
                },
                stoppingToken
            )
            .ConfigureAwait(false);

        _logger.LogInformation("Worker {WorkerId} registered with the worker registry", workerId);

        try
        {
            using var timer = new PeriodicTimer(_options.WorkerHeartbeatInterval, _timeProvider);

            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                await _registry
                    .HeartbeatAsync(
                        workerId,
                        _activity.ActiveTasks,
                        _activity.ProcessedTasks,
                        stoppingToken
                    )
                    .ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Expected during shutdown
        }
        finally
        {
            // Settled without the worker's token: unregistering during shutdown must still
            // reach the registry
            await _registry
                .UnregisterWorkerAsync(workerId, CancellationToken.None)
                .ConfigureAwait(false);
            _logger.LogInformation(
                "Worker {WorkerId} unregistered from the worker registry",
                workerId
            );
        }
    }
}
