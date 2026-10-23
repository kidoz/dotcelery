using DotCelery.Core.Dashboard;
using DotCelery.Core.MultiTenancy;
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
    private readonly ITenantRouter? _tenantRouter;
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
        : this(null, options, logger, activity, registry, timeProvider) { }

    /// <summary>Initializes tenant-aware worker registration with an authenticated queue router.</summary>
    public WorkerHeartbeatService(
        ITenantRouter? tenantRouter,
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
        _tenantRouter = tenantRouter;
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

        var registered = false;
        try
        {
            using var timer = new PeriodicTimer(_options.WorkerHeartbeatInterval, _timeProvider);
            do
            {
                try
                {
                    if (!registered)
                    {
                        await _registry
                            .RegisterWorkerAsync(
                                new WorkerInfo
                                {
                                    WorkerId = workerId,
                                    Hostname = Environment.MachineName,
                                    ProcessId = Environment.ProcessId,
                                    Queues =
                                        TenantContext.Current.TenantId is { } tenant
                                        && _tenantRouter
                                            ?.GetTenantQueues()
                                            .TryGetValue(tenant, out var queue) == true
                                            ? [queue]
                                            : _options.Queues,
                                    Concurrency = _options.Concurrency,
                                    StartedAt = startedAt,
                                    LastHeartbeat = _timeProvider.GetUtcNow(),
                                    Status = WorkerStatus.Online,
                                },
                                stoppingToken
                            )
                            .ConfigureAwait(false);
                        registered = true;
                    }
                    else
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
                catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
                {
                    // The registry entry may have expired during the outage.
                    registered = false;
                    _logger.LogWarning(
                        ex,
                        "Failed to heartbeat worker {WorkerId}; retrying",
                        workerId
                    );
                }
            } while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Expected during shutdown
        }
        finally
        {
            try
            {
                await _registry
                    .UnregisterWorkerAsync(workerId, CancellationToken.None)
                    .ConfigureAwait(false);
                _logger.LogInformation(
                    "Worker {WorkerId} unregistered from the worker registry",
                    workerId
                );
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to unregister worker {WorkerId}; its registration will expire",
                    workerId
                );
            }
        }
    }
}
