using System.Collections.Concurrent;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.MultiTenancy;
using DotCelery.Core.Storage.Stores;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Services;

/// <summary>
/// Manages task revocation by maintaining CancellationTokenSources for running tasks
/// and subscribing to revocation events for real-time cancellation.
/// </summary>
public sealed class RevocationManager : BackgroundService
{
    private readonly IRevocationStore? _revocationStore;
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _retention;
    private readonly ILogger<RevocationManager> _logger;
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _runningTasks = new();
    private readonly ConcurrentDictionary<string, RevocationEvent> _pendingRevocations = new();

    private string?[] _tenants = [null];

    internal void SetTenants(IEnumerable<string?> tenants) => _tenants = tenants.ToArray();

    private static string TaskKey(string taskId) =>
        TenantContext.Current.TenantId is { } tenant
            ? $"{tenant.Length}:{tenant}:{taskId}"
            : taskId;

    /// <summary>
    /// Initializes a new instance of the <see cref="RevocationManager"/> class.
    /// </summary>
    public RevocationManager(
        IOptions<WorkerOptions> options,
        ILogger<RevocationManager> logger,
        IRevocationStore? revocationStore = null
    )
        : this(
            options,
            logger,
            revocationStore,
            TimeProvider.System,
            Options.Create(new StorageStoreOptions())
        ) { }

    /// <summary>
    /// Initializes a revocation manager with the storage retention policy and clock.
    /// </summary>
    public RevocationManager(
        IOptions<WorkerOptions> options,
        ILogger<RevocationManager> logger,
        IRevocationStore? revocationStore,
        TimeProvider timeProvider,
        IOptions<StorageStoreOptions> storageOptions
    )
    {
        ArgumentNullException.ThrowIfNull(options);
        _revocationStore = revocationStore;
        _timeProvider = timeProvider;
        _retention = storageOptions.Value.RevocationRetention;
        _logger = logger;
    }

    /// <summary>
    /// Registers a task as running and returns a linked CancellationToken.
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <param name="parentToken">The parent cancellation token.</param>
    /// <returns>A linked CancellationTokenSource that can be cancelled on revocation.</returns>
    public CancellationTokenSource RegisterTask(string taskId, CancellationToken parentToken)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(parentToken);
        if (!_runningTasks.TryAdd(TaskKey(taskId), cts))
        {
            cts.Dispose();
            throw new InvalidOperationException($"Task {taskId} is already registered as running.");
        }

        // Check if there's a pending revocation for this task
        if (GetRevocationOptions(taskId) is { } options)
        {
            _logger.LogDebug("Found pending revocation for task {TaskId}", taskId);

            // Only cancel running task if Terminate is true
            // If Terminate is false, revocation only prevents pending execution (handled elsewhere)
            if (options.Terminate)
            {
                CancelTask(taskId, options);
            }
        }

        return cts;
    }

    /// <summary>
    /// Unregisters a task when it completes (success, failure, or cancellation).
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    public void UnregisterTask(string taskId)
    {
        if (_runningTasks.TryRemove(TaskKey(taskId), out var cts))
        {
            cts.Dispose();
        }
    }

    /// <summary>
    /// Checks if a task has been revoked.
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the task is revoked.</returns>
    public async ValueTask<bool> IsRevokedAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        if (_revocationStore is null)
        {
            return false;
        }

        return await _revocationStore
            .IsRevokedAsync(taskId, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Gets the revocation options for a task if it was revoked.
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <returns>The revoke options if the task was cancelled via revocation, null otherwise.</returns>
    public RevokeOptions? GetRevocationOptions(string taskId)
    {
        if (!_pendingRevocations.TryGetValue(TaskKey(taskId), out var revocation))
        {
            return null;
        }
        if (IsExpired(revocation))
        {
            ((ICollection<KeyValuePair<string, RevocationEvent>>)_pendingRevocations).Remove(
                new(TaskKey(taskId), revocation)
            );
            return null;
        }
        return revocation.Options;
    }

    /// <inheritdoc />
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.WhenAll(_tenants.Select(tenant => RunTenantAsync(tenant, stoppingToken)));

    private async Task RunTenantAsync(string? tenant, CancellationToken stoppingToken)
    {
        using var tenantScope = TenantContext.SetTenant(tenant);
        if (_revocationStore is null)
        {
            _logger.LogInformation("Revocation manager disabled - no revocation store configured");
            return;
        }

        _logger.LogInformation("Starting revocation manager");

        var pruning = PruneExpiredAsync(stoppingToken);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // Reconcile after every disconnect so revocations missed during an outage
                    // still reach tasks that are running.
                    await LoadExistingRevocationsAsync(stoppingToken).ConfigureAwait(false);
                    await foreach (
                        var evt in _revocationStore
                            .SubscribeAsync(stoppingToken)
                            .ConfigureAwait(false)
                    )
                    {
                        HandleRevocationEvent(evt);
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Revocation subscription failed; reconnecting");
                }
                await Task.Delay(TimeSpan.FromSeconds(1), _timeProvider, stoppingToken)
                    .ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Expected during shutdown.
        }
        finally
        {
            await pruning.ConfigureAwait(false);
        }

        _logger.LogInformation("Revocation manager stopped");
    }

    /// <inheritdoc />
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Cancel all running tasks on shutdown
        foreach (var kvp in _runningTasks)
        {
            try
            {
                await kvp.Value.CancelAsync();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Unable to cancel task {TaskId} during shutdown", kvp.Key);
            }
        }

        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task LoadExistingRevocationsAsync(CancellationToken cancellationToken)
    {
        if (_revocationStore is null)
        {
            return;
        }

        try
        {
            var count = 0;
            await foreach (
                var revocation in _revocationStore
                    .GetRevocationsAsync(cancellationToken)
                    .ConfigureAwait(false)
            )
            {
                // Store as pending - will be applied when task starts
                HandleRevocationEvent(revocation);
                count++;
            }

            if (count > 0)
            {
                _logger.LogInformation("Loaded {Count} existing revocations", count);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to load existing revocations");
            throw; // Retry reconciliation before subscribing, so missed revocations are not skipped.
        }
    }

    private void HandleRevocationEvent(RevocationEvent evt)
    {
        if (IsExpired(evt))
        {
            return;
        }
        _pendingRevocations[TaskKey(evt.TaskId)] = evt;
        _logger.LogDebug(
            "Received revocation event for task {TaskId}, terminate={Terminate}",
            evt.TaskId,
            evt.Options.Terminate
        );

        if (evt.Options.Terminate)
        {
            // Cancel running task if exists
            CancelTask(evt.TaskId, evt.Options);
        }
    }

    private void CancelTask(string taskId, RevokeOptions options)
    {
        if (!_runningTasks.TryGetValue(TaskKey(taskId), out var cts))
        {
            return;
        }

        try
        {
            if (options.Signal == CancellationSignal.Immediate)
            {
                // Cancel immediately - runs cancellation callbacks synchronously
                cts.Cancel();
                _logger.LogInformation("Task {TaskId} cancelled immediately", taskId);
            }
            else
            {
                // Graceful cancellation - schedule callbacks asynchronously to allow
                // current operation to complete its synchronous portion
                _ = CancelGracefullyAsync(cts, taskId);
                _logger.LogInformation("Task {TaskId} cancellation requested (graceful)", taskId);
            }
        }
        catch (ObjectDisposedException)
        {
            // Task already completed
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Cancellation callback failed for task {TaskId}", taskId);
        }
    }

    private async Task CancelGracefullyAsync(CancellationTokenSource cts, string taskId)
    {
        try
        {
            await cts.CancelAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Cancellation callback failed for task {TaskId}", taskId);
        }
    }

    private bool IsExpired(RevocationEvent evt) =>
        _timeProvider.GetUtcNow() - evt.Timestamp >= (evt.Options.Expiry ?? _retention);

    private async Task PruneExpiredAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMinutes(1), _timeProvider);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                foreach (var entry in _pendingRevocations)
                {
                    if (IsExpired(entry.Value))
                        (
                            (ICollection<KeyValuePair<string, RevocationEvent>>)_pendingRevocations
                        ).Remove(entry);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Expected during shutdown.
        }
    }
}
