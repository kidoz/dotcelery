using System.Collections.Concurrent;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
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
    private readonly WorkerOptions _options;
    private readonly ILogger<RevocationManager> _logger;
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _runningTasks = new();
    private readonly ConcurrentDictionary<string, RevokeOptions> _pendingRevocations = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="RevocationManager"/> class.
    /// </summary>
    public RevocationManager(
        IOptions<WorkerOptions> options,
        ILogger<RevocationManager> logger,
        IRevocationStore? revocationStore = null
    )
    {
        _revocationStore = revocationStore;
        _options = options.Value;
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
        _runningTasks[taskId] = cts;

        // Check if there's a pending revocation for this task
        if (_pendingRevocations.TryRemove(taskId, out var options))
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
        if (_runningTasks.TryRemove(taskId, out var cts))
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
        return _pendingRevocations.GetValueOrDefault(taskId);
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_revocationStore is null)
        {
            _logger.LogInformation("Revocation manager disabled - no revocation store configured");
            return;
        }

        _logger.LogInformation("Starting revocation manager");

        // Load existing revocations at startup
        await LoadExistingRevocationsAsync(stoppingToken).ConfigureAwait(false);

        // Subscribe to new revocations
        try
        {
            await foreach (
                var evt in _revocationStore.SubscribeAsync(stoppingToken).ConfigureAwait(false)
            )
            {
                HandleRevocationEvent(evt);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Expected during shutdown
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in revocation subscription");
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
            catch (ObjectDisposedException)
            {
                // Already disposed
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
                var taskId in _revocationStore
                    .GetRevokedTaskIdsAsync(cancellationToken)
                    .ConfigureAwait(false)
            )
            {
                // Store as pending - will be applied when task starts
                _pendingRevocations.TryAdd(taskId, RevokeOptions.Default);
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
        }
    }

    private void HandleRevocationEvent(RevocationEvent evt)
    {
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

        // Store for future reference (in case task hasn't started yet)
        _pendingRevocations[evt.TaskId] = evt.Options;
    }

    private void CancelTask(string taskId, RevokeOptions options)
    {
        if (!_runningTasks.TryGetValue(taskId, out var cts))
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
                _ = cts.CancelAsync();
                _logger.LogInformation("Task {TaskId} cancellation requested (graceful)", taskId);
            }
        }
        catch (ObjectDisposedException)
        {
            // Task already completed
        }
    }
}
