using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using DotCelery.Core.Dashboard;
using DotCelery.Core.MultiTenancy;

namespace DotCelery.Dashboard.Services;

/// <summary>
/// In-memory implementation of worker registry.
/// Suitable for single-instance deployments.
/// </summary>
public sealed class InMemoryWorkerRegistry : IWorkerRegistry
{
    private readonly ConcurrentDictionary<string, WorkerInfo> _workers = new();
    private readonly TimeProvider _timeProvider;
    private bool _disposed;

    private static string Prefix =>
        TenantContext.Current.TenantId is { } tenant ? $"{tenant.Length}:{tenant}:" : "global:";

    private static string Key(string workerId) => Prefix + workerId;

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryWorkerRegistry"/> class.
    /// </summary>
    /// <param name="timeProvider">Optional time provider for testing.</param>
    public InMemoryWorkerRegistry(TimeProvider? timeProvider = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public ValueTask RegisterWorkerAsync(
        WorkerInfo worker,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(worker);

        _workers[Key(worker.WorkerId)] = worker;
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask HeartbeatAsync(
        string workerId,
        int activeTasks = 0,
        long processedCount = 0,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(workerId);

        if (_workers.TryGetValue(Key(workerId), out var existing))
        {
            var updated = existing with
            {
                LastHeartbeat = _timeProvider.GetUtcNow(),
                ActiveTasks = activeTasks,
                ProcessedCount = processedCount,
                Status = WorkerStatus.Online,
            };

            _workers[Key(workerId)] = updated;
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask UnregisterWorkerAsync(
        string workerId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(workerId);

        _workers.TryRemove(Key(workerId), out _);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<WorkerInfo> GetActiveWorkersAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        foreach (
            var worker in _workers
                .Where(pair => pair.Key.StartsWith(Prefix, StringComparison.Ordinal))
                .Select(pair => pair.Value)
        )
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return worker;
            await Task.Yield();
        }
    }

    /// <inheritdoc />
    public ValueTask<WorkerInfo?> GetWorkerAsync(
        string workerId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(workerId);

        _workers.TryGetValue(Key(workerId), out var worker);
        return ValueTask.FromResult(worker);
    }

    /// <inheritdoc />
    public ValueTask<int> CleanupStaleWorkersAsync(
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var now = _timeProvider.GetUtcNow();
        var cutoff = now - timeout;
        var removed = 0;

        foreach (var kvp in _workers)
        {
            if (!kvp.Key.StartsWith(Prefix, StringComparison.Ordinal))
                continue;
            cancellationToken.ThrowIfCancellationRequested();

            if (kvp.Value.LastHeartbeat < cutoff)
            {
                if (kvp.Value.Status == WorkerStatus.Unresponsive)
                {
                    // Already unresponsive, remove
                    if (_workers.TryRemove(kvp.Key, out _))
                    {
                        removed++;
                    }
                }
                else
                {
                    // Mark as unresponsive
                    var updated = kvp.Value with
                    {
                        Status = WorkerStatus.Unresponsive,
                    };
                    _workers[kvp.Key] = updated;
                }
            }
        }

        return ValueTask.FromResult(removed);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return ValueTask.CompletedTask;
        }

        _disposed = true;
        _workers.Clear();
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Gets the current worker count. For testing purposes.
    /// </summary>
    public int Count =>
        _workers.Keys.Count(key => key.StartsWith(Prefix, StringComparison.Ordinal));
}
