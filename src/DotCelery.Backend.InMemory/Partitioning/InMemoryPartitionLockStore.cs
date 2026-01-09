using System.Collections.Concurrent;
using DotCelery.Core.Partitioning;

namespace DotCelery.Backend.InMemory.Partitioning;

/// <summary>
/// In-memory implementation of <see cref="IPartitionLockStore"/> for testing.
/// </summary>
public sealed class InMemoryPartitionLockStore : IPartitionLockStore
{
    private readonly ConcurrentDictionary<string, PartitionLock> _locks = new();
    private readonly Timer _cleanupTimer;

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryPartitionLockStore"/> class.
    /// </summary>
    public InMemoryPartitionLockStore()
    {
        // Clean up expired locks every 10 seconds
        _cleanupTimer = new Timer(
            CleanupExpiredLocks,
            null,
            TimeSpan.FromSeconds(10),
            TimeSpan.FromSeconds(10)
        );
    }

    /// <inheritdoc />
    public ValueTask<bool> TryAcquireAsync(
        string partitionKey,
        string taskId,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    )
    {
        var now = DateTimeOffset.UtcNow;
        var expiresAt = now.Add(timeout);

        var newLock = new PartitionLock(taskId, expiresAt);

        // Try to add new lock
        if (_locks.TryAdd(partitionKey, newLock))
        {
            return ValueTask.FromResult(true);
        }

        // Check if existing lock is held by the same task or is expired
        if (_locks.TryGetValue(partitionKey, out var existingLock))
        {
            // Same task already holds the lock - extend it (idempotent behavior)
            if (existingLock.TaskId == taskId)
            {
                _locks.TryUpdate(partitionKey, newLock, existingLock);
                return ValueTask.FromResult(true);
            }

            // Check if existing lock is expired
            if (existingLock.ExpiresAt < now)
            {
                // Try to replace expired lock
                if (_locks.TryUpdate(partitionKey, newLock, existingLock))
                {
                    return ValueTask.FromResult(true);
                }
            }
        }

        return ValueTask.FromResult(false);
    }

    /// <inheritdoc />
    public ValueTask<bool> ReleaseAsync(
        string partitionKey,
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        if (_locks.TryGetValue(partitionKey, out var existingLock))
        {
            if (existingLock.TaskId == taskId)
            {
                return ValueTask.FromResult(_locks.TryRemove(partitionKey, out _));
            }
        }

        return ValueTask.FromResult(false);
    }

    /// <inheritdoc />
    public ValueTask<bool> IsLockedAsync(
        string partitionKey,
        CancellationToken cancellationToken = default
    )
    {
        if (_locks.TryGetValue(partitionKey, out var existingLock))
        {
            return ValueTask.FromResult(existingLock.ExpiresAt > DateTimeOffset.UtcNow);
        }

        return ValueTask.FromResult(false);
    }

    /// <inheritdoc />
    public ValueTask<string?> GetLockHolderAsync(
        string partitionKey,
        CancellationToken cancellationToken = default
    )
    {
        if (_locks.TryGetValue(partitionKey, out var existingLock))
        {
            if (existingLock.ExpiresAt > DateTimeOffset.UtcNow)
            {
                return ValueTask.FromResult<string?>(existingLock.TaskId);
            }
        }

        return ValueTask.FromResult<string?>(null);
    }

    /// <inheritdoc />
    public ValueTask<bool> ExtendAsync(
        string partitionKey,
        string taskId,
        TimeSpan extension,
        CancellationToken cancellationToken = default
    )
    {
        if (_locks.TryGetValue(partitionKey, out var existingLock))
        {
            if (existingLock.TaskId == taskId)
            {
                var newLock = existingLock with
                {
                    ExpiresAt = DateTimeOffset.UtcNow.Add(extension),
                };

                return ValueTask.FromResult(_locks.TryUpdate(partitionKey, newLock, existingLock));
            }
        }

        return ValueTask.FromResult(false);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _cleanupTimer.Dispose();
        _locks.Clear();
        return ValueTask.CompletedTask;
    }

    private void CleanupExpiredLocks(object? state)
    {
        var now = DateTimeOffset.UtcNow;
        var expiredKeys = _locks
            .Where(kvp => kvp.Value.ExpiresAt < now)
            .Select(kvp => kvp.Key)
            .ToList();

        foreach (var key in expiredKeys)
        {
            _locks.TryRemove(key, out _);
        }
    }

    private sealed record PartitionLock(string TaskId, DateTimeOffset ExpiresAt);
}
