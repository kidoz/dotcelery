using System.Collections.Concurrent;
using DotCelery.Core.Execution;

namespace DotCelery.Backend.InMemory.Execution;

/// <summary>
/// In-memory implementation of <see cref="ITaskExecutionTracker"/> for testing.
/// </summary>
public sealed class InMemoryTaskExecutionTracker : ITaskExecutionTracker
{
    private readonly ConcurrentDictionary<string, ExecutionRecord> _executions = new();
    private readonly Timer _cleanupTimer;
    private readonly TimeSpan _defaultTimeout = TimeSpan.FromHours(1);

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryTaskExecutionTracker"/> class.
    /// </summary>
    public InMemoryTaskExecutionTracker()
    {
        _cleanupTimer = new Timer(
            CleanupExpired,
            null,
            TimeSpan.FromSeconds(30),
            TimeSpan.FromSeconds(30)
        );
    }

    /// <inheritdoc />
    public ValueTask<bool> TryStartAsync(
        string taskName,
        string taskId,
        string? key = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default
    )
    {
        var lookupKey = GetLookupKey(taskName, key);
        var now = DateTimeOffset.UtcNow;
        var expiresAt = now.Add(timeout ?? _defaultTimeout);

        var newRecord = new ExecutionRecord(taskId, key, now, expiresAt);

        // Try to add new execution
        if (_executions.TryAdd(lookupKey, newRecord))
        {
            return ValueTask.FromResult(true);
        }

        // Check if existing is expired
        if (_executions.TryGetValue(lookupKey, out var existing))
        {
            if (existing.ExpiresAt < now)
            {
                // Replace expired record
                if (_executions.TryUpdate(lookupKey, newRecord, existing))
                {
                    return ValueTask.FromResult(true);
                }
            }
        }

        return ValueTask.FromResult(false);
    }

    /// <inheritdoc />
    public ValueTask StopAsync(
        string taskName,
        string taskId,
        string? key = null,
        CancellationToken cancellationToken = default
    )
    {
        var lookupKey = GetLookupKey(taskName, key);

        if (_executions.TryGetValue(lookupKey, out var existing))
        {
            if (existing.TaskId == taskId)
            {
                _executions.TryRemove(lookupKey, out _);
            }
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<bool> IsExecutingAsync(
        string taskName,
        string? key = null,
        CancellationToken cancellationToken = default
    )
    {
        var lookupKey = GetLookupKey(taskName, key);

        if (_executions.TryGetValue(lookupKey, out var existing))
        {
            return ValueTask.FromResult(existing.ExpiresAt > DateTimeOffset.UtcNow);
        }

        return ValueTask.FromResult(false);
    }

    /// <inheritdoc />
    public ValueTask<string?> GetExecutingTaskIdAsync(
        string taskName,
        string? key = null,
        CancellationToken cancellationToken = default
    )
    {
        var lookupKey = GetLookupKey(taskName, key);

        if (_executions.TryGetValue(lookupKey, out var existing))
        {
            if (existing.ExpiresAt > DateTimeOffset.UtcNow)
            {
                return ValueTask.FromResult<string?>(existing.TaskId);
            }
        }

        return ValueTask.FromResult<string?>(null);
    }

    /// <inheritdoc />
    public ValueTask<bool> ExtendAsync(
        string taskName,
        string taskId,
        string? key = null,
        TimeSpan? extension = null,
        CancellationToken cancellationToken = default
    )
    {
        var lookupKey = GetLookupKey(taskName, key);

        if (_executions.TryGetValue(lookupKey, out var existing))
        {
            if (existing.TaskId == taskId)
            {
                var newRecord = existing with
                {
                    ExpiresAt = DateTimeOffset.UtcNow.Add(extension ?? _defaultTimeout),
                };

                return ValueTask.FromResult(_executions.TryUpdate(lookupKey, newRecord, existing));
            }
        }

        return ValueTask.FromResult(false);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyDictionary<string, ExecutingTaskInfo>> GetAllExecutingAsync(
        CancellationToken cancellationToken = default
    )
    {
        var now = DateTimeOffset.UtcNow;
        var result = _executions
            .Where(kvp => kvp.Value.ExpiresAt > now)
            .ToDictionary(
                kvp => kvp.Key,
                kvp => new ExecutingTaskInfo
                {
                    TaskId = kvp.Value.TaskId,
                    Key = kvp.Value.Key,
                    StartedAt = kvp.Value.StartedAt,
                    ExpiresAt = kvp.Value.ExpiresAt,
                }
            );

        return ValueTask.FromResult<IReadOnlyDictionary<string, ExecutingTaskInfo>>(result);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _cleanupTimer.Dispose();
        _executions.Clear();
        return ValueTask.CompletedTask;
    }

    private static string GetLookupKey(string taskName, string? key)
    {
        return key is null ? taskName : $"{taskName}:{key}";
    }

    private void CleanupExpired(object? state)
    {
        var now = DateTimeOffset.UtcNow;
        var expiredKeys = _executions
            .Where(kvp => kvp.Value.ExpiresAt < now)
            .Select(kvp => kvp.Key)
            .ToList();

        foreach (var key in expiredKeys)
        {
            _executions.TryRemove(key, out _);
        }
    }

    private sealed record ExecutionRecord(
        string TaskId,
        string? Key,
        DateTimeOffset StartedAt,
        DateTimeOffset ExpiresAt
    );
}
