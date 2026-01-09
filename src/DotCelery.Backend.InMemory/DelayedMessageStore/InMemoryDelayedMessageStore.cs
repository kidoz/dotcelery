using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;

namespace DotCelery.Backend.InMemory.DelayedMessageStore;

/// <summary>
/// In-memory delayed message store for testing and development.
/// Uses a sorted dictionary for efficient retrieval of due messages.
/// </summary>
public sealed class InMemoryDelayedMessageStore : IDelayedMessageStore
{
    private readonly SortedDictionary<DateTimeOffset, List<TaskMessage>> _messagesByTime = new();
    private readonly ConcurrentDictionary<string, DateTimeOffset> _taskIdToTime = new();
    private readonly Lock _lock = new();
    private bool _disposed;

    /// <inheritdoc />
    public ValueTask AddAsync(
        TaskMessage message,
        DateTimeOffset deliveryTime,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(message);

        lock (_lock)
        {
            // Remove existing entry if task already scheduled
            if (_taskIdToTime.TryRemove(message.Id, out var existingTime))
            {
                RemoveFromTimeSlot(existingTime, message.Id);
            }

            // Add to sorted dictionary
            if (!_messagesByTime.TryGetValue(deliveryTime, out var messages))
            {
                messages = [];
                _messagesByTime[deliveryTime] = messages;
            }

            messages.Add(message);
            _taskIdToTime[message.Id] = deliveryTime;
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<TaskMessage> GetDueMessagesAsync(
        DateTimeOffset now,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        List<TaskMessage> dueMessages;

        lock (_lock)
        {
            dueMessages = [];
            var timesToRemove = new List<DateTimeOffset>();

            foreach (var kvp in _messagesByTime)
            {
                if (kvp.Key > now)
                {
                    break; // Sorted, so no more due messages
                }

                dueMessages.AddRange(kvp.Value);
                timesToRemove.Add(kvp.Key);
            }

            // Remove collected messages
            foreach (var time in timesToRemove)
            {
                if (_messagesByTime.TryGetValue(time, out var messages))
                {
                    foreach (var msg in messages)
                    {
                        _taskIdToTime.TryRemove(msg.Id, out _);
                    }

                    _messagesByTime.Remove(time);
                }
            }
        }

        // Yield outside the lock
        foreach (var message in dueMessages)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return message;
            await Task.Yield(); // Allow cancellation between messages
        }
    }

    /// <inheritdoc />
    public ValueTask<bool> RemoveAsync(string taskId, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        lock (_lock)
        {
            if (!_taskIdToTime.TryRemove(taskId, out var deliveryTime))
            {
                return ValueTask.FromResult(false);
            }

            RemoveFromTimeSlot(deliveryTime, taskId);
            return ValueTask.FromResult(true);
        }
    }

    /// <inheritdoc />
    public ValueTask<long> GetPendingCountAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        return ValueTask.FromResult((long)_taskIdToTime.Count);
    }

    /// <inheritdoc />
    public ValueTask<DateTimeOffset?> GetNextDeliveryTimeAsync(
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        lock (_lock)
        {
            if (_messagesByTime.Count == 0)
            {
                return ValueTask.FromResult<DateTimeOffset?>(null);
            }

            // SortedDictionary enumeration starts with smallest key
            foreach (var kvp in _messagesByTime)
            {
                return ValueTask.FromResult<DateTimeOffset?>(kvp.Key);
            }

            return ValueTask.FromResult<DateTimeOffset?>(null);
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return ValueTask.CompletedTask;
        }

        _disposed = true;

        lock (_lock)
        {
            _messagesByTime.Clear();
            _taskIdToTime.Clear();
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Clears all delayed messages. For testing purposes.
    /// </summary>
    public void Clear()
    {
        lock (_lock)
        {
            _messagesByTime.Clear();
            _taskIdToTime.Clear();
        }
    }

    /// <summary>
    /// Gets the count of pending messages. For testing purposes.
    /// </summary>
    public int Count => _taskIdToTime.Count;

    private void RemoveFromTimeSlot(DateTimeOffset time, string taskId)
    {
        // Must be called within lock
        if (_messagesByTime.TryGetValue(time, out var messages))
        {
            messages.RemoveAll(m => m.Id == taskId);

            if (messages.Count == 0)
            {
                _messagesByTime.Remove(time);
            }
        }
    }
}
