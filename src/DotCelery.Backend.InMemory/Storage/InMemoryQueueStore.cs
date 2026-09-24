using DotCelery.Core.Storage;

namespace DotCelery.Backend.InMemory.Storage;

/// <summary>
/// In-memory <see cref="IQueueStore"/>.
/// </summary>
internal sealed class InMemoryQueueStore : IQueueStore
{
    private readonly Dictionary<string, Dictionary<string, Item>> _queues = new(
        StringComparer.Ordinal
    );
    private readonly Lock _lock = new();
    private readonly TimeProvider _timeProvider;

    public InMemoryQueueStore(TimeProvider timeProvider)
    {
        _timeProvider = timeProvider;
    }

    public ValueTask EnqueueAsync(
        string queue,
        string id,
        ReadOnlyMemory<byte> payload,
        DateTimeOffset dueAt,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(queue);
        StorageGuard.ThrowIfInvalidKey(id);

        lock (_lock)
        {
            if (!_queues.TryGetValue(queue, out var items))
            {
                items = new Dictionary<string, Item>(StringComparer.Ordinal);
                _queues[queue] = items;
            }

            items[id] = new Item { Payload = payload.ToArray(), DueAt = dueAt };
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<QueueItem>> ClaimAsync(
        string queue,
        int maxItems,
        TimeSpan visibilityTimeout,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(queue);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxItems, 1);
        StorageGuard.ThrowIfNotPositive(visibilityTimeout);

        lock (_lock)
        {
            if (!_queues.TryGetValue(queue, out var items))
            {
                return ValueTask.FromResult<IReadOnlyList<QueueItem>>([]);
            }

            var now = _timeProvider.GetUtcNow();
            var claimable = items
                .Where(i => i.Value.DueAt <= now && !i.Value.IsClaimed(now))
                .OrderBy(i => i.Value.DueAt)
                .ThenBy(i => i.Key, StringComparer.Ordinal)
                .Take(maxItems)
                .ToList();

            var claimed = new List<QueueItem>(claimable.Count);
            foreach (var (id, item) in claimable)
            {
                item.ClaimToken = Guid.NewGuid();
                item.ClaimedUntil = now + visibilityTimeout;
                item.DeliveryCount++;

                claimed.Add(
                    new QueueItem(
                        queue,
                        id,
                        item.Payload,
                        item.DueAt,
                        item.DeliveryCount,
                        item.ClaimToken.Value,
                        item.ClaimedUntil
                    )
                );
            }

            return ValueTask.FromResult<IReadOnlyList<QueueItem>>(claimed);
        }
    }

    public ValueTask<bool> CompleteAsync(
        QueueItem item,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(item);

        lock (_lock)
        {
            if (!TryGetClaimed(item, out var items))
            {
                return ValueTask.FromResult(false);
            }

            items.Remove(item.Id);
            return ValueTask.FromResult(true);
        }
    }

    public ValueTask<bool> AbandonAsync(
        QueueItem item,
        DateTimeOffset? dueAt = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(item);

        lock (_lock)
        {
            if (!TryGetClaimed(item, out var items))
            {
                return ValueTask.FromResult(false);
            }

            var stored = items[item.Id];
            stored.ClaimToken = null;
            stored.DueAt = dueAt ?? stored.DueAt;
            return ValueTask.FromResult(true);
        }
    }

    public ValueTask<bool> RemoveAsync(
        string queue,
        string id,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(queue);
        StorageGuard.ThrowIfInvalidKey(id);

        lock (_lock)
        {
            return ValueTask.FromResult(
                _queues.TryGetValue(queue, out var items) && items.Remove(id)
            );
        }
    }

    public ValueTask<long> CountAsync(string queue, CancellationToken cancellationToken = default)
    {
        StorageGuard.ThrowIfInvalidName(queue);

        lock (_lock)
        {
            return ValueTask.FromResult(
                _queues.TryGetValue(queue, out var items) ? (long)items.Count : 0L
            );
        }
    }

    public ValueTask<DateTimeOffset?> GetNextDueAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(queue);

        lock (_lock)
        {
            if (!_queues.TryGetValue(queue, out var items))
            {
                return ValueTask.FromResult<DateTimeOffset?>(null);
            }

            var now = _timeProvider.GetUtcNow();
            var waiting = items.Values.Where(i => !i.IsClaimed(now)).ToList();

            return ValueTask.FromResult<DateTimeOffset?>(
                waiting.Count == 0 ? null : waiting.Min(i => i.DueAt)
            );
        }
    }

    private bool TryGetClaimed(QueueItem item, out Dictionary<string, Item> items)
    {
        return _queues.TryGetValue(item.Queue, out items!)
            && items.TryGetValue(item.Id, out var stored)
            && stored.ClaimToken == item.ClaimToken;
    }

    private sealed class Item
    {
        public required byte[] Payload { get; init; }

        public DateTimeOffset DueAt { get; set; }

        public int DeliveryCount { get; set; }

        public Guid? ClaimToken { get; set; }

        public DateTimeOffset ClaimedUntil { get; set; }

        public bool IsClaimed(DateTimeOffset now) => ClaimToken is not null && ClaimedUntil > now;
    }
}
