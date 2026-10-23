using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using DotCelery.Core.MultiTenancy;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// Queue items a store has claimed for its consumer and not yet settled, by item ID.
/// </summary>
internal sealed class HeldClaims
{
    private readonly ConcurrentDictionary<string, QueueItem> _items = new(StringComparer.Ordinal);

    public void Add(QueueItem item) => _items[Key(item.Id)] = item;

    public bool TryTake(string id, [NotNullWhen(true)] out QueueItem? item) =>
        _items.TryRemove(Key(id), out item);

    private static string Key(string id)
    {
        var tenant = TenantContext.Current.TenantId;
        return tenant is null ? id : $"{tenant.Length}:{tenant}:{id}";
    }

    // A consumer that never settles its items, for example because it failed, would otherwise
    // leave them here after the queue has delivered them again
    public void RemoveExpired(DateTimeOffset now)
    {
        foreach (var entry in _items)
        {
            if (entry.Value.ClaimedUntil <= now)
            {
                _items.TryRemove(entry);
            }
        }
    }
}
