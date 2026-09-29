namespace DotCelery.Backend.Redis.Storage;

/// <summary>
/// Key names. Each collection, queue, counter, and window, and the set of all leases, has its
/// own hash tag, so every script touches keys in one cluster slot.
/// </summary>
internal sealed class RedisKeys(string prefix)
{
    public string Collection(string collection) => $"{{{prefix}d:{collection}}}";

    public string Leases { get; } = $"{{{prefix}leases}}";

    public string Queue(string queue) => $"{{{prefix}q:{queue}}}";

    public string Counter(string key) => $"{{{prefix}c:{key}}}";

    public string Window(string key) => $"{{{prefix}w:{key}}}";

    public string Channel(string channel) => $"{prefix}n:{channel}";

    // Registries that let a purge find everything that can expire
    public string CollectionRegistry { get; } = $"{prefix}collections";

    public string CounterRegistry { get; } = $"{prefix}counters";

    public string WindowRegistry { get; } = $"{prefix}windows";
}
