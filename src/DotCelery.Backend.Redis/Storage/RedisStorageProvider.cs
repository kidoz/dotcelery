using DotCelery.Core.Storage;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.Storage;

/// <summary>
/// Redis provider of the storage primitives.
/// </summary>
/// <remarks>
/// Expiry is decided by the given clock, not by Redis key expiry, so expired data stays until
/// it is read or <see cref="PurgeExpiredAsync"/> runs. Each script only touches keys that share
/// one hash tag, so it runs within one cluster slot.
/// </remarks>
public sealed class RedisStorageProvider : IStorageProvider
{
    private readonly RedisContext _context;
    private readonly RedisDocumentStore _documents;
    private readonly RedisLeaseStore _leases;
    private readonly RedisCounterStore _counters;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisStorageProvider"/> class.
    /// </summary>
    /// <param name="connections">The shared connections.</param>
    /// <param name="options">The storage options.</param>
    /// <param name="timeProvider">The clock for expiry, due times, and lease deadlines.</param>
    public RedisStorageProvider(
        IRedisConnectionProvider connections,
        RedisStorageOptions options,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(connections);
        ArgumentNullException.ThrowIfNull(options);

        _context = new RedisContext(connections, options, timeProvider ?? TimeProvider.System);
        _documents = new RedisDocumentStore(_context);
        _leases = new RedisLeaseStore(_context);
        _counters = new RedisCounterStore(_context);
        Queues = new RedisQueueStore(_context);
        Notifications = new RedisNotificationChannel(_context);
    }

    /// <inheritdoc />
    public string Name => "Redis";

    /// <inheritdoc />
    public IDocumentStore Documents => _documents;

    /// <inheritdoc />
    public ILeaseStore Leases => _leases;

    /// <inheritdoc />
    public IQueueStore Queues { get; }

    /// <inheritdoc />
    public ICounterStore Counters => _counters;

    /// <inheritdoc />
    public INotificationChannel? Notifications { get; }

    /// <inheritdoc />
    public async ValueTask<long> PurgeExpiredAsync(CancellationToken cancellationToken = default)
    {
        var database = await _context.GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        long purged = 0;

        foreach (
            var collection in await database
                .SetMembersAsync(_context.Keys.CollectionRegistry)
                .ConfigureAwait(false)
        )
        {
            purged += await _documents
                .PurgeExpiredAsync(collection.ToString(), cancellationToken)
                .ConfigureAwait(false);
        }

        purged += await _leases.PurgeExpiredAsync(cancellationToken).ConfigureAwait(false);
        purged += await _counters.PurgeExpiredAsync(cancellationToken).ConfigureAwait(false);
        return purged;
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsHealthyAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var database = await _context.GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
            await database.PingAsync().ConfigureAwait(false);
            return true;
        }
        catch (Exception ex) when (ex is RedisException or TimeoutException)
        {
            return false;
        }
    }
}
