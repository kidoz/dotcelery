using DotCelery.Core.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.Storage;

/// <summary>
/// MongoDB provider of the storage primitives.
/// </summary>
/// <remarks>
/// Expiry is decided by the given clock, not by TTL indexes, so expired data stays until
/// <see cref="PurgeExpiredAsync"/> runs. Indexes are created on first use.
/// </remarks>
public sealed class MongoStorageProvider : IStorageProvider
{
    private readonly MongoContext _context;
    private readonly MongoDocumentStore _documents;
    private readonly MongoLeaseStore _leases;
    private readonly MongoCounterStore _counters;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoStorageProvider"/> class.
    /// </summary>
    /// <param name="clients">The shared clients.</param>
    /// <param name="options">The storage options.</param>
    /// <param name="timeProvider">The clock for expiry, due times, and lease deadlines.</param>
    public MongoStorageProvider(
        IMongoClientProvider clients,
        MongoStorageOptions options,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(clients);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.DatabaseName);

        _context = new MongoContext(clients, options, timeProvider ?? TimeProvider.System);
        _documents = new MongoDocumentStore(_context);
        _leases = new MongoLeaseStore(_context);
        _counters = new MongoCounterStore(_context);
        Queues = new MongoQueueStore(_context);
        Notifications = options.UseNotifications ? new MongoNotificationChannel(_context) : null;
    }

    /// <inheritdoc />
    public string Name => "MongoDB";

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
    public async ValueTask<long> PurgeExpiredAsync(CancellationToken cancellationToken = default) =>
        await _documents.PurgeExpiredAsync(cancellationToken).ConfigureAwait(false)
        + await _leases.PurgeExpiredAsync(cancellationToken).ConfigureAwait(false)
        + await _counters.PurgeExpiredAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async ValueTask<bool> IsHealthyAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var documents = await _context
                .GetCollectionAsync(MongoContext.DocumentsName, cancellationToken)
                .ConfigureAwait(false);
            await documents
                .Database.RunCommandAsync<BsonDocument>(
                    new BsonDocument("ping", 1),
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
            return true;
        }
        catch (Exception ex) when (ex is MongoException or TimeoutException)
        {
            return false;
        }
    }
}
