using MongoDB.Bson;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.Storage;

/// <summary>
/// What the MongoDB primitives share: the database, its indexes, sequences, and clock.
/// </summary>
internal sealed class MongoContext(
    IMongoClientProvider clients,
    MongoStorageOptions options,
    TimeProvider timeProvider
)
{
    public const string DocumentsName = "documents";
    public const string LeasesName = "leases";
    public const string QueueItemsName = "queue_items";
    public const string CountersName = "counters";
    public const string WindowsName = "windows";
    public const string NotificationsName = "notifications";
    private const string SequencesName = "sequences";

    private static readonly FilterDefinitionBuilder<BsonDocument> Filter =
        Builders<BsonDocument>.Filter;

    private readonly Lock _lock = new();
    private Task<IMongoDatabase>? _database;

    public MongoStorageOptions Options => options;

    public long Now() => MongoTime.ToMicroseconds(timeProvider.GetUtcNow());

    /// <summary>
    /// A filter that matches documents whose expiry, if any, has not passed.
    /// </summary>
    public static FilterDefinition<BsonDocument> Live(string field, long now) =>
        Filter.Or(Filter.Eq(field, BsonNull.Value), Filter.Gt(field, now));

    public async Task<IMongoCollection<BsonDocument>> GetCollectionAsync(
        string name,
        CancellationToken cancellationToken
    )
    {
        var database = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        return database.GetCollection<BsonDocument>(options.CollectionPrefix + name);
    }

    public async Task<long> NextAsync(string sequence, CancellationToken cancellationToken)
    {
        var sequences = await GetCollectionAsync(SequencesName, cancellationToken)
            .ConfigureAwait(false);
        var document = await sequences
            .FindOneAndUpdateAsync(
                Filter.Eq("_id", sequence),
                Builders<BsonDocument>.Update.Inc("n", 1L),
                new FindOneAndUpdateOptions<BsonDocument>
                {
                    IsUpsert = true,
                    ReturnDocument = ReturnDocument.After,
                },
                cancellationToken
            )
            .ConfigureAwait(false);
        return document["n"].ToInt64();
    }

    public static bool IsDuplicateKey(MongoException exception) =>
        exception is MongoWriteException { WriteError.Category: ServerErrorCategory.DuplicateKey }
        || exception is MongoCommandException { Code: 11000 };

    // Indexes are created once per process; a failed attempt is retried by the next caller
    private Task<IMongoDatabase> GetDatabaseAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_lock)
        {
            if (_database is null || _database.IsFaulted || _database.IsCanceled)
            {
                _database = InitializeAsync();
            }

            return _database;
        }
    }

    private async Task<IMongoDatabase> InitializeAsync()
    {
        var database = clients.GetClient(options).GetDatabase(options.DatabaseName);
        IMongoCollection<BsonDocument> Collection(string name) =>
            database.GetCollection<BsonDocument>(options.CollectionPrefix + name);
        var keys = Builders<BsonDocument>.IndexKeys;

        await Collection(DocumentsName)
            .Indexes.CreateManyAsync([
                new(
                    keys.Ascending("c").Ascending("i"),
                    new CreateIndexOptions { Unique = true, Name = "collection_id" }
                ),
                new(
                    keys.Ascending("c").Ascending("sk").Ascending("i"),
                    new CreateIndexOptions { Name = "collection_sort" }
                ),
                new(
                    keys.Ascending("c").Ascending("ik").Ascending("sk").Ascending("i"),
                    new CreateIndexOptions { Name = "collection_index_sort" }
                ),
                new(keys.Ascending("exp"), new CreateIndexOptions { Name = "expiry" }),
            ])
            .ConfigureAwait(false);
        await Collection(LeasesName)
            .Indexes.CreateOneAsync(
                new CreateIndexModel<BsonDocument>(
                    keys.Ascending("exp"),
                    new CreateIndexOptions { Name = "expiry" }
                )
            )
            .ConfigureAwait(false);
        await Collection(QueueItemsName)
            .Indexes.CreateManyAsync([
                new(
                    keys.Ascending("q").Ascending("i"),
                    new CreateIndexOptions { Unique = true, Name = "queue_id" }
                ),
                new(
                    keys.Ascending("q").Ascending("due").Ascending("i"),
                    new CreateIndexOptions { Name = "queue_due" }
                ),
                new(
                    keys.Ascending("q").Ascending("tok"),
                    new CreateIndexOptions { Name = "queue_claim" }
                ),
            ])
            .ConfigureAwait(false);
        await Collection(CountersName)
            .Indexes.CreateOneAsync(
                new CreateIndexModel<BsonDocument>(
                    keys.Ascending("exp"),
                    new CreateIndexOptions { Name = "expiry" }
                )
            )
            .ConfigureAwait(false);

        if (options.UseNotifications)
        {
            await CreateNotificationCollectionAsync(database).ConfigureAwait(false);
        }

        return database;
    }

    // A tailable cursor dies when its query matches nothing, so the collection always keeps a document
    private async Task CreateNotificationCollectionAsync(IMongoDatabase database)
    {
        var name = options.CollectionPrefix + NotificationsName;
        try
        {
            await database
                .CreateCollectionAsync(
                    name,
                    new CreateCollectionOptions
                    {
                        Capped = true,
                        MaxSize = options.NotificationCollectionSize,
                    }
                )
                .ConfigureAwait(false);
        }
        catch (MongoCommandException ex) when (ex.CodeName == "NamespaceExists") { }

        var notifications = database.GetCollection<BsonDocument>(name);
        if (await notifications.Find(Filter.Empty).Limit(1).AnyAsync().ConfigureAwait(false))
        {
            return;
        }

        await notifications
            .InsertOneAsync(MongoNotificationChannel.CreateMessage("", ""))
            .ConfigureAwait(false);
    }
}
