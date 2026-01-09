using DotCelery.Core.Migrations;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.Migrations;

/// <summary>
/// MongoDB implementation of <see cref="IMigrationStore"/>.
/// Stores migration history in a MongoDB collection.
/// </summary>
public sealed class MongoMigrationStore : IMigrationStore
{
    private readonly IMongoDatabase _database;
    private readonly string _collectionName;
    private IMongoCollection<MigrationDocument>? _collection;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoMigrationStore"/> class.
    /// </summary>
    /// <param name="database">The MongoDB database.</param>
    /// <param name="collectionName">The migration history collection name.</param>
    public MongoMigrationStore(IMongoDatabase database, string collectionName = "_migrations")
    {
        _database = database;
        _collectionName = collectionName;
    }

    /// <inheritdoc />
    public async ValueTask InitializeAsync(CancellationToken cancellationToken = default)
    {
        _collection = _database.GetCollection<MigrationDocument>(_collectionName);

        // Create index on version for fast lookups
        var indexModel = new CreateIndexModel<MigrationDocument>(
            Builders<MigrationDocument>.IndexKeys.Ascending(d => d.Version),
            new CreateIndexOptions { Name = "idx_version", Unique = true }
        );

        await _collection
            .Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<long>> GetAppliedVersionsAsync(
        CancellationToken cancellationToken = default
    )
    {
        if (_collection is null)
        {
            await InitializeAsync(cancellationToken).ConfigureAwait(false);
        }

        var documents = await _collection!
            .Find(FilterDefinition<MigrationDocument>.Empty)
            .SortBy(d => d.Version)
            .ToListAsync(cancellationToken)
            .ConfigureAwait(false);

        return documents.Select(d => d.Version).ToList();
    }

    /// <inheritdoc />
    public async ValueTask MarkAppliedAsync(
        long version,
        string description,
        CancellationToken cancellationToken = default
    )
    {
        if (_collection is null)
        {
            await InitializeAsync(cancellationToken).ConfigureAwait(false);
        }

        var document = new MigrationDocument
        {
            Version = version,
            Description = description,
            AppliedAt = DateTime.UtcNow,
        };

        var options = new ReplaceOptions { IsUpsert = true };
        var filter = Builders<MigrationDocument>.Filter.Eq(d => d.Version, version);

        await _collection!
            .ReplaceOneAsync(filter, document, options, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask MarkRolledBackAsync(
        long version,
        CancellationToken cancellationToken = default
    )
    {
        if (_collection is null)
        {
            await InitializeAsync(cancellationToken).ConfigureAwait(false);
        }

        var filter = Builders<MigrationDocument>.Filter.Eq(d => d.Version, version);
        await _collection!.DeleteOneAsync(filter, cancellationToken).ConfigureAwait(false);
    }
}

/// <summary>
/// MongoDB document for migration records.
/// </summary>
internal sealed class MigrationDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.Int64)]
    public long Version { get; set; }

    [BsonElement("description")]
    public string Description { get; set; } = string.Empty;

    [BsonElement("applied_at")]
    public DateTime AppliedAt { get; set; }
}
