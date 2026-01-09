using DotCelery.Core.Migrations;
using MongoDB.Bson;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.Migrations;

/// <summary>
/// MongoDB implementation of <see cref="IMigrationContext"/>.
/// Supports JavaScript commands and collection operations.
/// </summary>
public sealed class MongoMigrationContext : IMigrationContext
{
    private readonly IMongoDatabase _database;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoMigrationContext"/> class.
    /// </summary>
    /// <param name="database">The MongoDB database.</param>
    public MongoMigrationContext(IMongoDatabase database)
    {
        _database = database;
    }

    /// <summary>
    /// Gets the MongoDB database for direct operations.
    /// </summary>
    public IMongoDatabase Database => _database;

    /// <inheritdoc />
    public async ValueTask ExecuteAsync(
        string command,
        CancellationToken cancellationToken = default
    )
    {
        // For MongoDB, commands are JSON documents that can be run as database commands
        var bsonCommand = BsonDocument.Parse(command);
        await _database
            .RunCommandAsync<BsonDocument>(bsonCommand, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask ExecuteAsync(
        string command,
        IDictionary<string, object?> parameters,
        CancellationToken cancellationToken = default
    )
    {
        var bsonCommand = BsonDocument.Parse(command);

        // Merge parameters into the command
        foreach (var (name, value) in parameters)
        {
            bsonCommand[name] = BsonValue.Create(value);
        }

        await _database
            .RunCommandAsync<BsonDocument>(bsonCommand, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<bool> ExistsAsync(
        string name,
        CancellationToken cancellationToken = default
    )
    {
        var filter = new BsonDocument("name", name);
        var collections = await _database
            .ListCollectionNamesAsync(
                new ListCollectionNamesOptions { Filter = filter },
                cancellationToken
            )
            .ConfigureAwait(false);

        return await collections.AnyAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates a collection if it doesn't exist.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async ValueTask CreateCollectionAsync(
        string collectionName,
        CancellationToken cancellationToken = default
    )
    {
        if (!await ExistsAsync(collectionName, cancellationToken).ConfigureAwait(false))
        {
            await _database
                .CreateCollectionAsync(collectionName, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Drops a collection if it exists.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async ValueTask DropCollectionAsync(
        string collectionName,
        CancellationToken cancellationToken = default
    )
    {
        await _database
            .DropCollectionAsync(collectionName, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Creates an index on a collection.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="indexName">The index name.</param>
    /// <param name="keys">The index keys as a JSON document.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async ValueTask CreateIndexAsync(
        string collectionName,
        string indexName,
        string keys,
        CancellationToken cancellationToken = default
    )
    {
        var collection = _database.GetCollection<BsonDocument>(collectionName);
        var keysDocument = BsonDocument.Parse(keys);
        var indexModel = new CreateIndexModel<BsonDocument>(
            keysDocument,
            new CreateIndexOptions { Name = indexName }
        );

        await collection
            .Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Drops an index from a collection.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="indexName">The index name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async ValueTask DropIndexAsync(
        string collectionName,
        string indexName,
        CancellationToken cancellationToken = default
    )
    {
        var collection = _database.GetCollection<BsonDocument>(collectionName);
        await collection.Indexes.DropOneAsync(indexName, cancellationToken).ConfigureAwait(false);
    }
}
