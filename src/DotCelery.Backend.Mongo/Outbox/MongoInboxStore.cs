using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.Outbox;

/// <summary>
/// MongoDB implementation of <see cref="IInboxStore"/>.
/// Provides message deduplication for exactly-once processing.
/// </summary>
public sealed class MongoInboxStore : IInboxStore
{
    private readonly MongoInboxStoreOptions _options;
    private readonly ILogger<MongoInboxStore> _logger;
    private readonly MongoClient _client;
    private readonly IMongoDatabase _database;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    private IMongoCollection<InboxDocument>? _collection;
    private bool _initialized;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoInboxStore"/> class.
    /// </summary>
    public MongoInboxStore(
        IOptions<MongoInboxStoreOptions> options,
        ILogger<MongoInboxStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _client = new MongoClient(_options.ConnectionString);
        _database = _client.GetDatabase(_options.DatabaseName);
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsProcessedAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<InboxDocument>.Filter.Eq(d => d.Id, messageId);
        var count = await _collection!
            .CountDocumentsAsync(filter, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        return count > 0;
    }

    /// <inheritdoc />
    public async ValueTask MarkProcessedAsync(
        string messageId,
        object? transaction = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var document = new InboxDocument { Id = messageId, ProcessedAt = DateTime.UtcNow };

        var filter = Builders<InboxDocument>.Filter.Eq(d => d.Id, messageId);
        var replaceOptions = new ReplaceOptions { IsUpsert = true };

        await _collection!
            .ReplaceOneAsync(filter, document, replaceOptions, cancellationToken)
            .ConfigureAwait(false);

        _logger.LogDebug("Marked message {MessageId} as processed", messageId);
    }

    /// <inheritdoc />
    public async ValueTask<long> GetCountAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        return await _collection!
            .CountDocumentsAsync(
                FilterDefinition<InboxDocument>.Empty,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<long> CleanupAsync(
        TimeSpan olderThan,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var cutoff = DateTime.UtcNow - olderThan;
        var filter = Builders<InboxDocument>.Filter.Lt(d => d.ProcessedAt, cutoff);

        var result = await _collection!
            .DeleteManyAsync(filter, cancellationToken)
            .ConfigureAwait(false);

        if (result.DeletedCount > 0)
        {
            _logger.LogInformation(
                "Cleaned up {Count} processed inbox records older than {Cutoff}",
                result.DeletedCount,
                cutoff
            );
        }

        return result.DeletedCount;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _initLock.Dispose();
        _logger.LogInformation("MongoDB inbox store disposed");
    }

    private async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_initialized)
        {
            return;
        }

        await _initLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_initialized)
            {
                return;
            }

            _collection = _database.GetCollection<InboxDocument>(_options.CollectionName);

            if (_options.AutoCreateIndexes)
            {
                await CreateIndexesAsync(cancellationToken).ConfigureAwait(false);
            }

            _initialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }

    private async Task CreateIndexesAsync(CancellationToken cancellationToken)
    {
        var indexes = new List<CreateIndexModel<InboxDocument>>
        {
            new(
                Builders<InboxDocument>.IndexKeys.Ascending(d => d.ProcessedAt),
                new CreateIndexOptions { Name = "idx_processed_at" }
            ),
        };

        await _collection!
            .Indexes.CreateManyAsync(indexes, cancellationToken)
            .ConfigureAwait(false);
    }
}

internal sealed class InboxDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.String)]
    public string Id { get; set; } = string.Empty;

    [BsonElement("processed_at")]
    public DateTime ProcessedAt { get; set; }
}
