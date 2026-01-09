using DotCelery.Core.Partitioning;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.Partitioning;

/// <summary>
/// MongoDB implementation of <see cref="IPartitionLockStore"/>.
/// Uses MongoDB TTL indexes for automatic lock expiration.
/// </summary>
public sealed class MongoPartitionLockStore : IPartitionLockStore
{
    private readonly MongoPartitionLockStoreOptions _options;
    private readonly ILogger<MongoPartitionLockStore> _logger;
    private readonly MongoClient _client;
    private readonly IMongoDatabase _database;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    private IMongoCollection<PartitionLockDocument>? _collection;
    private bool _initialized;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoPartitionLockStore"/> class.
    /// </summary>
    public MongoPartitionLockStore(
        IOptions<MongoPartitionLockStoreOptions> options,
        ILogger<MongoPartitionLockStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _client = new MongoClient(_options.ConnectionString);
        _database = _client.GetDatabase(_options.DatabaseName);
    }

    /// <inheritdoc />
    public async ValueTask<bool> TryAcquireAsync(
        string partitionKey,
        string taskId,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(partitionKey);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var now = DateTime.UtcNow;
        var expiresAt = now.Add(timeout);

        // Try to insert or update only if lock doesn't exist or is expired
        var filter = Builders<PartitionLockDocument>.Filter.And(
            Builders<PartitionLockDocument>.Filter.Eq(d => d.PartitionKey, partitionKey),
            Builders<PartitionLockDocument>.Filter.Or(
                Builders<PartitionLockDocument>.Filter.Lt(d => d.ExpiresAt, now),
                Builders<PartitionLockDocument>.Filter.Eq(d => d.TaskId, taskId)
            )
        );

        var update = Builders<PartitionLockDocument>
            .Update.Set(d => d.TaskId, taskId)
            .Set(d => d.AcquiredAt, now)
            .Set(d => d.ExpiresAt, expiresAt)
            .SetOnInsert(d => d.PartitionKey, partitionKey);

        try
        {
            var result = await _collection!
                .UpdateOneAsync(
                    filter,
                    update,
                    new UpdateOptions { IsUpsert = true },
                    cancellationToken
                )
                .ConfigureAwait(false);

            var acquired = result.ModifiedCount > 0 || result.UpsertedId is not null;

            if (acquired)
            {
                _logger.LogDebug(
                    "Acquired partition lock for {PartitionKey} by task {TaskId}",
                    partitionKey,
                    taskId
                );
            }

            return acquired;
        }
        catch (MongoWriteException ex)
            when (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
        {
            // Lock already exists and is held by another task
            _logger.LogDebug(
                "Failed to acquire partition lock for {PartitionKey} - already held",
                partitionKey
            );
            return false;
        }
    }

    /// <inheritdoc />
    public async ValueTask<bool> ReleaseAsync(
        string partitionKey,
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(partitionKey);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<PartitionLockDocument>.Filter.And(
            Builders<PartitionLockDocument>.Filter.Eq(d => d.PartitionKey, partitionKey),
            Builders<PartitionLockDocument>.Filter.Eq(d => d.TaskId, taskId)
        );

        var result = await _collection!
            .DeleteOneAsync(filter, cancellationToken)
            .ConfigureAwait(false);

        if (result.DeletedCount > 0)
        {
            _logger.LogDebug(
                "Released partition lock for {PartitionKey} by task {TaskId}",
                partitionKey,
                taskId
            );
        }

        return result.DeletedCount > 0;
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsLockedAsync(
        string partitionKey,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(partitionKey);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<PartitionLockDocument>.Filter.And(
            Builders<PartitionLockDocument>.Filter.Eq(d => d.PartitionKey, partitionKey),
            Builders<PartitionLockDocument>.Filter.Gt(d => d.ExpiresAt, DateTime.UtcNow)
        );

        var count = await _collection!
            .CountDocumentsAsync(filter, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        return count > 0;
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetLockHolderAsync(
        string partitionKey,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(partitionKey);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<PartitionLockDocument>.Filter.And(
            Builders<PartitionLockDocument>.Filter.Eq(d => d.PartitionKey, partitionKey),
            Builders<PartitionLockDocument>.Filter.Gt(d => d.ExpiresAt, DateTime.UtcNow)
        );

        var projection = Builders<PartitionLockDocument>.Projection.Include(d => d.TaskId);

        var document = await _collection!
            .Find(filter)
            .Project<PartitionLockDocument>(projection)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);

        return document?.TaskId;
    }

    /// <inheritdoc />
    public async ValueTask<bool> ExtendAsync(
        string partitionKey,
        string taskId,
        TimeSpan extension,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(partitionKey);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<PartitionLockDocument>.Filter.And(
            Builders<PartitionLockDocument>.Filter.Eq(d => d.PartitionKey, partitionKey),
            Builders<PartitionLockDocument>.Filter.Eq(d => d.TaskId, taskId),
            Builders<PartitionLockDocument>.Filter.Gt(d => d.ExpiresAt, DateTime.UtcNow)
        );

        // MongoDB doesn't support Inc on DateTime, so we need to get-set approach
        var currentDoc = await _collection!
            .Find(filter)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);

        if (currentDoc is null)
        {
            return false;
        }

        var newExpiresAt = currentDoc.ExpiresAt.Add(extension);
        var setUpdate = Builders<PartitionLockDocument>.Update.Set(d => d.ExpiresAt, newExpiresAt);

        var result = await _collection!
            .UpdateOneAsync(filter, setUpdate, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        if (result.ModifiedCount > 0)
        {
            _logger.LogDebug(
                "Extended partition lock for {PartitionKey} by {Extension}",
                partitionKey,
                extension
            );
        }

        return result.ModifiedCount > 0;
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
        _logger.LogInformation("MongoDB partition lock store disposed");
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

            _collection = _database.GetCollection<PartitionLockDocument>(_options.CollectionName);

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
        var indexes = new List<CreateIndexModel<PartitionLockDocument>>
        {
            new(
                Builders<PartitionLockDocument>.IndexKeys.Ascending(d => d.ExpiresAt),
                new CreateIndexOptions { Name = "idx_expires_at", ExpireAfter = TimeSpan.Zero }
            ),
            new(
                Builders<PartitionLockDocument>.IndexKeys.Ascending(d => d.TaskId),
                new CreateIndexOptions { Name = "idx_task_id" }
            ),
        };

        await _collection!
            .Indexes.CreateManyAsync(indexes, cancellationToken)
            .ConfigureAwait(false);
    }
}

internal sealed class PartitionLockDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.String)]
    public string PartitionKey { get; set; } = string.Empty;

    [BsonElement("task_id")]
    public string TaskId { get; set; } = string.Empty;

    [BsonElement("acquired_at")]
    public DateTime AcquiredAt { get; set; }

    [BsonElement("expires_at")]
    public DateTime ExpiresAt { get; set; }
}
