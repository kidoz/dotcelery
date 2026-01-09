using System.Runtime.CompilerServices;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.Revocation;

/// <summary>
/// MongoDB implementation of <see cref="IRevocationStore"/>.
/// </summary>
public sealed class MongoRevocationStore : IRevocationStore
{
    private readonly MongoRevocationStoreOptions _options;
    private readonly ILogger<MongoRevocationStore> _logger;
    private readonly MongoClient _client;
    private readonly IMongoDatabase _database;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    private IMongoCollection<RevocationDocument>? _collection;
    private bool _initialized;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoRevocationStore"/> class.
    /// </summary>
    public MongoRevocationStore(
        IOptions<MongoRevocationStoreOptions> options,
        ILogger<MongoRevocationStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _client = new MongoClient(_options.ConnectionString);
        _database = _client.GetDatabase(_options.DatabaseName);
    }

    /// <inheritdoc />
    public async ValueTask RevokeAsync(
        string taskId,
        RevokeOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        options ??= RevokeOptions.Default;
        var expiry = options.Expiry ?? _options.DefaultRetention;
        var expiresAt = DateTime.UtcNow.Add(expiry);

        var document = new RevocationDocument
        {
            TaskId = taskId,
            Terminate = options.Terminate,
            Signal = options.Signal.ToString(),
            RevokedAt = DateTime.UtcNow,
            ExpiresAt = expiresAt,
        };

        var filter = Builders<RevocationDocument>.Filter.Eq(d => d.TaskId, taskId);
        var replaceOptions = new ReplaceOptions { IsUpsert = true };

        await _collection!
            .ReplaceOneAsync(filter, document, replaceOptions, cancellationToken)
            .ConfigureAwait(false);

        _logger.LogDebug(
            "Revoked task {TaskId} with terminate={Terminate}",
            taskId,
            options.Terminate
        );
    }

    /// <inheritdoc />
    public async ValueTask RevokeAsync(
        IEnumerable<string> taskIds,
        RevokeOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(taskIds);

        options ??= RevokeOptions.Default;
        var expiry = options.Expiry ?? _options.DefaultRetention;
        var expiresAt = DateTime.UtcNow.Add(expiry);
        var now = DateTime.UtcNow;

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var bulkOps = new List<WriteModel<RevocationDocument>>();

        foreach (var taskId in taskIds)
        {
            if (string.IsNullOrEmpty(taskId))
            {
                continue;
            }

            var document = new RevocationDocument
            {
                TaskId = taskId,
                Terminate = options.Terminate,
                Signal = options.Signal.ToString(),
                RevokedAt = now,
                ExpiresAt = expiresAt,
            };

            var filter = Builders<RevocationDocument>.Filter.Eq(d => d.TaskId, taskId);
            bulkOps.Add(
                new ReplaceOneModel<RevocationDocument>(filter, document) { IsUpsert = true }
            );
        }

        if (bulkOps.Count > 0)
        {
            await _collection!
                .BulkWriteAsync(bulkOps, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
            _logger.LogDebug("Revoked {Count} tasks", bulkOps.Count);
        }
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsRevokedAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<RevocationDocument>.Filter.And(
            Builders<RevocationDocument>.Filter.Eq(d => d.TaskId, taskId),
            Builders<RevocationDocument>.Filter.Gt(d => d.ExpiresAt, DateTime.UtcNow)
        );

        var count = await _collection!
            .CountDocumentsAsync(filter, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        return count > 0;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> GetRevokedTaskIdsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<RevocationDocument>.Filter.Gt(d => d.ExpiresAt, DateTime.UtcNow);
        var projection = Builders<RevocationDocument>.Projection.Include(d => d.TaskId);

        using var cursor = await _collection!
            .Find(filter)
            .Project<RevocationDocument>(projection)
            .ToCursorAsync(cancellationToken)
            .ConfigureAwait(false);

        while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
        {
            foreach (var document in cursor.Current)
            {
                yield return document.TaskId;
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask<long> CleanupAsync(
        TimeSpan maxAge,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var cutoff = DateTime.UtcNow - maxAge;
        var filter = Builders<RevocationDocument>.Filter.Lt(d => d.RevokedAt, cutoff);
        var result = await _collection!
            .DeleteManyAsync(filter, cancellationToken)
            .ConfigureAwait(false);

        if (result.DeletedCount > 0)
        {
            _logger.LogInformation("Cleaned up {Count} expired revocations", result.DeletedCount);
        }

        return result.DeletedCount;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RevocationEvent> SubscribeAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        // Use MongoDB change streams to watch for new revocations
        var pipeline = new EmptyPipelineDefinition<
            ChangeStreamDocument<RevocationDocument>
        >().Match(change =>
            change.OperationType == ChangeStreamOperationType.Insert
            || change.OperationType == ChangeStreamOperationType.Replace
        );

        var options = new ChangeStreamOptions
        {
            FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
        };

        using var cursor = await _collection!
            .WatchAsync(pipeline, options, cancellationToken)
            .ConfigureAwait(false);

        while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
        {
            foreach (var change in cursor.Current)
            {
                if (change.FullDocument is null)
                {
                    continue;
                }

                var doc = change.FullDocument;
                var signal = Enum.TryParse<CancellationSignal>(doc.Signal, out var s)
                    ? s
                    : CancellationSignal.Graceful;

                yield return new RevocationEvent
                {
                    TaskId = doc.TaskId,
                    Options = new RevokeOptions { Terminate = doc.Terminate, Signal = signal },
                    Timestamp = new DateTimeOffset(doc.RevokedAt, TimeSpan.Zero),
                };
            }
        }
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
        _logger.LogInformation("MongoDB revocation store disposed");
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

            _collection = _database.GetCollection<RevocationDocument>(_options.CollectionName);

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
        var indexes = new List<CreateIndexModel<RevocationDocument>>
        {
            new(
                Builders<RevocationDocument>.IndexKeys.Ascending(d => d.ExpiresAt),
                new CreateIndexOptions { Name = "idx_expires_at", ExpireAfter = TimeSpan.Zero }
            ),
            new(
                Builders<RevocationDocument>.IndexKeys.Ascending(d => d.RevokedAt),
                new CreateIndexOptions { Name = "idx_revoked_at" }
            ),
        };

        await _collection!
            .Indexes.CreateManyAsync(indexes, cancellationToken)
            .ConfigureAwait(false);
    }
}

internal sealed class RevocationDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.String)]
    public string TaskId { get; set; } = string.Empty;

    [BsonElement("terminate")]
    public bool Terminate { get; set; }

    [BsonElement("signal")]
    public string Signal { get; set; } = string.Empty;

    [BsonElement("revoked_at")]
    public DateTime RevokedAt { get; set; }

    [BsonElement("expires_at")]
    public DateTime ExpiresAt { get; set; }
}
