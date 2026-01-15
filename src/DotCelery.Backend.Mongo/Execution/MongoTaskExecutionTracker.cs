using DotCelery.Core.Execution;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.Execution;

/// <summary>
/// MongoDB implementation of <see cref="ITaskExecutionTracker"/>.
/// Uses MongoDB TTL indexes for automatic expiration.
/// </summary>
public sealed class MongoTaskExecutionTracker : ITaskExecutionTracker
{
    private readonly MongoTaskExecutionTrackerOptions _options;
    private readonly ILogger<MongoTaskExecutionTracker> _logger;
    private readonly MongoClient _client;
    private readonly IMongoDatabase _database;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    private IMongoCollection<ExecutionTrackingDocument>? _collection;
    private bool _initialized;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoTaskExecutionTracker"/> class.
    /// </summary>
    public MongoTaskExecutionTracker(
        IOptions<MongoTaskExecutionTrackerOptions> options,
        ILogger<MongoTaskExecutionTracker> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _client = new MongoClient(_options.ConnectionString);
        _database = _client.GetDatabase(_options.DatabaseName);
    }

    /// <inheritdoc />
    public async ValueTask<bool> TryStartAsync(
        string taskName,
        string taskId,
        string? key = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskName);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var now = DateTime.UtcNow;
        var expiresAt = now.Add(timeout ?? _options.DefaultTimeout);
        var documentId = BuildDocumentId(taskName, key);

        // Try to insert or update only if document doesn't exist or is expired
        var filter = Builders<ExecutionTrackingDocument>.Filter.And(
            Builders<ExecutionTrackingDocument>.Filter.Eq(d => d.Id, documentId),
            Builders<ExecutionTrackingDocument>.Filter.Or(
                Builders<ExecutionTrackingDocument>.Filter.Lt(d => d.ExpiresAt, now),
                Builders<ExecutionTrackingDocument>.Filter.Eq(d => d.TaskId, taskId)
            )
        );

        var update = Builders<ExecutionTrackingDocument>
            .Update.Set(d => d.TaskId, taskId)
            .Set(d => d.TaskName, taskName)
            .Set(d => d.Key, key)
            .Set(d => d.StartedAt, now)
            .Set(d => d.ExpiresAt, expiresAt);

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

            var started = result.ModifiedCount > 0 || result.UpsertedId is not null;

            if (started)
            {
                _logger.LogDebug(
                    "Started tracking execution for {TaskName} with task {TaskId}",
                    taskName,
                    taskId
                );
            }

            return started;
        }
        catch (MongoWriteException ex)
            when (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
        {
            // Another instance is already executing
            _logger.LogDebug(
                "Failed to start tracking for {TaskName} - another instance is already executing",
                taskName
            );
            return false;
        }
    }

    /// <inheritdoc />
    public async ValueTask StopAsync(
        string taskName,
        string taskId,
        string? key = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskName);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var documentId = BuildDocumentId(taskName, key);
        var filter = Builders<ExecutionTrackingDocument>.Filter.And(
            Builders<ExecutionTrackingDocument>.Filter.Eq(d => d.Id, documentId),
            Builders<ExecutionTrackingDocument>.Filter.Eq(d => d.TaskId, taskId)
        );

        var result = await _collection!
            .DeleteOneAsync(filter, cancellationToken)
            .ConfigureAwait(false);

        if (result.DeletedCount > 0)
        {
            _logger.LogDebug(
                "Stopped tracking execution for {TaskName} with task {TaskId}",
                taskName,
                taskId
            );
        }
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsExecutingAsync(
        string taskName,
        string? key = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskName);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var documentId = BuildDocumentId(taskName, key);
        var filter = Builders<ExecutionTrackingDocument>.Filter.And(
            Builders<ExecutionTrackingDocument>.Filter.Eq(d => d.Id, documentId),
            Builders<ExecutionTrackingDocument>.Filter.Gt(d => d.ExpiresAt, DateTime.UtcNow)
        );

        var count = await _collection!
            .CountDocumentsAsync(filter, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        return count > 0;
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetExecutingTaskIdAsync(
        string taskName,
        string? key = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskName);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var documentId = BuildDocumentId(taskName, key);
        var filter = Builders<ExecutionTrackingDocument>.Filter.And(
            Builders<ExecutionTrackingDocument>.Filter.Eq(d => d.Id, documentId),
            Builders<ExecutionTrackingDocument>.Filter.Gt(d => d.ExpiresAt, DateTime.UtcNow)
        );

        var projection = Builders<ExecutionTrackingDocument>.Projection.Include(d => d.TaskId);

        var document = await _collection!
            .Find(filter)
            .Project<ExecutionTrackingDocument>(projection)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);

        return document?.TaskId;
    }

    /// <inheritdoc />
    public async ValueTask<bool> ExtendAsync(
        string taskName,
        string taskId,
        string? key = null,
        TimeSpan? extension = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskName);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var documentId = BuildDocumentId(taskName, key);
        var filter = Builders<ExecutionTrackingDocument>.Filter.And(
            Builders<ExecutionTrackingDocument>.Filter.Eq(d => d.Id, documentId),
            Builders<ExecutionTrackingDocument>.Filter.Eq(d => d.TaskId, taskId),
            Builders<ExecutionTrackingDocument>.Filter.Gt(d => d.ExpiresAt, DateTime.UtcNow)
        );

        // Get current document to calculate new expiry
        var currentDoc = await _collection!
            .Find(filter)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);

        if (currentDoc is null)
        {
            return false;
        }

        var extensionTime = extension ?? _options.DefaultTimeout;
        var newExpiresAt = currentDoc.ExpiresAt.Add(extensionTime);

        var update = Builders<ExecutionTrackingDocument>.Update.Set(d => d.ExpiresAt, newExpiresAt);
        var result = await _collection!
            .UpdateOneAsync(filter, update, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        if (result.ModifiedCount > 0)
        {
            _logger.LogDebug(
                "Extended execution tracking for {TaskName} by {Extension}",
                taskName,
                extensionTime
            );
        }

        return result.ModifiedCount > 0;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, ExecutingTaskInfo>> GetAllExecutingAsync(
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<ExecutionTrackingDocument>.Filter.Gt(
            d => d.ExpiresAt,
            DateTime.UtcNow
        );

        var documents = await _collection!
            .Find(filter)
            .ToListAsync(cancellationToken)
            .ConfigureAwait(false);

        var result = new Dictionary<string, ExecutingTaskInfo>();
        foreach (var doc in documents)
        {
            result[doc.TaskName] = new ExecutingTaskInfo
            {
                TaskId = doc.TaskId,
                Key = doc.Key,
                StartedAt = new DateTimeOffset(doc.StartedAt, TimeSpan.Zero),
                ExpiresAt = new DateTimeOffset(doc.ExpiresAt, TimeSpan.Zero),
            };
        }

        return result;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await ValueTask.CompletedTask;

        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _initLock.Dispose();
        _logger.LogInformation("MongoDB task execution tracker disposed");
    }

    private static string BuildDocumentId(string taskName, string? key)
    {
        return key is not null ? $"{taskName}:{key}" : taskName;
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

            _collection = _database.GetCollection<ExecutionTrackingDocument>(
                _options.CollectionName
            );

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
        var indexes = new List<CreateIndexModel<ExecutionTrackingDocument>>
        {
            new(
                Builders<ExecutionTrackingDocument>.IndexKeys.Ascending(d => d.ExpiresAt),
                new CreateIndexOptions { Name = "idx_expires_at", ExpireAfter = TimeSpan.Zero }
            ),
            new(
                Builders<ExecutionTrackingDocument>.IndexKeys.Ascending(d => d.TaskName),
                new CreateIndexOptions { Name = "idx_task_name" }
            ),
        };

        await _collection!
            .Indexes.CreateManyAsync(indexes, cancellationToken)
            .ConfigureAwait(false);
    }
}

internal sealed class ExecutionTrackingDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.String)]
    public string Id { get; set; } = string.Empty;

    [BsonElement("task_id")]
    public string TaskId { get; set; } = string.Empty;

    [BsonElement("task_name")]
    public string TaskName { get; set; } = string.Empty;

    [BsonElement("key")]
    [BsonIgnoreIfNull]
    public string? Key { get; set; }

    [BsonElement("started_at")]
    public DateTime StartedAt { get; set; }

    [BsonElement("expires_at")]
    public DateTime ExpiresAt { get; set; }
}
