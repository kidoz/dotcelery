using DotCelery.Core.Batches;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.Batches;

/// <summary>
/// MongoDB implementation of <see cref="IBatchStore"/>.
/// </summary>
public sealed class MongoBatchStore : IBatchStore
{
    private readonly MongoBatchStoreOptions _options;
    private readonly ILogger<MongoBatchStore> _logger;
    private readonly MongoClient _client;
    private readonly IMongoDatabase _database;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    private IMongoCollection<BatchDocument>? _collection;
    private bool _initialized;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoBatchStore"/> class.
    /// </summary>
    public MongoBatchStore(
        IOptions<MongoBatchStoreOptions> options,
        ILogger<MongoBatchStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _client = new MongoClient(_options.ConnectionString);
        _database = _client.GetDatabase(_options.DatabaseName);
    }

    /// <inheritdoc />
    public async ValueTask CreateAsync(Batch batch, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(batch);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var document = BatchDocument.FromBatch(batch);

        await _collection!
            .InsertOneAsync(document, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        _logger.LogDebug("Created batch {BatchId} with {Count} tasks", batch.Id, batch.TotalTasks);
    }

    /// <inheritdoc />
    public async ValueTask<Batch?> GetAsync(
        string batchId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(batchId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<BatchDocument>.Filter.Eq(d => d.Id, batchId);
        var document = await _collection!
            .Find(filter)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);

        return document?.ToBatch();
    }

    /// <inheritdoc />
    public async ValueTask UpdateStateAsync(
        string batchId,
        BatchState state,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(batchId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<BatchDocument>.Filter.Eq(d => d.Id, batchId);
        var update = Builders<BatchDocument>.Update.Set(d => d.State, state.ToString());

        if (state is BatchState.Completed or BatchState.Failed)
        {
            update = update.Set(d => d.CompletedAt, DateTime.UtcNow);
        }

        await _collection!
            .UpdateOneAsync(filter, update, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<Batch?> MarkTaskCompletedAsync(
        string batchId,
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(batchId);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<BatchDocument>.Filter.Eq(d => d.Id, batchId);
        var update = Builders<BatchDocument>.Update.AddToSet(d => d.CompletedTaskIds, taskId);

        await _collection!
            .UpdateOneAsync(filter, update, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        return await GetAsync(batchId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<Batch?> MarkTaskFailedAsync(
        string batchId,
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(batchId);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<BatchDocument>.Filter.Eq(d => d.Id, batchId);
        var update = Builders<BatchDocument>.Update.AddToSet(d => d.FailedTaskIds, taskId);

        await _collection!
            .UpdateOneAsync(filter, update, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        return await GetAsync(batchId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<bool> DeleteAsync(
        string batchId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(batchId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<BatchDocument>.Filter.Eq(d => d.Id, batchId);
        var result = await _collection!
            .DeleteOneAsync(filter, cancellationToken)
            .ConfigureAwait(false);

        return result.DeletedCount > 0;
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetBatchIdForTaskAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<BatchDocument>.Filter.AnyEq(d => d.TaskIds, taskId);
        var projection = Builders<BatchDocument>.Projection.Include(d => d.Id);

        var document = await _collection!
            .Find(filter)
            .Project<BatchDocument>(projection)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);

        return document?.Id;
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
        _logger.LogInformation("MongoDB batch store disposed");
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

            _collection = _database.GetCollection<BatchDocument>(_options.CollectionName);

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
        var indexes = new List<CreateIndexModel<BatchDocument>>
        {
            new(
                Builders<BatchDocument>.IndexKeys.Ascending(d => d.TaskIds),
                new CreateIndexOptions { Name = "idx_task_ids" }
            ),
            new(
                Builders<BatchDocument>.IndexKeys.Ascending(d => d.State),
                new CreateIndexOptions { Name = "idx_state" }
            ),
        };

        await _collection!
            .Indexes.CreateManyAsync(indexes, cancellationToken)
            .ConfigureAwait(false);
    }
}

internal sealed class BatchDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.String)]
    public string Id { get; set; } = string.Empty;

    [BsonElement("name")]
    [BsonIgnoreIfNull]
    public string? Name { get; set; }

    [BsonElement("state")]
    public string State { get; set; } = string.Empty;

    [BsonElement("task_ids")]
    public List<string> TaskIds { get; set; } = [];

    [BsonElement("completed_task_ids")]
    public List<string> CompletedTaskIds { get; set; } = [];

    [BsonElement("failed_task_ids")]
    public List<string> FailedTaskIds { get; set; } = [];

    [BsonElement("callback_task_id")]
    [BsonIgnoreIfNull]
    public string? CallbackTaskId { get; set; }

    [BsonElement("created_at")]
    public DateTime CreatedAt { get; set; }

    [BsonElement("completed_at")]
    [BsonIgnoreIfNull]
    public DateTime? CompletedAt { get; set; }

    public static BatchDocument FromBatch(Batch batch)
    {
        return new BatchDocument
        {
            Id = batch.Id,
            Name = batch.Name,
            State = batch.State.ToString(),
            TaskIds = batch.TaskIds.ToList(),
            CompletedTaskIds = batch.CompletedTaskIds.ToList(),
            FailedTaskIds = batch.FailedTaskIds.ToList(),
            CallbackTaskId = batch.CallbackTaskId,
            CreatedAt = batch.CreatedAt.UtcDateTime,
            CompletedAt = batch.CompletedAt?.UtcDateTime,
        };
    }

    public Batch ToBatch()
    {
        var state = Enum.TryParse<BatchState>(State, out var s) ? s : BatchState.Pending;

        return new Batch
        {
            Id = Id,
            Name = Name,
            State = state,
            TaskIds = TaskIds,
            CompletedTaskIds = CompletedTaskIds,
            FailedTaskIds = FailedTaskIds,
            CallbackTaskId = CallbackTaskId,
            CreatedAt = new DateTimeOffset(CreatedAt, TimeSpan.Zero),
            CompletedAt = CompletedAt.HasValue
                ? new DateTimeOffset(CompletedAt.Value, TimeSpan.Zero)
                : null,
        };
    }
}
