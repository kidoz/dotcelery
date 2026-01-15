using System.Runtime.CompilerServices;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Signals;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.Signals;

/// <summary>
/// MongoDB implementation of <see cref="ISignalStore"/>.
/// Provides durable signal queue with visibility timeout pattern.
/// </summary>
public sealed class MongoSignalStore : ISignalStore
{
    private readonly MongoSignalStoreOptions _options;
    private readonly ILogger<MongoSignalStore> _logger;
    private readonly MongoClient _client;
    private readonly IMongoDatabase _database;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    private IMongoCollection<SignalDocument>? _collection;
    private bool _initialized;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoSignalStore"/> class.
    /// </summary>
    public MongoSignalStore(
        IOptions<MongoSignalStoreOptions> options,
        ILogger<MongoSignalStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _client = new MongoClient(_options.ConnectionString);
        _database = _client.GetDatabase(_options.DatabaseName);
    }

    /// <inheritdoc />
    public async ValueTask EnqueueAsync(
        SignalMessage message,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(message);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var document = new SignalDocument
        {
            Id = message.Id,
            SignalType = message.SignalType,
            TaskId = message.TaskId,
            TaskName = message.TaskName,
            Payload = message.Payload,
            CreatedAt = message.CreatedAt.UtcDateTime,
            Status = SignalStatus.Pending,
            VisibleAt = DateTime.UtcNow,
        };

        await _collection!
            .InsertOneAsync(document, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        _logger.LogDebug(
            "Enqueued signal {SignalId} of type {SignalType} for task {TaskId}",
            message.Id,
            message.SignalType,
            message.TaskId
        );
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<SignalMessage> DequeueAsync(
        int batchSize = 100,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var now = DateTime.UtcNow;
        var visibilityTimeout = now.Add(_options.VisibilityTimeout);
        var count = 0;

        while (count < batchSize && !cancellationToken.IsCancellationRequested)
        {
            // Find and update atomically - claim the message
            var filter = Builders<SignalDocument>.Filter.And(
                Builders<SignalDocument>.Filter.Eq(d => d.Status, SignalStatus.Pending),
                Builders<SignalDocument>.Filter.Lte(d => d.VisibleAt, now)
            );

            var update = Builders<SignalDocument>
                .Update.Set(d => d.Status, SignalStatus.Processing)
                .Set(d => d.VisibleAt, visibilityTimeout);

            var options = new FindOneAndUpdateOptions<SignalDocument>
            {
                ReturnDocument = ReturnDocument.After,
                Sort = Builders<SignalDocument>.Sort.Ascending(d => d.CreatedAt),
            };

            var document = await _collection!
                .FindOneAndUpdateAsync(filter, update, options, cancellationToken)
                .ConfigureAwait(false);

            if (document is null)
            {
                // No more messages available
                break;
            }

            count++;

            yield return new SignalMessage
            {
                Id = document.Id,
                SignalType = document.SignalType,
                TaskId = document.TaskId,
                TaskName = document.TaskName,
                Payload = document.Payload,
                CreatedAt = new DateTimeOffset(document.CreatedAt, TimeSpan.Zero),
            };
        }
    }

    /// <inheritdoc />
    public async ValueTask AcknowledgeAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<SignalDocument>.Filter.And(
            Builders<SignalDocument>.Filter.Eq(d => d.Id, messageId),
            Builders<SignalDocument>.Filter.Eq(d => d.Status, SignalStatus.Processing)
        );

        var result = await _collection!
            .DeleteOneAsync(filter, cancellationToken)
            .ConfigureAwait(false);

        if (result.DeletedCount > 0)
        {
            _logger.LogDebug("Acknowledged signal {SignalId}", messageId);
        }
    }

    /// <inheritdoc />
    public async ValueTask RejectAsync(
        string messageId,
        bool requeue = true,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<SignalDocument>.Filter.Eq(d => d.Id, messageId);

        if (requeue)
        {
            // Put back in queue with immediate visibility
            var update = Builders<SignalDocument>
                .Update.Set(d => d.Status, SignalStatus.Pending)
                .Set(d => d.VisibleAt, DateTime.UtcNow);

            await _collection!
                .UpdateOneAsync(filter, update, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            _logger.LogDebug("Requeued signal {SignalId}", messageId);
        }
        else
        {
            // Delete permanently
            await _collection!.DeleteOneAsync(filter, cancellationToken).ConfigureAwait(false);

            _logger.LogDebug("Rejected and deleted signal {SignalId}", messageId);
        }
    }

    /// <inheritdoc />
    public async ValueTask<long> GetPendingCountAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<SignalDocument>.Filter.Eq(d => d.Status, SignalStatus.Pending);
        return await _collection!
            .CountDocumentsAsync(filter, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
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
        _logger.LogInformation("MongoDB signal store disposed");
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

            _collection = _database.GetCollection<SignalDocument>(_options.CollectionName);

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
        var indexes = new List<CreateIndexModel<SignalDocument>>
        {
            new(
                Builders<SignalDocument>
                    .IndexKeys.Ascending(d => d.Status)
                    .Ascending(d => d.VisibleAt),
                new CreateIndexOptions { Name = "idx_status_visible" }
            ),
            new(
                Builders<SignalDocument>.IndexKeys.Ascending(d => d.TaskId),
                new CreateIndexOptions { Name = "idx_task_id" }
            ),
            new(
                Builders<SignalDocument>.IndexKeys.Ascending(d => d.CreatedAt),
                new CreateIndexOptions { Name = "idx_created_at" }
            ),
        };

        await _collection!
            .Indexes.CreateManyAsync(indexes, cancellationToken)
            .ConfigureAwait(false);
    }
}

internal enum SignalStatus
{
    Pending,
    Processing,
}

internal sealed class SignalDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.String)]
    public string Id { get; set; } = string.Empty;

    [BsonElement("signal_type")]
    public string SignalType { get; set; } = string.Empty;

    [BsonElement("task_id")]
    public string TaskId { get; set; } = string.Empty;

    [BsonElement("task_name")]
    public string TaskName { get; set; } = string.Empty;

    [BsonElement("payload")]
    public string Payload { get; set; } = string.Empty;

    [BsonElement("created_at")]
    public DateTime CreatedAt { get; set; }

    [BsonElement("status")]
    public SignalStatus Status { get; set; }

    [BsonElement("visible_at")]
    public DateTime VisibleAt { get; set; }
}
