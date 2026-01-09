using System.Runtime.CompilerServices;
using DotCelery.Core.Abstractions;
using DotCelery.Core.DeadLetter;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.DeadLetter;

/// <summary>
/// MongoDB implementation of <see cref="IDeadLetterStore"/>.
/// </summary>
public sealed class MongoDeadLetterStore : IDeadLetterStore
{
    private readonly MongoDeadLetterStoreOptions _options;
    private readonly ILogger<MongoDeadLetterStore> _logger;
    private readonly MongoClient _client;
    private readonly IMongoDatabase _database;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    private IMongoCollection<DeadLetterDocument>? _collection;
    private bool _initialized;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoDeadLetterStore"/> class.
    /// </summary>
    public MongoDeadLetterStore(
        IOptions<MongoDeadLetterStoreOptions> options,
        ILogger<MongoDeadLetterStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _client = new MongoClient(_options.ConnectionString);
        _database = _client.GetDatabase(_options.DatabaseName);
    }

    /// <inheritdoc />
    public async ValueTask StoreAsync(
        DeadLetterMessage message,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(message);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var expiresAt = message.ExpiresAt ?? DateTimeOffset.UtcNow.Add(_options.DefaultRetention);
        var document = DeadLetterDocument.FromMessage(message, expiresAt);

        var filter = Builders<DeadLetterDocument>.Filter.Eq(d => d.Id, message.Id);
        var options = new ReplaceOptions { IsUpsert = true };

        await _collection!
            .ReplaceOneAsync(filter, document, options, cancellationToken)
            .ConfigureAwait(false);

        _logger.LogDebug(
            "Stored dead letter message {MessageId} for task {TaskId}",
            message.Id,
            message.TaskId
        );
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<DeadLetterMessage> GetAllAsync(
        int limit = 100,
        int offset = 0,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<DeadLetterDocument>.Filter.Gt(d => d.ExpiresAt, DateTime.UtcNow);
        var sort = Builders<DeadLetterDocument>.Sort.Descending(d => d.Timestamp);

        using var cursor = await _collection!
            .Find(filter)
            .Sort(sort)
            .Skip(offset)
            .Limit(limit)
            .ToCursorAsync(cancellationToken)
            .ConfigureAwait(false);

        while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
        {
            foreach (var document in cursor.Current)
            {
                yield return document.ToMessage();
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask<DeadLetterMessage?> GetAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<DeadLetterDocument>.Filter.And(
            Builders<DeadLetterDocument>.Filter.Eq(d => d.Id, messageId),
            Builders<DeadLetterDocument>.Filter.Gt(d => d.ExpiresAt, DateTime.UtcNow)
        );

        var document = await _collection!
            .Find(filter)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);

        return document?.ToMessage();
    }

    /// <inheritdoc />
    public async ValueTask<bool> RequeueAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        // For MongoDB, requeue means delete and let the caller handle republishing
        return await DeleteAsync(messageId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<bool> DeleteAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<DeadLetterDocument>.Filter.Eq(d => d.Id, messageId);
        var result = await _collection!
            .DeleteOneAsync(filter, cancellationToken)
            .ConfigureAwait(false);

        if (result.DeletedCount > 0)
        {
            _logger.LogDebug("Deleted dead letter message {MessageId}", messageId);
        }

        return result.DeletedCount > 0;
    }

    /// <inheritdoc />
    public async ValueTask<long> GetCountAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<DeadLetterDocument>.Filter.Gt(d => d.ExpiresAt, DateTime.UtcNow);
        return await _collection!
            .CountDocumentsAsync(filter, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<long> PurgeAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var result = await _collection!
            .DeleteManyAsync(FilterDefinition<DeadLetterDocument>.Empty, cancellationToken)
            .ConfigureAwait(false);

        _logger.LogInformation("Purged {Count} dead letter messages", result.DeletedCount);
        return result.DeletedCount;
    }

    /// <inheritdoc />
    public async ValueTask<long> CleanupExpiredAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<DeadLetterDocument>.Filter.Lte(d => d.ExpiresAt, DateTime.UtcNow);
        var result = await _collection!
            .DeleteManyAsync(filter, cancellationToken)
            .ConfigureAwait(false);

        if (result.DeletedCount > 0)
        {
            _logger.LogInformation(
                "Cleaned up {Count} expired dead letter messages",
                result.DeletedCount
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
        _logger.LogInformation("MongoDB dead letter store disposed");
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

            _collection = _database.GetCollection<DeadLetterDocument>(_options.CollectionName);

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
        var indexes = new List<CreateIndexModel<DeadLetterDocument>>
        {
            new(
                Builders<DeadLetterDocument>.IndexKeys.Ascending(d => d.ExpiresAt),
                new CreateIndexOptions { Name = "idx_expires_at", ExpireAfter = TimeSpan.Zero }
            ),
            new(
                Builders<DeadLetterDocument>.IndexKeys.Descending(d => d.Timestamp),
                new CreateIndexOptions { Name = "idx_timestamp" }
            ),
            new(
                Builders<DeadLetterDocument>.IndexKeys.Ascending(d => d.TaskId),
                new CreateIndexOptions { Name = "idx_task_id" }
            ),
        };

        await _collection!
            .Indexes.CreateManyAsync(indexes, cancellationToken)
            .ConfigureAwait(false);
    }
}

internal sealed class DeadLetterDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.String)]
    public string Id { get; set; } = string.Empty;

    [BsonElement("task_id")]
    public string TaskId { get; set; } = string.Empty;

    [BsonElement("task_name")]
    public string TaskName { get; set; } = string.Empty;

    [BsonElement("queue")]
    public string Queue { get; set; } = string.Empty;

    [BsonElement("reason")]
    public string Reason { get; set; } = string.Empty;

    [BsonElement("original_message")]
    public byte[] OriginalMessage { get; set; } = [];

    [BsonElement("exception_message")]
    [BsonIgnoreIfNull]
    public string? ExceptionMessage { get; set; }

    [BsonElement("exception_type")]
    [BsonIgnoreIfNull]
    public string? ExceptionType { get; set; }

    [BsonElement("stack_trace")]
    [BsonIgnoreIfNull]
    public string? StackTrace { get; set; }

    [BsonElement("retry_count")]
    public int RetryCount { get; set; }

    [BsonElement("timestamp")]
    public DateTime Timestamp { get; set; }

    [BsonElement("expires_at")]
    public DateTime ExpiresAt { get; set; }

    [BsonElement("worker")]
    [BsonIgnoreIfNull]
    public string? Worker { get; set; }

    public static DeadLetterDocument FromMessage(
        DeadLetterMessage message,
        DateTimeOffset expiresAt
    )
    {
        return new DeadLetterDocument
        {
            Id = message.Id,
            TaskId = message.TaskId,
            TaskName = message.TaskName,
            Queue = message.Queue,
            Reason = message.Reason.ToString(),
            OriginalMessage = message.OriginalMessage,
            ExceptionMessage = message.ExceptionMessage,
            ExceptionType = message.ExceptionType,
            StackTrace = message.StackTrace,
            RetryCount = message.RetryCount,
            Timestamp = message.Timestamp.UtcDateTime,
            ExpiresAt = expiresAt.UtcDateTime,
            Worker = message.Worker,
        };
    }

    public DeadLetterMessage ToMessage()
    {
        var reason = Enum.TryParse<DeadLetterReason>(Reason, out var r)
            ? r
            : DeadLetterReason.Failed;

        return new DeadLetterMessage
        {
            Id = Id,
            TaskId = TaskId,
            TaskName = TaskName,
            Queue = Queue,
            Reason = reason,
            OriginalMessage = OriginalMessage,
            ExceptionMessage = ExceptionMessage,
            ExceptionType = ExceptionType,
            StackTrace = StackTrace,
            RetryCount = RetryCount,
            Timestamp = new DateTimeOffset(Timestamp, TimeSpan.Zero),
            ExpiresAt = new DateTimeOffset(ExpiresAt, TimeSpan.Zero),
            Worker = Worker,
        };
    }
}
