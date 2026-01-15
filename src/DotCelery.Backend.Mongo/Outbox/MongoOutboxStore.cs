using System.Runtime.CompilerServices;
using System.Text.Json;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Outbox;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.Outbox;

/// <summary>
/// MongoDB implementation of <see cref="IOutboxStore"/>.
/// Provides transactional outbox pattern for exactly-once message delivery.
/// </summary>
public sealed class MongoOutboxStore : IOutboxStore
{
    private readonly MongoOutboxStoreOptions _options;
    private readonly ILogger<MongoOutboxStore> _logger;
    private readonly MongoClient _client;
    private readonly IMongoDatabase _database;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    private IMongoCollection<OutboxDocument>? _collection;
    private bool _initialized;
    private bool _disposed;
    private long _sequenceCounter;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoOutboxStore"/> class.
    /// </summary>
    public MongoOutboxStore(
        IOptions<MongoOutboxStoreOptions> options,
        ILogger<MongoOutboxStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _client = new MongoClient(_options.ConnectionString);
        _database = _client.GetDatabase(_options.DatabaseName);
    }

    /// <inheritdoc />
    public async ValueTask StoreAsync(
        OutboxMessage message,
        object? transaction = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(message);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sequenceNumber = Interlocked.Increment(ref _sequenceCounter);

        var document = new OutboxDocument
        {
            Id = message.Id,
            TaskMessageJson = JsonSerializer.Serialize(
                message.TaskMessage,
                DotCeleryJsonContext.Default.TaskMessage
            ),
            CreatedAt = message.CreatedAt.UtcDateTime,
            Status = message.Status.ToString(),
            Attempts = message.Attempts,
            LastError = message.LastError,
            DispatchedAt = message.DispatchedAt?.UtcDateTime,
            SequenceNumber = sequenceNumber,
        };

        await _collection!
            .InsertOneAsync(document, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        _logger.LogDebug("Stored outbox message {MessageId}", message.Id);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<OutboxMessage> GetPendingAsync(
        int limit = 100,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<OutboxDocument>.Filter.Eq(
            d => d.Status,
            OutboxMessageStatus.Pending.ToString()
        );
        var sort = Builders<OutboxDocument>.Sort.Ascending(d => d.SequenceNumber);

        var cursor = await _collection!
            .Find(filter)
            .Sort(sort)
            .Limit(limit)
            .ToCursorAsync(cancellationToken)
            .ConfigureAwait(false);

        while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
        {
            foreach (var document in cursor.Current)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    yield break;
                }

                var taskMessage = JsonSerializer.Deserialize(
                    document.TaskMessageJson,
                    DotCeleryJsonContext.Default.TaskMessage
                );

                if (taskMessage is null)
                {
                    _logger.LogWarning(
                        "Failed to deserialize task message for outbox {MessageId}",
                        document.Id
                    );
                    continue;
                }

                yield return new OutboxMessage
                {
                    Id = document.Id,
                    TaskMessage = taskMessage,
                    CreatedAt = new DateTimeOffset(document.CreatedAt, TimeSpan.Zero),
                    Status = Enum.Parse<OutboxMessageStatus>(document.Status),
                    Attempts = document.Attempts,
                    LastError = document.LastError,
                    DispatchedAt = document.DispatchedAt.HasValue
                        ? new DateTimeOffset(document.DispatchedAt.Value, TimeSpan.Zero)
                        : null,
                    SequenceNumber = document.SequenceNumber,
                };
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask MarkDispatchedAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<OutboxDocument>.Filter.Eq(d => d.Id, messageId);
        var update = Builders<OutboxDocument>
            .Update.Set(d => d.Status, OutboxMessageStatus.Dispatched.ToString())
            .Set(d => d.DispatchedAt, DateTime.UtcNow);

        await _collection!
            .UpdateOneAsync(filter, update, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        _logger.LogDebug("Marked outbox message {MessageId} as dispatched", messageId);
    }

    /// <inheritdoc />
    public async ValueTask MarkFailedAsync(
        string messageId,
        string errorMessage,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<OutboxDocument>.Filter.Eq(d => d.Id, messageId);
        var document = await _collection!
            .Find(filter)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);

        if (document is null)
        {
            return;
        }

        var newAttempts = document.Attempts + 1;
        var newStatus = newAttempts >= 5 ? OutboxMessageStatus.Failed.ToString() : document.Status;

        var update = Builders<OutboxDocument>
            .Update.Set(d => d.Status, newStatus)
            .Set(d => d.Attempts, newAttempts)
            .Set(d => d.LastError, errorMessage);

        await _collection!
            .UpdateOneAsync(filter, update, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        _logger.LogDebug(
            "Marked outbox message {MessageId} as failed (attempt {Attempt})",
            messageId,
            newAttempts
        );
    }

    /// <inheritdoc />
    public async ValueTask<long> GetPendingCountAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<OutboxDocument>.Filter.Eq(
            d => d.Status,
            OutboxMessageStatus.Pending.ToString()
        );
        return await _collection!
            .CountDocumentsAsync(filter, cancellationToken: cancellationToken)
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
        var filter = Builders<OutboxDocument>.Filter.And(
            Builders<OutboxDocument>.Filter.Eq(
                d => d.Status,
                OutboxMessageStatus.Dispatched.ToString()
            ),
            Builders<OutboxDocument>.Filter.Lt(d => d.DispatchedAt, cutoff)
        );

        var result = await _collection!
            .DeleteManyAsync(filter, cancellationToken)
            .ConfigureAwait(false);

        if (result.DeletedCount > 0)
        {
            _logger.LogInformation(
                "Cleaned up {Count} dispatched outbox messages older than {Cutoff}",
                result.DeletedCount,
                cutoff
            );
        }

        return result.DeletedCount;
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
        _logger.LogInformation("MongoDB outbox store disposed");
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

            _collection = _database.GetCollection<OutboxDocument>(_options.CollectionName);

            if (_options.AutoCreateIndexes)
            {
                await CreateIndexesAsync(cancellationToken).ConfigureAwait(false);
            }

            // Initialize sequence counter from existing max
            var maxDoc = await _collection
                .Find(FilterDefinition<OutboxDocument>.Empty)
                .Sort(Builders<OutboxDocument>.Sort.Descending(d => d.SequenceNumber))
                .Limit(1)
                .FirstOrDefaultAsync(cancellationToken)
                .ConfigureAwait(false);

            _sequenceCounter = maxDoc?.SequenceNumber ?? 0;

            _initialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }

    private async Task CreateIndexesAsync(CancellationToken cancellationToken)
    {
        var indexes = new List<CreateIndexModel<OutboxDocument>>
        {
            new(
                Builders<OutboxDocument>.IndexKeys.Ascending(d => d.Status),
                new CreateIndexOptions { Name = "idx_status" }
            ),
            new(
                Builders<OutboxDocument>.IndexKeys.Ascending(d => d.SequenceNumber),
                new CreateIndexOptions { Name = "idx_sequence" }
            ),
            new(
                Builders<OutboxDocument>.IndexKeys.Ascending(d => d.DispatchedAt),
                new CreateIndexOptions { Name = "idx_dispatched_at", Sparse = true }
            ),
        };

        await _collection!
            .Indexes.CreateManyAsync(indexes, cancellationToken)
            .ConfigureAwait(false);
    }
}

internal sealed class OutboxDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.String)]
    public string Id { get; set; } = string.Empty;

    [BsonElement("task_message")]
    public string TaskMessageJson { get; set; } = string.Empty;

    [BsonElement("created_at")]
    public DateTime CreatedAt { get; set; }

    [BsonElement("status")]
    public string Status { get; set; } = string.Empty;

    [BsonElement("attempts")]
    public int Attempts { get; set; }

    [BsonElement("last_error")]
    [BsonIgnoreIfNull]
    public string? LastError { get; set; }

    [BsonElement("dispatched_at")]
    [BsonIgnoreIfNull]
    public DateTime? DispatchedAt { get; set; }

    [BsonElement("sequence_number")]
    public long SequenceNumber { get; set; }
}
