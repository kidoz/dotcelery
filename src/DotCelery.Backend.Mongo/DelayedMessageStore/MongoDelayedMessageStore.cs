using System.Runtime.CompilerServices;
using System.Text.Json;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.DelayedMessageStore;

/// <summary>
/// MongoDB implementation of <see cref="IDelayedMessageStore"/>.
/// </summary>
public sealed class MongoDelayedMessageStore : IDelayedMessageStore
{
    private readonly MongoDelayedMessageStoreOptions _options;
    private readonly ILogger<MongoDelayedMessageStore> _logger;
    private readonly MongoClient _client;
    private readonly IMongoDatabase _database;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    private IMongoCollection<DelayedMessageDocument>? _collection;
    private bool _initialized;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoDelayedMessageStore"/> class.
    /// </summary>
    public MongoDelayedMessageStore(
        IOptions<MongoDelayedMessageStoreOptions> options,
        ILogger<MongoDelayedMessageStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _client = new MongoClient(_options.ConnectionString);
        _database = _client.GetDatabase(_options.DatabaseName);
    }

    /// <inheritdoc />
    public async ValueTask AddAsync(
        TaskMessage message,
        DateTimeOffset deliveryTime,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(message);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var document = new DelayedMessageDocument
        {
            TaskId = message.Id,
            Message = JsonSerializer.Serialize(message, DotCeleryJsonContext.Default.TaskMessage),
            DeliveryTime = deliveryTime.UtcDateTime,
            CreatedAt = DateTime.UtcNow,
        };

        var filter = Builders<DelayedMessageDocument>.Filter.Eq(d => d.TaskId, message.Id);
        var options = new ReplaceOptions { IsUpsert = true };

        await _collection!
            .ReplaceOneAsync(filter, document, options, cancellationToken)
            .ConfigureAwait(false);

        _logger.LogDebug(
            "Added delayed message {TaskId} for delivery at {DeliveryTime}",
            message.Id,
            deliveryTime
        );
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<TaskMessage> GetDueMessagesAsync(
        DateTimeOffset now,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<DelayedMessageDocument>.Filter.Lte(
            d => d.DeliveryTime,
            now.UtcDateTime
        );
        var sort = Builders<DelayedMessageDocument>.Sort.Ascending(d => d.DeliveryTime);

        using var cursor = await _collection!
            .Find(filter)
            .Sort(sort)
            .Limit(_options.BatchSize)
            .ToCursorAsync(cancellationToken)
            .ConfigureAwait(false);

        while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
        {
            foreach (var document in cursor.Current)
            {
                // Delete and return
                var deleteFilter = Builders<DelayedMessageDocument>.Filter.Eq(
                    d => d.TaskId,
                    document.TaskId
                );
                await _collection!
                    .DeleteOneAsync(deleteFilter, cancellationToken)
                    .ConfigureAwait(false);

                var message = JsonSerializer.Deserialize(
                    document.Message,
                    DotCeleryJsonContext.Default.TaskMessage
                );
                if (message is not null)
                {
                    yield return message;
                }
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask<bool> RemoveAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<DelayedMessageDocument>.Filter.Eq(d => d.TaskId, taskId);
        var result = await _collection!
            .DeleteOneAsync(filter, cancellationToken)
            .ConfigureAwait(false);

        if (result.DeletedCount > 0)
        {
            _logger.LogDebug("Removed delayed message {TaskId}", taskId);
        }

        return result.DeletedCount > 0;
    }

    /// <inheritdoc />
    public async ValueTask<long> GetPendingCountAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        return await _collection!
            .CountDocumentsAsync(
                FilterDefinition<DelayedMessageDocument>.Empty,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<DateTimeOffset?> GetNextDeliveryTimeAsync(
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sort = Builders<DelayedMessageDocument>.Sort.Ascending(d => d.DeliveryTime);
        var projection = Builders<DelayedMessageDocument>.Projection.Include(d => d.DeliveryTime);

        var document = await _collection!
            .Find(FilterDefinition<DelayedMessageDocument>.Empty)
            .Sort(sort)
            .Project<DelayedMessageDocument>(projection)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);

        if (document is null)
        {
            return null;
        }

        return new DateTimeOffset(document.DeliveryTime, TimeSpan.Zero);
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
        _logger.LogInformation("MongoDB delayed message store disposed");
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

            _collection = _database.GetCollection<DelayedMessageDocument>(_options.CollectionName);

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
        var indexes = new List<CreateIndexModel<DelayedMessageDocument>>
        {
            new(
                Builders<DelayedMessageDocument>.IndexKeys.Ascending(d => d.DeliveryTime),
                new CreateIndexOptions { Name = "idx_delivery_time" }
            ),
        };

        await _collection!
            .Indexes.CreateManyAsync(indexes, cancellationToken)
            .ConfigureAwait(false);
    }
}

internal sealed class DelayedMessageDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.String)]
    public string TaskId { get; set; } = string.Empty;

    [BsonElement("message")]
    public string Message { get; set; } = string.Empty;

    [BsonElement("delivery_time")]
    public DateTime DeliveryTime { get; set; }

    [BsonElement("created_at")]
    public DateTime CreatedAt { get; set; }
}
