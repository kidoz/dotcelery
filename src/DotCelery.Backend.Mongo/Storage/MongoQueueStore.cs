using DotCelery.Core.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.Storage;

/// <summary>
/// MongoDB <see cref="IQueueStore"/>. Items are claimed by marking them with a claim token in
/// one conditional update, so concurrent consumers never claim the same item.
/// </summary>
internal sealed class MongoQueueStore(MongoContext context) : IQueueStore
{
    private static readonly FilterDefinitionBuilder<BsonDocument> Filter =
        Builders<BsonDocument>.Filter;

    private static readonly SortDefinition<BsonDocument> DueOrder = Builders<BsonDocument>
        .Sort.Ascending("due")
        .Ascending("i");

    public async ValueTask EnqueueAsync(
        string queue,
        string id,
        ReadOnlyMemory<byte> payload,
        DateTimeOffset dueAt,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(queue);
        StorageGuard.ThrowIfInvalidKey(id);

        var items = await ItemsAsync(cancellationToken).ConfigureAwait(false);
        var item = new BsonDocument
        {
            { "q", queue },
            { "i", id },
            { "p", new BsonBinaryData(payload.ToArray()) },
            { "due", MongoTime.ToMicroseconds(dueAt) },
            { "dc", 0 },
            { "tok", BsonNull.Value },
            { "until", BsonNull.Value },
        };

        // Two enqueues of a missing item race on the unique index; the loser replaces
        while (true)
        {
            try
            {
                await items
                    .ReplaceOneAsync(
                        Key(queue, id),
                        item,
                        new ReplaceOptions { IsUpsert = true },
                        cancellationToken
                    )
                    .ConfigureAwait(false);
                return;
            }
            catch (MongoException ex) when (MongoContext.IsDuplicateKey(ex)) { }
        }
    }

    public async ValueTask<IReadOnlyList<QueueItem>> ClaimAsync(
        string queue,
        int maxItems,
        TimeSpan visibilityTimeout,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(queue);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxItems, 1);
        StorageGuard.ThrowIfNotPositive(visibilityTimeout);

        var items = await ItemsAsync(cancellationToken).ConfigureAwait(false);
        var now = context.Now();
        var claimedUntil = now + MongoTime.ToMicroseconds(visibilityTimeout);
        var claimToken = Guid.NewGuid();
        var token = claimToken.ToString("N");
        var claimable =
            Filter.Eq("q", queue)
            & Filter.Lte("due", now)
            & Filter.Or(Filter.Eq("until", BsonNull.Value), Filter.Lte("until", now));

        // Candidates another consumer claims first no longer match, so look again for the rest
        long claimed = 0;
        while (claimed < maxItems)
        {
            var candidates = await items
                .Find(claimable)
                .Sort(DueOrder)
                .Limit((int)(maxItems - claimed))
                .Project(Builders<BsonDocument>.Projection.Include("i"))
                .ToListAsync(cancellationToken)
                .ConfigureAwait(false);
            if (candidates.Count == 0)
            {
                break;
            }

            var result = await items
                .UpdateManyAsync(
                    claimable & Filter.In("i", candidates.Select(c => c["i"])),
                    Builders<BsonDocument>
                        .Update.Set("tok", token)
                        .Set("until", claimedUntil)
                        .Inc("dc", 1),
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
            claimed += result.ModifiedCount;
        }

        if (claimed == 0)
        {
            return [];
        }

        var documents = await items
            .Find(Filter.Eq("q", queue) & Filter.Eq("tok", token))
            .Sort(DueOrder)
            .ToListAsync(cancellationToken)
            .ConfigureAwait(false);
        return documents
            .Select(d => new QueueItem(
                queue,
                d["i"].AsString,
                d["p"].AsByteArray,
                MongoTime.FromMicroseconds(d["due"].ToInt64()),
                d["dc"].ToInt32(),
                claimToken,
                MongoTime.FromMicroseconds(claimedUntil)
            ))
            .ToList();
    }

    public async ValueTask<bool> CompleteAsync(
        QueueItem item,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(item);

        var items = await ItemsAsync(cancellationToken).ConfigureAwait(false);
        var result = await items
            .DeleteOneAsync(Claim(item), cancellationToken)
            .ConfigureAwait(false);
        return result.DeletedCount == 1;
    }

    public async ValueTask<bool> AbandonAsync(
        QueueItem item,
        DateTimeOffset? dueAt = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(item);

        var items = await ItemsAsync(cancellationToken).ConfigureAwait(false);
        var result = await items
            .UpdateOneAsync(
                Claim(item),
                Builders<BsonDocument>
                    .Update.Set("tok", BsonNull.Value)
                    .Set("until", BsonNull.Value)
                    .Set("due", MongoTime.ToMicroseconds(dueAt ?? item.DueAt)),
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);
        return result.ModifiedCount == 1;
    }

    public async ValueTask<bool> RemoveAsync(
        string queue,
        string id,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(queue);
        StorageGuard.ThrowIfInvalidKey(id);

        var items = await ItemsAsync(cancellationToken).ConfigureAwait(false);
        var result = await items
            .DeleteOneAsync(Key(queue, id), cancellationToken)
            .ConfigureAwait(false);
        return result.DeletedCount == 1;
    }

    public async ValueTask<long> CountAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(queue);

        var items = await ItemsAsync(cancellationToken).ConfigureAwait(false);
        return await items
            .CountDocumentsAsync(Filter.Eq("q", queue), cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask<DateTimeOffset?> GetNextDueAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(queue);

        var items = await ItemsAsync(cancellationToken).ConfigureAwait(false);
        var now = context.Now();
        var next = await items
            .Find(
                Filter.Eq("q", queue)
                    & Filter.Or(Filter.Eq("until", BsonNull.Value), Filter.Lte("until", now))
            )
            .Sort(DueOrder)
            .Limit(1)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);
        return next is null ? null : MongoTime.FromMicroseconds(next["due"].ToInt64());
    }

    private static FilterDefinition<BsonDocument> Key(string queue, string id) =>
        Filter.Eq("q", queue) & Filter.Eq("i", id);

    private static FilterDefinition<BsonDocument> Claim(QueueItem item) =>
        Key(item.Queue, item.Id) & Filter.Eq("tok", item.ClaimToken.ToString("N"));

    private Task<IMongoCollection<BsonDocument>> ItemsAsync(CancellationToken cancellationToken) =>
        context.GetCollectionAsync(MongoContext.QueueItemsName, cancellationToken);
}
