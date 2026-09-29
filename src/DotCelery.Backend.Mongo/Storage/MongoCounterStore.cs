using DotCelery.Core.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.Storage;

/// <summary>
/// MongoDB <see cref="ICounterStore"/>. A counter is a document of its value and expiry; a
/// window is one document of event times, checked and updated in one pipeline update.
/// </summary>
internal sealed class MongoCounterStore(MongoContext context) : ICounterStore
{
    private static readonly FilterDefinitionBuilder<BsonDocument> Filter =
        Builders<BsonDocument>.Filter;

    public async ValueTask<long> IncrementAsync(
        string key,
        long delta = 1,
        TimeSpan? timeToLive = null,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidKey(key);
        if (timeToLive is { } ttl)
        {
            StorageGuard.ThrowIfNotPositive(ttl, nameof(timeToLive));
        }

        var counters = await CountersAsync(cancellationToken).ConfigureAwait(false);
        var now = context.Now();
        var created = new BsonDocument
        {
            { "_id", key },
            { "n", delta },
            {
                "exp",
                timeToLive is { } lifetime
                    ? now + MongoTime.ToMicroseconds(lifetime)
                    : BsonNull.Value
            },
        };

        while (true)
        {
            var incremented = await counters
                .FindOneAndUpdateAsync(
                    Filter.Eq("_id", key) & MongoContext.Live("exp", now),
                    Builders<BsonDocument>.Update.Inc("n", delta),
                    new FindOneAndUpdateOptions<BsonDocument>
                    {
                        ReturnDocument = ReturnDocument.After,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
            if (incremented is not null)
            {
                return incremented["n"].ToInt64();
            }

            // An expired counter starts again from zero
            var restarted = await counters
                .ReplaceOneAsync(
                    Filter.Eq("_id", key) & Filter.Lte("exp", now),
                    created,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
            if (restarted.MatchedCount == 1)
            {
                return delta;
            }

            try
            {
                await counters
                    .InsertOneAsync(created, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
                return delta;
            }
            catch (MongoException ex) when (MongoContext.IsDuplicateKey(ex)) { }
        }
    }

    public async ValueTask<long> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        StorageGuard.ThrowIfInvalidKey(key);

        var counters = await CountersAsync(cancellationToken).ConfigureAwait(false);
        var counter = await counters
            .Find(Filter.Eq("_id", key) & MongoContext.Live("exp", context.Now()))
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);
        return counter?["n"].ToInt64() ?? 0;
    }

    public async ValueTask<bool> DeleteAsync(
        string key,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidKey(key);

        var counters = await CountersAsync(cancellationToken).ConfigureAwait(false);
        var result = await counters
            .DeleteOneAsync(
                Filter.Eq("_id", key) & MongoContext.Live("exp", context.Now()),
                cancellationToken
            )
            .ConfigureAwait(false);
        return result.DeletedCount == 1;
    }

    public async ValueTask<WindowResult> TryAddToWindowAsync(
        string key,
        int limit,
        TimeSpan window,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidKey(key);
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        StorageGuard.ThrowIfNotPositive(window);

        var windows = await WindowsAsync(cancellationToken).ConfigureAwait(false);
        var now = context.Now();
        var length = MongoTime.ToMicroseconds(window);

        // Events at or before the window start have left it; w keeps the longest window for purges
        var update = new PipelineUpdateDefinition<BsonDocument>(
            new BsonDocument[]
            {
                new(
                    "$set",
                    new BsonDocument(
                        "e",
                        EventsAfter(
                            new BsonDocument("$ifNull", new BsonArray { "$e", new BsonArray() }),
                            now - length
                        )
                    )
                ),
                new(
                    "$set",
                    new BsonDocument
                    {
                        {
                            "added",
                            new BsonDocument(
                                "$lt",
                                new BsonArray { new BsonDocument("$size", "$e"), limit }
                            )
                        },
                        {
                            "w",
                            new BsonDocument(
                                "$max",
                                new BsonArray
                                {
                                    new BsonDocument("$ifNull", new BsonArray { "$w", 0L }),
                                    length,
                                }
                            )
                        },
                    }
                ),
                new(
                    "$set",
                    new BsonDocument(
                        "e",
                        new BsonDocument(
                            "$cond",
                            new BsonArray
                            {
                                "$added",
                                new BsonDocument(
                                    "$concatArrays",
                                    new BsonArray
                                    {
                                        "$e",
                                        new BsonArray { now },
                                    }
                                ),
                                "$e",
                            }
                        )
                    )
                ),
            }
        );

        BsonDocument result;
        while (true)
        {
            try
            {
                result = await windows
                    .FindOneAndUpdateAsync(
                        Filter.Eq("_id", key),
                        update,
                        new FindOneAndUpdateOptions<BsonDocument>
                        {
                            IsUpsert = true,
                            ReturnDocument = ReturnDocument.After,
                        },
                        cancellationToken
                    )
                    .ConfigureAwait(false);
                break;
            }
            catch (MongoException ex) when (MongoContext.IsDuplicateKey(ex)) { }
        }

        var events = result["e"].AsBsonArray;
        return result["added"].AsBoolean
            ? new WindowResult(true, events.Count, null)
            : new WindowResult(
                false,
                events.Count,
                MongoTime.FromMicroseconds(events.Min(e => e.ToInt64()) + length)
                    - MongoTime.FromMicroseconds(now)
            );
    }

    public async ValueTask<WindowSnapshot> GetWindowAsync(
        string key,
        TimeSpan window,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidKey(key);
        StorageGuard.ThrowIfNotPositive(window);

        var windows = await WindowsAsync(cancellationToken).ConfigureAwait(false);
        var start = context.Now() - MongoTime.ToMicroseconds(window);
        var document = await windows
            .Find(Filter.Eq("_id", key))
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);

        var events =
            document?["e"].AsBsonArray.Select(e => e.ToInt64()).Where(e => e > start).ToList()
            ?? [];
        return new WindowSnapshot(
            events.Count,
            events.Count > 0 ? MongoTime.FromMicroseconds(events.Min()) : null
        );
    }

    // Counters with a time to live, and windows whose events have all left the longest window
    public async ValueTask<long> PurgeExpiredAsync(CancellationToken cancellationToken)
    {
        var counters = await CountersAsync(cancellationToken).ConfigureAwait(false);
        var now = context.Now();
        var purged = (
            await counters
                .DeleteManyAsync(Filter.Lte("exp", now), cancellationToken)
                .ConfigureAwait(false)
        ).DeletedCount;

        var windows = await WindowsAsync(cancellationToken).ConfigureAwait(false);
        await windows
            .UpdateManyAsync(
                Filter.Empty,
                new PipelineUpdateDefinition<BsonDocument>(
                    new BsonDocument[]
                    {
                        new(
                            "$set",
                            new BsonDocument(
                                "e",
                                EventsAfter(
                                    "$e",
                                    new BsonDocument("$subtract", new BsonArray { now, "$w" })
                                )
                            )
                        ),
                    }
                ),
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);
        purged += (
            await windows
                .DeleteManyAsync(Filter.Size("e", 0), cancellationToken)
                .ConfigureAwait(false)
        ).DeletedCount;

        return purged;
    }

    private static BsonDocument EventsAfter(BsonValue events, BsonValue start) =>
        new(
            "$filter",
            new BsonDocument
            {
                { "input", events },
                { "cond", new BsonDocument("$gt", new BsonArray { "$$this", start }) },
            }
        );

    private Task<IMongoCollection<BsonDocument>> CountersAsync(
        CancellationToken cancellationToken
    ) => context.GetCollectionAsync(MongoContext.CountersName, cancellationToken);

    private Task<IMongoCollection<BsonDocument>> WindowsAsync(
        CancellationToken cancellationToken
    ) => context.GetCollectionAsync(MongoContext.WindowsName, cancellationToken);
}
