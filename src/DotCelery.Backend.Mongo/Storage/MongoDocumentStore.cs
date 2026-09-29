using System.Runtime.CompilerServices;
using DotCelery.Core.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.Storage;

/// <summary>
/// MongoDB <see cref="IDocumentStore"/>. All collections share one MongoDB collection, keyed
/// by collection and id; versions come from a sequence per collection.
/// </summary>
internal sealed class MongoDocumentStore(MongoContext context) : IDocumentStore
{
    private static readonly FilterDefinitionBuilder<BsonDocument> Filter =
        Builders<BsonDocument>.Filter;

    public async ValueTask<StoredDocument?> GetAsync(
        string collection,
        string id,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(collection);
        StorageGuard.ThrowIfInvalidKey(id);

        var documents = await DocumentsAsync(cancellationToken).ConfigureAwait(false);
        var document = await documents
            .Find(Key(collection, id) & MongoContext.Live("exp", context.Now()))
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);
        return document is null ? null : ToDocument(document);
    }

    public async ValueTask<long?> TryInsertAsync(
        string collection,
        string id,
        ReadOnlyMemory<byte> value,
        DocumentWriteOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        Validate(collection, id, options);

        var documents = await DocumentsAsync(cancellationToken).ConfigureAwait(false);
        var now = context.Now();

        // An expired document is treated as deleted
        await documents
            .DeleteOneAsync(Key(collection, id) & Filter.Lte("exp", now), cancellationToken)
            .ConfigureAwait(false);

        var version = await NextVersionAsync(collection, cancellationToken).ConfigureAwait(false);
        try
        {
            await documents
                .InsertOneAsync(
                    ToBson(collection, id, value, version, options, now),
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
            return version;
        }
        catch (MongoException ex) when (MongoContext.IsDuplicateKey(ex))
        {
            return null;
        }
    }

    public async ValueTask<long?> TryReplaceAsync(
        string collection,
        string id,
        ReadOnlyMemory<byte> value,
        long expectedVersion,
        DocumentWriteOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        Validate(collection, id, options);

        var documents = await DocumentsAsync(cancellationToken).ConfigureAwait(false);
        var now = context.Now();
        var version = await NextVersionAsync(collection, cancellationToken).ConfigureAwait(false);
        var result = await documents
            .ReplaceOneAsync(
                Key(collection, id)
                    & Filter.Eq("ver", expectedVersion)
                    & MongoContext.Live("exp", now),
                ToBson(collection, id, value, version, options, now),
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);
        return result.MatchedCount == 1 ? version : null;
    }

    public async ValueTask<long> UpsertAsync(
        string collection,
        string id,
        ReadOnlyMemory<byte> value,
        DocumentWriteOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        Validate(collection, id, options);

        var documents = await DocumentsAsync(cancellationToken).ConfigureAwait(false);
        var version = await NextVersionAsync(collection, cancellationToken).ConfigureAwait(false);
        var replacement = ToBson(collection, id, value, version, options, context.Now());

        // Two upserts of a missing document race on the unique index; the loser replaces
        while (true)
        {
            try
            {
                await documents
                    .ReplaceOneAsync(
                        Key(collection, id),
                        replacement,
                        new ReplaceOptions { IsUpsert = true },
                        cancellationToken
                    )
                    .ConfigureAwait(false);
                return version;
            }
            catch (MongoException ex) when (MongoContext.IsDuplicateKey(ex)) { }
        }
    }

    public async ValueTask<bool> DeleteAsync(
        string collection,
        string id,
        long? expectedVersion = null,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(collection);
        StorageGuard.ThrowIfInvalidKey(id);

        var documents = await DocumentsAsync(cancellationToken).ConfigureAwait(false);
        var filter = Key(collection, id) & MongoContext.Live("exp", context.Now());
        if (expectedVersion is { } version)
        {
            filter &= Filter.Eq("ver", version);
        }

        var result = await documents
            .DeleteOneAsync(filter, cancellationToken)
            .ConfigureAwait(false);
        return result.DeletedCount == 1;
    }

    public async IAsyncEnumerable<StoredDocument> QueryAsync(
        string collection,
        DocumentFilter filter,
        DocumentPage? page = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(collection);
        StorageGuard.ThrowIfInvalid(filter);
        StorageGuard.ThrowIfInvalid(page);

        var sort = Builders<BsonDocument>.Sort;
        var documents = await DocumentsAsync(cancellationToken).ConfigureAwait(false);
        var find = documents
            .Find(Matching(collection, filter))
            .Sort(
                page?.Descending == true
                    ? sort.Descending("sk").Descending("i")
                    : sort.Ascending("sk").Ascending("i")
            )
            .Skip(page?.Offset ?? 0);
        if (page?.Limit is { } limit)
        {
            find = find.Limit(limit);
        }

        using var cursor = await find.ToCursorAsync(cancellationToken).ConfigureAwait(false);
        while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
        {
            foreach (var document in cursor.Current)
            {
                yield return ToDocument(document);
            }
        }
    }

    public async ValueTask<long> CountAsync(
        string collection,
        DocumentFilter filter,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(collection);
        StorageGuard.ThrowIfInvalid(filter);

        var documents = await DocumentsAsync(cancellationToken).ConfigureAwait(false);
        return await documents
            .CountDocumentsAsync(Matching(collection, filter), cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask<long> DeleteManyAsync(
        string collection,
        DocumentFilter filter,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(collection);
        StorageGuard.ThrowIfInvalid(filter);

        var documents = await DocumentsAsync(cancellationToken).ConfigureAwait(false);
        var result = await documents
            .DeleteManyAsync(Matching(collection, filter), cancellationToken)
            .ConfigureAwait(false);
        return result.DeletedCount;
    }

    public async ValueTask<long> PurgeExpiredAsync(CancellationToken cancellationToken)
    {
        var documents = await DocumentsAsync(cancellationToken).ConfigureAwait(false);
        var result = await documents
            .DeleteManyAsync(Filter.Lte("exp", context.Now()), cancellationToken)
            .ConfigureAwait(false);
        return result.DeletedCount;
    }

    private static FilterDefinition<BsonDocument> Key(string collection, string id) =>
        Filter.Eq("c", collection) & Filter.Eq("i", id);

    // A range compares numbers only, so documents without a sort key (null) never match one
    private FilterDefinition<BsonDocument> Matching(string collection, DocumentFilter filter)
    {
        var matching = Filter.Eq("c", collection) & MongoContext.Live("exp", context.Now());
        if (filter.IndexKey is { } indexKey)
        {
            matching &= Filter.Eq("ik", indexKey);
        }

        if (filter.SortKeyFrom is { } from)
        {
            matching &= Filter.Gte("sk", MongoTime.ToMicroseconds(from));
        }

        if (filter.SortKeyBefore is { } before)
        {
            matching &= Filter.Lt("sk", MongoTime.ToMicroseconds(before));
        }

        return matching;
    }

    private static void Validate(string collection, string id, DocumentWriteOptions? options)
    {
        StorageGuard.ThrowIfInvalidName(collection);
        StorageGuard.ThrowIfInvalidKey(id);
        StorageGuard.ThrowIfInvalid(options);
    }

    private static BsonDocument ToBson(
        string collection,
        string id,
        ReadOnlyMemory<byte> value,
        long version,
        DocumentWriteOptions? options,
        long now
    ) =>
        new()
        {
            { "c", collection },
            { "i", id },
            { "v", new BsonBinaryData(value.ToArray()) },
            { "ver", version },
            { "ik", options?.IndexKey is { } indexKey ? indexKey : BsonNull.Value },
            {
                "sk",
                options?.SortKey is { } sortKey ? MongoTime.ToMicroseconds(sortKey) : BsonNull.Value
            },
            {
                "exp",
                options?.TimeToLive is { } timeToLive
                    ? now + MongoTime.ToMicroseconds(timeToLive)
                    : BsonNull.Value
            },
        };

    private static StoredDocument ToDocument(BsonDocument document) =>
        new(
            document["i"].AsString,
            document["v"].AsByteArray,
            document["ver"].ToInt64(),
            document["ik"].IsBsonNull ? null : document["ik"].AsString,
            document["sk"].IsBsonNull ? null : MongoTime.FromMicroseconds(document["sk"].ToInt64()),
            document["exp"].IsBsonNull
                ? null
                : MongoTime.FromMicroseconds(document["exp"].ToInt64())
        );

    private Task<IMongoCollection<BsonDocument>> DocumentsAsync(
        CancellationToken cancellationToken
    ) => context.GetCollectionAsync(MongoContext.DocumentsName, cancellationToken);

    private Task<long> NextVersionAsync(string collection, CancellationToken cancellationToken) =>
        context.NextAsync("documents:" + collection, cancellationToken);
}
