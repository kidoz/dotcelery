using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using DotCelery.Core.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.Storage;

/// <summary>
/// MongoDB <see cref="ILeaseStore"/>. Each lease is a document keyed by the leased key; one
/// sequence issues every fencing token.
/// </summary>
internal sealed class MongoLeaseStore(MongoContext context) : ILeaseStore
{
    private const int MaxAttempts = 10;

    private static readonly FilterDefinitionBuilder<BsonDocument> Filter =
        Builders<BsonDocument>.Filter;

    private static readonly FindOneAndUpdateOptions<BsonDocument> ReturnUpdated = new()
    {
        ReturnDocument = ReturnDocument.After,
    };

    public async ValueTask<Lease?> TryAcquireAsync(
        string key,
        string owner,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidKey(key);
        StorageGuard.ThrowIfInvalidKey(owner);
        StorageGuard.ThrowIfNotPositive(duration);

        var leases = await LeasesAsync(cancellationToken).ConfigureAwait(false);
        var now = context.Now();
        var expiresAt = now + MongoTime.ToMicroseconds(duration);

        for (var attempt = 0; attempt < MaxAttempts; attempt++)
        {
            // The same owner keeps its token and acquisition time while its lease is live
            var extended = await leases
                .FindOneAndUpdateAsync(
                    Filter.Eq("_id", key) & Filter.Eq("owner", owner) & Filter.Gt("exp", now),
                    Builders<BsonDocument>.Update.Set("exp", expiresAt),
                    ReturnUpdated,
                    cancellationToken
                )
                .ConfigureAwait(false);
            if (extended is not null)
            {
                return ToLease(extended);
            }

            var token = await context
                .NextAsync("lease-tokens", cancellationToken)
                .ConfigureAwait(false);
            var lease = new BsonDocument
            {
                { "_id", key },
                { "owner", owner },
                { "token", token },
                { "acq", now },
                { "exp", expiresAt },
            };

            var takenOver = await leases
                .FindOneAndReplaceAsync(
                    Filter.Eq("_id", key) & Filter.Lte("exp", now),
                    lease,
                    new FindOneAndReplaceOptions<BsonDocument>
                    {
                        ReturnDocument = ReturnDocument.After,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
            if (takenOver is not null)
            {
                return ToLease(takenOver);
            }

            try
            {
                await leases
                    .InsertOneAsync(lease, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
                return ToLease(lease);
            }
            catch (MongoException ex) when (MongoContext.IsDuplicateKey(ex))
            {
                if (
                    await GetAsync(key, cancellationToken).ConfigureAwait(false) is { } current
                    && current.Owner != owner
                )
                {
                    return null;
                }
            }
        }

        return null;
    }

    public async ValueTask<Lease?> RenewAsync(
        Lease lease,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(lease);
        StorageGuard.ThrowIfNotPositive(duration);

        var leases = await LeasesAsync(cancellationToken).ConfigureAwait(false);
        var now = context.Now();
        var renewed = await leases
            .FindOneAndUpdateAsync(
                Filter.Eq("_id", lease.Key)
                    & Filter.Eq("token", lease.Token)
                    & Filter.Gt("exp", now),
                Builders<BsonDocument>.Update.Set("exp", now + MongoTime.ToMicroseconds(duration)),
                ReturnUpdated,
                cancellationToken
            )
            .ConfigureAwait(false);
        return renewed is null ? null : ToLease(renewed);
    }

    public async ValueTask<bool> ReleaseAsync(
        Lease lease,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(lease);

        var leases = await LeasesAsync(cancellationToken).ConfigureAwait(false);
        var result = await leases
            .DeleteOneAsync(
                Filter.Eq("_id", lease.Key) & Filter.Eq("token", lease.Token),
                cancellationToken
            )
            .ConfigureAwait(false);
        return result.DeletedCount == 1;
    }

    public async ValueTask<Lease?> GetAsync(
        string key,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidKey(key);

        var leases = await LeasesAsync(cancellationToken).ConfigureAwait(false);
        var lease = await leases
            .Find(Filter.Eq("_id", key) & Filter.Gt("exp", context.Now()))
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);
        return lease is null ? null : ToLease(lease);
    }

    public async IAsyncEnumerable<Lease> ListAsync(
        string keyPrefix,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidPrefix(keyPrefix);

        var leases = await LeasesAsync(cancellationToken).ConfigureAwait(false);
        var filter = Filter.Gt("exp", context.Now());
        if (keyPrefix.Length > 0)
        {
            filter &= Filter.Regex("_id", new BsonRegularExpression("^" + Regex.Escape(keyPrefix)));
        }

        using var cursor = await leases
            .Find(filter)
            .Sort(Builders<BsonDocument>.Sort.Ascending("_id"))
            .ToCursorAsync(cancellationToken)
            .ConfigureAwait(false);
        while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
        {
            foreach (var lease in cursor.Current)
            {
                yield return ToLease(lease);
            }
        }
    }

    public async ValueTask<long> PurgeExpiredAsync(CancellationToken cancellationToken)
    {
        var leases = await LeasesAsync(cancellationToken).ConfigureAwait(false);
        var result = await leases
            .DeleteManyAsync(Filter.Lte("exp", context.Now()), cancellationToken)
            .ConfigureAwait(false);
        return result.DeletedCount;
    }

    private static Lease ToLease(BsonDocument lease) =>
        new(
            lease["_id"].AsString,
            lease["owner"].AsString,
            lease["token"].ToInt64(),
            MongoTime.FromMicroseconds(lease["acq"].ToInt64()),
            MongoTime.FromMicroseconds(lease["exp"].ToInt64())
        );

    private Task<IMongoCollection<BsonDocument>> LeasesAsync(CancellationToken cancellationToken) =>
        context.GetCollectionAsync(MongoContext.LeasesName, cancellationToken);
}
