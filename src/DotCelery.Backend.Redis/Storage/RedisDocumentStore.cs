using System.Runtime.CompilerServices;
using DotCelery.Core.Storage;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.Storage;

/// <summary>
/// Redis <see cref="IDocumentStore"/>. Each document is a hash; sorted sets order the
/// collection and each index key by sort key and id, and track expiry.
/// </summary>
internal sealed class RedisDocumentStore(RedisContext context) : IDocumentStore
{
    // KEYS[1] is the collection's hash-tagged base key. A document without a sort key scores
    // -inf, so it comes first and never matches a range. Expired documents are removed before
    // a query or count reads the sorted sets.
    private const string Functions = """
        local function doc_key(id) return KEYS[1] .. ':doc:' .. id end
        local function live(key, now)
            local exp = redis.call('HGET', key, 'exp')
            if exp == false then return redis.call('EXISTS', key) == 1 end
            return tonumber(exp) > tonumber(now)
        end
        local function remove_doc(id)
            local key = doc_key(id)
            local ik = redis.call('HGET', key, 'ik')
            redis.call('DEL', key)
            redis.call('ZREM', KEYS[1] .. ':all', id)
            redis.call('ZREM', KEYS[1] .. ':exp', id)
            if ik then redis.call('ZREM', KEYS[1] .. ':ik:' .. ik, id) end
        end
        local function purge(now)
            local ids = redis.call('ZRANGEBYSCORE', KEYS[1] .. ':exp', '-inf', now)
            for _, id in ipairs(ids) do remove_doc(id) end
            return #ids
        end
        local function write_doc(id, value, ik, sk, exp)
            local key = doc_key(id)
            if redis.call('EXISTS', key) == 1 then remove_doc(id) end
            local version = redis.call('INCR', KEYS[1] .. ':ver')
            redis.call('HSET', key, 'v', value, 'ver', version)
            local score = '-inf'
            if sk ~= '' then
                redis.call('HSET', key, 'sk', sk)
                score = sk
            end
            redis.call('ZADD', KEYS[1] .. ':all', score, id)
            if ik ~= '' then
                redis.call('HSET', key, 'ik', ik)
                redis.call('ZADD', KEYS[1] .. ':ik:' .. ik, score, id)
            end
            if exp ~= '' then
                redis.call('HSET', key, 'exp', exp)
                redis.call('ZADD', KEYS[1] .. ':exp', exp, id)
            end
            return version
        end
        local function members(now, index)
            purge(now)
            if index ~= '' then return KEYS[1] .. ':ik:' .. index end
            return KEYS[1] .. ':all'
        end

        """;

    // ARGV: id, value, index key, sort key, expiry, now
    private const string InsertScript =
        Functions
        + """
            if live(doc_key(ARGV[1]), ARGV[6]) then return false end
            return write_doc(ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5])
            """;

    // ARGV: id, value, index key, sort key, expiry, now, expected version
    private const string ReplaceScript =
        Functions
        + """
            local key = doc_key(ARGV[1])
            if not live(key, ARGV[6]) then return false end
            if redis.call('HGET', key, 'ver') ~= ARGV[7] then return false end
            return write_doc(ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5])
            """;

    // ARGV: id, value, index key, sort key, expiry
    private const string UpsertScript =
        Functions + "return write_doc(ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5])";

    // ARGV: id, now, expected version or ''
    private const string DeleteScript =
        Functions
        + """
            local key = doc_key(ARGV[1])
            if not live(key, ARGV[2]) then
                if redis.call('EXISTS', key) == 1 then remove_doc(ARGV[1]) end
                return 0
            end
            if ARGV[3] ~= '' and redis.call('HGET', key, 'ver') ~= ARGV[3] then return 0 end
            remove_doc(ARGV[1])
            return 1
            """;

    // ARGV: now, min, max, descending, offset, limit, index key
    private const string QueryScript =
        Functions
        + """
            local set = members(ARGV[1], ARGV[7])
            local ids
            if ARGV[4] == '1' then
                ids = redis.call('ZREVRANGEBYSCORE', set, ARGV[3], ARGV[2], 'LIMIT', ARGV[5], ARGV[6])
            else
                ids = redis.call('ZRANGEBYSCORE', set, ARGV[2], ARGV[3], 'LIMIT', ARGV[5], ARGV[6])
            end
            local result = {}
            for _, id in ipairs(ids) do
                local fields = redis.call('HMGET', doc_key(id), 'v', 'ver', 'ik', 'sk', 'exp')
                table.insert(result, id)
                for i = 1, 5 do table.insert(result, fields[i]) end
            end
            return result
            """;

    // ARGV: now, min, max, index key
    private const string CountScript =
        Functions + "return redis.call('ZCOUNT', members(ARGV[1], ARGV[4]), ARGV[2], ARGV[3])";

    // ARGV: now, min, max, index key
    private const string DeleteManyScript =
        Functions
        + """
            local ids = redis.call('ZRANGEBYSCORE', members(ARGV[1], ARGV[4]), ARGV[2], ARGV[3])
            for _, id in ipairs(ids) do remove_doc(id) end
            return #ids
            """;

    // ARGV: now
    private const string PurgeScript = Functions + "return purge(ARGV[1])";

    private const int FieldsPerDocument = 6;

    private static readonly RedisValue[] Fields = ["v", "ver", "ik", "sk", "exp"];

    public async ValueTask<StoredDocument?> GetAsync(
        string collection,
        string id,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(collection);
        StorageGuard.ThrowIfInvalidKey(id);

        var database = await context.GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var fields = await database
            .HashGetAsync($"{context.Keys.Collection(collection)}:doc:{id}", Fields)
            .ConfigureAwait(false);
        if (fields[0].IsNull)
        {
            return null;
        }

        var document = ToDocument(id, fields[0], fields[1], fields[2], fields[3], fields[4]);
        return
            document.ExpiresAt is { } expiresAt
            && RedisTime.ToMicroseconds(expiresAt) <= context.Now()
            ? null
            : document;
    }

    public async ValueTask<long?> TryInsertAsync(
        string collection,
        string id,
        ReadOnlyMemory<byte> value,
        DocumentWriteOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        var result = await WriteAsync(
                InsertScript,
                collection,
                id,
                value,
                options,
                [],
                cancellationToken
            )
            .ConfigureAwait(false);
        return result.IsNull ? null : (long)result;
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
        var result = await WriteAsync(
                ReplaceScript,
                collection,
                id,
                value,
                options,
                [expectedVersion],
                cancellationToken
            )
            .ConfigureAwait(false);
        return result.IsNull ? null : (long)result;
    }

    public async ValueTask<long> UpsertAsync(
        string collection,
        string id,
        ReadOnlyMemory<byte> value,
        DocumentWriteOptions? options = null,
        CancellationToken cancellationToken = default
    ) =>
        (long)
            await WriteAsync(UpsertScript, collection, id, value, options, [], cancellationToken)
                .ConfigureAwait(false);

    public async ValueTask<bool> DeleteAsync(
        string collection,
        string id,
        long? expectedVersion = null,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(collection);
        StorageGuard.ThrowIfInvalidKey(id);

        var result = await context
            .EvaluateAsync(
                DeleteScript,
                context.Keys.Collection(collection),
                [id, context.Now(), expectedVersion is { } version ? version : ""],
                cancellationToken
            )
            .ConfigureAwait(false);
        return (long)result == 1;
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

        var (min, max) = Range(filter);
        var result = await context
            .EvaluateAsync(
                QueryScript,
                context.Keys.Collection(collection),
                [
                    context.Now(),
                    min,
                    max,
                    page?.Descending == true ? "1" : "0",
                    page?.Offset ?? 0,
                    page?.Limit ?? -1,
                    filter.IndexKey ?? "",
                ],
                cancellationToken
            )
            .ConfigureAwait(false);

        var values = (RedisResult[])result!;
        for (var i = 0; i < values.Length; i += FieldsPerDocument)
        {
            yield return ToDocument(
                (string)values[i]!,
                (RedisValue)values[i + 1],
                (RedisValue)values[i + 2],
                (RedisValue)values[i + 3],
                (RedisValue)values[i + 4],
                (RedisValue)values[i + 5]
            );
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

        var (min, max) = Range(filter);
        return (long)
            await context
                .EvaluateAsync(
                    CountScript,
                    context.Keys.Collection(collection),
                    [context.Now(), min, max, filter.IndexKey ?? ""],
                    cancellationToken
                )
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

        var (min, max) = Range(filter);
        return (long)
            await context
                .EvaluateAsync(
                    DeleteManyScript,
                    context.Keys.Collection(collection),
                    [context.Now(), min, max, filter.IndexKey ?? ""],
                    cancellationToken
                )
                .ConfigureAwait(false);
    }

    public async ValueTask<long> PurgeExpiredAsync(
        string collection,
        CancellationToken cancellationToken
    ) =>
        (long)
            await context
                .EvaluateAsync(
                    PurgeScript,
                    context.Keys.Collection(collection),
                    [context.Now()],
                    cancellationToken
                )
                .ConfigureAwait(false);

    // Without bounds, documents without a sort key (scored -inf) match; any bound excludes them
    private static (string Min, string Max) Range(DocumentFilter filter)
    {
        if (filter.SortKeyFrom is null && filter.SortKeyBefore is null)
        {
            return ("-inf", "+inf");
        }

        return (
            filter.SortKeyFrom is { } from
                ? RedisTime.Format(RedisTime.ToMicroseconds(from))
                : "(-inf",
            filter.SortKeyBefore is { } before
                ? "(" + RedisTime.Format(RedisTime.ToMicroseconds(before))
                : "+inf"
        );
    }

    private static StoredDocument ToDocument(
        string id,
        RedisValue value,
        RedisValue version,
        RedisValue indexKey,
        RedisValue sortKey,
        RedisValue expiresAt
    ) =>
        new(
            id,
            (byte[])value!,
            (long)version,
            indexKey.IsNull ? null : indexKey.ToString(),
            sortKey.IsNull ? null : RedisTime.FromMicroseconds((long)sortKey),
            expiresAt.IsNull ? null : RedisTime.FromMicroseconds((long)expiresAt)
        );

    private async Task<RedisResult> WriteAsync(
        string script,
        string collection,
        string id,
        ReadOnlyMemory<byte> value,
        DocumentWriteOptions? options,
        RedisValue[] extra,
        CancellationToken cancellationToken
    )
    {
        StorageGuard.ThrowIfInvalidName(collection);
        StorageGuard.ThrowIfInvalidKey(id);
        StorageGuard.ThrowIfInvalid(options);

        var now = context.Now();
        var expiresAt = options?.TimeToLive is { } timeToLive
            ? RedisTime.Format(now + RedisTime.ToMicroseconds(timeToLive))
            : "";
        if (expiresAt.Length > 0)
        {
            await context
                .RegisterAsync(context.Keys.CollectionRegistry, collection, cancellationToken)
                .ConfigureAwait(false);
        }

        return await context
            .EvaluateAsync(
                script,
                context.Keys.Collection(collection),
                [
                    id,
                    value,
                    options?.IndexKey ?? "",
                    options?.SortKey is { } sortKey
                        ? RedisTime.Format(RedisTime.ToMicroseconds(sortKey))
                        : "",
                    expiresAt,
                    now,
                    .. extra,
                ],
                cancellationToken
            )
            .ConfigureAwait(false);
    }
}
