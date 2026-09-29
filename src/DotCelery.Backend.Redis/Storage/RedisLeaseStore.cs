using System.Runtime.CompilerServices;
using System.Text;
using DotCelery.Core.Storage;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.Storage;

/// <summary>
/// Redis <see cref="ILeaseStore"/>. Each lease is a hash; all leases share one hash tag, so a
/// sorted set can list them by key and one counter issues every fencing token.
/// </summary>
internal sealed class RedisLeaseStore(RedisContext context) : ILeaseStore
{
    // KEYS[1] is the leases' hash-tagged base key
    private const string Functions = """
        local function lease_key(key) return KEYS[1] .. ':lease:' .. key end
        local function read(key)
            local f = redis.call('HMGET', lease_key(key), 'owner', 'token', 'acq', 'exp')
            return {key, f[1], f[2], f[3], f[4]}
        end
        local function remove(key)
            redis.call('DEL', lease_key(key))
            redis.call('ZREM', KEYS[1] .. ':keys', key)
            redis.call('ZREM', KEYS[1] .. ':exp', key)
        end
        local function set_expiry(key, exp)
            redis.call('HSET', lease_key(key), 'exp', exp)
            redis.call('ZADD', KEYS[1] .. ':exp', exp, key)
        end

        """;

    // The same owner keeps its token and acquisition time while its lease is live; any new
    // holder gets new ones. ARGV: key, owner, now, expiry
    private const string AcquireScript =
        Functions
        + """
            local lease = read(ARGV[1])
            local live = lease[5] and tonumber(lease[5]) > tonumber(ARGV[3])
            if live and lease[2] ~= ARGV[2] then return false end
            if not live then
                local token = redis.call('INCR', KEYS[1] .. ':token')
                redis.call('HSET', lease_key(ARGV[1]), 'owner', ARGV[2], 'token', token, 'acq', ARGV[3])
                redis.call('ZADD', KEYS[1] .. ':keys', 0, ARGV[1])
            end
            set_expiry(ARGV[1], ARGV[4])
            return read(ARGV[1])
            """;

    // ARGV: key, token, now, expiry
    private const string RenewScript =
        Functions
        + """
            local lease = read(ARGV[1])
            if lease[3] ~= ARGV[2] or tonumber(lease[5]) <= tonumber(ARGV[3]) then return false end
            set_expiry(ARGV[1], ARGV[4])
            return read(ARGV[1])
            """;

    // ARGV: key, token
    private const string ReleaseScript =
        Functions
        + """
            if redis.call('HGET', lease_key(ARGV[1]), 'token') ~= ARGV[2] then return 0 end
            remove(ARGV[1])
            return 1
            """;

    // ARGV: min, max, now
    private const string ListScript =
        Functions
        + """
            local result = {}
            for _, key in ipairs(redis.call('ZRANGEBYLEX', KEYS[1] .. ':keys', ARGV[1], ARGV[2])) do
                local lease = read(key)
                if lease[5] and tonumber(lease[5]) > tonumber(ARGV[3]) then
                    for i = 1, 5 do table.insert(result, lease[i]) end
                end
            end
            return result
            """;

    // ARGV: now
    private const string PurgeScript =
        Functions
        + """
            local keys = redis.call('ZRANGEBYSCORE', KEYS[1] .. ':exp', '-inf', ARGV[1])
            for _, key in ipairs(keys) do remove(key) end
            return #keys
            """;

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

        var now = context.Now();
        return ToLease(
            await context
                .EvaluateAsync(
                    AcquireScript,
                    context.Keys.Leases,
                    [key, owner, now, now + RedisTime.ToMicroseconds(duration)],
                    cancellationToken
                )
                .ConfigureAwait(false)
        );
    }

    public async ValueTask<Lease?> RenewAsync(
        Lease lease,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(lease);
        StorageGuard.ThrowIfNotPositive(duration);

        var now = context.Now();
        return ToLease(
            await context
                .EvaluateAsync(
                    RenewScript,
                    context.Keys.Leases,
                    [lease.Key, lease.Token, now, now + RedisTime.ToMicroseconds(duration)],
                    cancellationToken
                )
                .ConfigureAwait(false)
        );
    }

    public async ValueTask<bool> ReleaseAsync(
        Lease lease,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(lease);

        return (long)
                await context
                    .EvaluateAsync(
                        ReleaseScript,
                        context.Keys.Leases,
                        [lease.Key, lease.Token],
                        cancellationToken
                    )
                    .ConfigureAwait(false) == 1;
    }

    public async ValueTask<Lease?> GetAsync(
        string key,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidKey(key);

        var database = await context.GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var fields = await database
            .HashGetAsync($"{context.Keys.Leases}:lease:{key}", Fields)
            .ConfigureAwait(false);

        return fields[3].IsNull || (long)fields[3] <= context.Now()
            ? null
            : new Lease(
                key,
                fields[0].ToString(),
                (long)fields[1],
                RedisTime.FromMicroseconds((long)fields[2]),
                RedisTime.FromMicroseconds((long)fields[3])
            );
    }

    public async IAsyncEnumerable<Lease> ListAsync(
        string keyPrefix,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidPrefix(keyPrefix);

        // Keys are compared as UTF-8 bytes, which never include 0xFF
        var (min, max) =
            keyPrefix.Length == 0
                ? ((RedisValue)"-", (RedisValue)"+")
                : (
                    (RedisValue)("[" + keyPrefix),
                    (RedisValue)(byte[])[.. Encoding.UTF8.GetBytes("[" + keyPrefix), 0xFF]
                );
        var result = await context
            .EvaluateAsync(
                ListScript,
                context.Keys.Leases,
                [min, max, context.Now()],
                cancellationToken
            )
            .ConfigureAwait(false);

        var values = (RedisResult[])result!;
        for (var i = 0; i < values.Length; i += 5)
        {
            yield return ToLease(values.AsSpan(i, 5).ToArray())!;
        }
    }

    public async ValueTask<long> PurgeExpiredAsync(CancellationToken cancellationToken) =>
        (long)
            await context
                .EvaluateAsync(PurgeScript, context.Keys.Leases, [context.Now()], cancellationToken)
                .ConfigureAwait(false);

    private static readonly RedisValue[] Fields = ["owner", "token", "acq", "exp"];

    private static Lease? ToLease(RedisResult result) =>
        result.IsNull ? null : ToLease((RedisResult[])result!);

    private static Lease? ToLease(RedisResult[] fields) =>
        new(
            (string)fields[0]!,
            (string)fields[1]!,
            (long)fields[2],
            RedisTime.FromMicroseconds((long)fields[3]),
            RedisTime.FromMicroseconds((long)fields[4])
        );
}
