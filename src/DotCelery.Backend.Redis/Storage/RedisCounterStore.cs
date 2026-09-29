using System.Collections.Concurrent;
using System.Globalization;
using DotCelery.Core.Storage;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.Storage;

/// <summary>
/// Redis <see cref="ICounterStore"/>. A counter is a hash of its value and expiry; a window
/// is a sorted set of events by time.
/// </summary>
internal sealed class RedisCounterStore(RedisContext context) : ICounterStore
{
    private readonly ConcurrentDictionary<string, long> _windows = new(StringComparer.Ordinal);

    // KEYS[1] is the counter. An expired counter starts again from zero. ARGV: delta, now, expiry or ''
    private const string IncrementScript = """
        local exp = redis.call('HGET', KEYS[1], 'exp')
        if exp and tonumber(exp) <= tonumber(ARGV[2]) then redis.call('DEL', KEYS[1]) end
        if redis.call('EXISTS', KEYS[1]) == 0 and ARGV[3] ~= '' then
            redis.call('HSET', KEYS[1], 'exp', ARGV[3])
        end
        return redis.call('HINCRBY', KEYS[1], 'n', ARGV[1])
        """;

    // ARGV: now
    private const string DeleteScript = """
        local exp = redis.call('HGET', KEYS[1], 'exp')
        local live = redis.call('EXISTS', KEYS[1]) == 1 and (not exp or tonumber(exp) > tonumber(ARGV[1]))
        redis.call('DEL', KEYS[1])
        if live then return 1 end
        return 0
        """;

    // KEYS[1] is the window. Events at or before the window start have left it.
    // ARGV: now, window start, limit, event id
    private const string AddToWindowScript = """
        redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[2])
        local count = redis.call('ZCARD', KEYS[1])
        if count < tonumber(ARGV[3]) then
            redis.call('ZADD', KEYS[1], ARGV[1], ARGV[4])
            return {1, count + 1, false}
        end
        return {0, count, redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')[2]}
        """;

    // ARGV: window start
    private const string WindowScript = """
        local count = redis.call('ZCOUNT', KEYS[1], '(' .. ARGV[1], '+inf')
        local oldest = redis.call('ZRANGEBYSCORE', KEYS[1], '(' .. ARGV[1], '+inf', 'WITHSCORES', 'LIMIT', 0, 1)
        return {count, oldest[2] or false}
        """;

    // ARGV: window start
    private const string PurgeWindowScript = """
        local removed = redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
        return {removed, redis.call('ZCARD', KEYS[1])}
        """;

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
            await context
                .RegisterAsync(context.Keys.CounterRegistry, key, cancellationToken)
                .ConfigureAwait(false);
        }

        var now = context.Now();
        return (long)
            await context
                .EvaluateAsync(
                    IncrementScript,
                    context.Keys.Counter(key),
                    [
                        delta,
                        now,
                        timeToLive is { } lifetime ? now + RedisTime.ToMicroseconds(lifetime) : "",
                    ],
                    cancellationToken
                )
                .ConfigureAwait(false);
    }

    public async ValueTask<long> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        StorageGuard.ThrowIfInvalidKey(key);

        var database = await context.GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var fields = await database
            .HashGetAsync(context.Keys.Counter(key), [(RedisValue)"n", (RedisValue)"exp"])
            .ConfigureAwait(false);

        return fields[0].IsNull || (!fields[1].IsNull && (long)fields[1] <= context.Now())
            ? 0
            : (long)fields[0];
    }

    public async ValueTask<bool> DeleteAsync(
        string key,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidKey(key);

        return (long)
                await context
                    .EvaluateAsync(
                        DeleteScript,
                        context.Keys.Counter(key),
                        [context.Now()],
                        cancellationToken
                    )
                    .ConfigureAwait(false) == 1;
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

        var windowMicroseconds = RedisTime.ToMicroseconds(window);
        await RegisterWindowAsync(key, windowMicroseconds, cancellationToken).ConfigureAwait(false);

        var now = context.Now();
        var result = (RedisResult[])
            (
                await context
                    .EvaluateAsync(
                        AddToWindowScript,
                        context.Keys.Window(key),
                        [now, now - windowMicroseconds, limit, Guid.NewGuid().ToString("N")],
                        cancellationToken
                    )
                    .ConfigureAwait(false)
            )!;

        return (long)result[0] == 1
            ? new WindowResult(true, (long)result[1], null)
            : new WindowResult(
                false,
                (long)result[1],
                RedisTime.FromMicroseconds((long)result[2] + windowMicroseconds)
                    - RedisTime.FromMicroseconds(now)
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

        var result = (RedisResult[])
            (
                await context
                    .EvaluateAsync(
                        WindowScript,
                        context.Keys.Window(key),
                        [context.Now() - RedisTime.ToMicroseconds(window)],
                        cancellationToken
                    )
                    .ConfigureAwait(false)
            )!;

        return new WindowSnapshot(
            (long)result[0],
            result[1].IsNull ? null : RedisTime.FromMicroseconds((long)result[1])
        );
    }

    // Counters with a time to live, and events that have left their window
    public async ValueTask<long> PurgeExpiredAsync(CancellationToken cancellationToken)
    {
        var database = await context.GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var now = context.Now();
        long purged = 0;

        foreach (
            var key in await database
                .SetMembersAsync(context.Keys.CounterRegistry)
                .ConfigureAwait(false)
        )
        {
            var counterKey = context.Keys.Counter(key.ToString());
            var expiresAt = await database.HashGetAsync(counterKey, "exp").ConfigureAwait(false);
            if (expiresAt.IsNull || (long)expiresAt <= now)
            {
                if (
                    !expiresAt.IsNull
                    && await database.KeyDeleteAsync(counterKey).ConfigureAwait(false)
                )
                {
                    purged++;
                }

                await database
                    .SetRemoveAsync(context.Keys.CounterRegistry, key)
                    .ConfigureAwait(false);
            }
        }

        foreach (
            var entry in await database
                .HashGetAllAsync(context.Keys.WindowRegistry)
                .ConfigureAwait(false)
        )
        {
            var result = (RedisResult[])
                (
                    await context
                        .EvaluateAsync(
                            PurgeWindowScript,
                            context.Keys.Window(entry.Name.ToString()),
                            [now - (long)entry.Value],
                            cancellationToken
                        )
                        .ConfigureAwait(false)
                )!;
            purged += (long)result[0];
            if ((long)result[1] == 0)
            {
                await database
                    .HashDeleteAsync(context.Keys.WindowRegistry, entry.Name)
                    .ConfigureAwait(false);
            }
        }

        return purged;
    }

    // The window registry records each key's longest window, which a purge must keep
    private async Task RegisterWindowAsync(
        string key,
        long windowMicroseconds,
        CancellationToken cancellationToken
    )
    {
        if (_windows.TryGetValue(key, out var known) && known >= windowMicroseconds)
        {
            return;
        }

        var database = await context.GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var registered = await database
            .HashGetAsync(context.Keys.WindowRegistry, key)
            .ConfigureAwait(false);
        if (registered.IsNull || (long)registered < windowMicroseconds)
        {
            await database
                .HashSetAsync(
                    context.Keys.WindowRegistry,
                    key,
                    windowMicroseconds.ToString(CultureInfo.InvariantCulture)
                )
                .ConfigureAwait(false);
        }

        _windows[key] = Math.Max(windowMicroseconds, registered.IsNull ? 0 : (long)registered);
    }
}
