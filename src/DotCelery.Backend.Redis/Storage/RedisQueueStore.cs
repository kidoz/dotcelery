using DotCelery.Core.Storage;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.Storage;

/// <summary>
/// Redis <see cref="IQueueStore"/>. Each item is a hash; waiting items are in a sorted set by
/// due time and id, and claimed items in another by the time their claim expires. Items whose
/// claim expired move back to the waiting set before items are claimed.
/// </summary>
internal sealed class RedisQueueStore(RedisContext context) : IQueueStore
{
    // KEYS[1] is the queue's hash-tagged base key. An expired claim keeps its token until the
    // item is claimed again, so its holder can still complete or abandon it until then.
    private const string Functions = """
        local function item_key(id) return KEYS[1] .. ':item:' .. id end
        local function release_expired(now)
            local ids = redis.call('ZRANGEBYSCORE', KEYS[1] .. ':claimed', '-inf', now)
            for _, id in ipairs(ids) do
                redis.call('ZREM', KEYS[1] .. ':claimed', id)
                redis.call('ZADD', KEYS[1] .. ':ready', redis.call('HGET', item_key(id), 'due'), id)
            end
        end
        local function remove(id)
            redis.call('ZREM', KEYS[1] .. ':ready', id)
            redis.call('ZREM', KEYS[1] .. ':claimed', id)
            return redis.call('DEL', item_key(id))
        end

        """;

    // ARGV: id, payload, due
    private const string EnqueueScript =
        Functions
        + """
            remove(ARGV[1])
            redis.call('HSET', item_key(ARGV[1]), 'p', ARGV[2], 'due', ARGV[3], 'dc', 0)
            redis.call('ZADD', KEYS[1] .. ':ready', ARGV[3], ARGV[1])
            """;

    // ARGV: now, max items, claimed until, claim token
    private const string ClaimScript =
        Functions
        + """
            release_expired(ARGV[1])
            local result = {}
            local ids = redis.call('ZRANGEBYSCORE', KEYS[1] .. ':ready', '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
            for _, id in ipairs(ids) do
                local key = item_key(id)
                redis.call('ZREM', KEYS[1] .. ':ready', id)
                redis.call('ZADD', KEYS[1] .. ':claimed', ARGV[3], id)
                local count = redis.call('HINCRBY', key, 'dc', 1)
                redis.call('HSET', key, 'tok', ARGV[4], 'until', ARGV[3])
                local fields = redis.call('HMGET', key, 'p', 'due')
                table.insert(result, id)
                table.insert(result, fields[1])
                table.insert(result, fields[2])
                table.insert(result, count)
            end
            return result
            """;

    // ARGV: id, claim token
    private const string CompleteScript =
        Functions
        + """
            if redis.call('HGET', item_key(ARGV[1]), 'tok') ~= ARGV[2] then return 0 end
            remove(ARGV[1])
            return 1
            """;

    // ARGV: id, claim token, due or ''
    private const string AbandonScript =
        Functions
        + """
            local key = item_key(ARGV[1])
            if redis.call('HGET', key, 'tok') ~= ARGV[2] then return 0 end
            redis.call('HDEL', key, 'tok', 'until')
            local due = ARGV[3]
            if due == '' then due = redis.call('HGET', key, 'due') end
            redis.call('HSET', key, 'due', due)
            redis.call('ZREM', KEYS[1] .. ':claimed', ARGV[1])
            redis.call('ZADD', KEYS[1] .. ':ready', due, ARGV[1])
            return 1
            """;

    // ARGV: id
    private const string RemoveScript = Functions + "return remove(ARGV[1])";

    private const string CountScript =
        "return redis.call('ZCARD', KEYS[1] .. ':ready') + redis.call('ZCARD', KEYS[1] .. ':claimed')";

    // ARGV: now
    private const string NextDueScript =
        Functions
        + """
            release_expired(ARGV[1])
            local first = redis.call('ZRANGE', KEYS[1] .. ':ready', 0, 0, 'WITHSCORES')
            if #first == 0 then return false end
            return first[2]
            """;

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

        await context
            .EvaluateAsync(
                EnqueueScript,
                context.Keys.Queue(queue),
                [id, payload, RedisTime.ToMicroseconds(dueAt)],
                cancellationToken
            )
            .ConfigureAwait(false);
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

        var now = context.Now();
        var claimedUntil = now + RedisTime.ToMicroseconds(visibilityTimeout);
        var claimToken = Guid.NewGuid();
        var result = await context
            .EvaluateAsync(
                ClaimScript,
                context.Keys.Queue(queue),
                [now, maxItems, claimedUntil, claimToken.ToString("N")],
                cancellationToken
            )
            .ConfigureAwait(false);

        var values = (RedisResult[])result!;
        var items = new List<QueueItem>(values.Length / 4);
        for (var i = 0; i < values.Length; i += 4)
        {
            items.Add(
                new QueueItem(
                    queue,
                    (string)values[i]!,
                    (byte[])values[i + 1]!,
                    RedisTime.FromMicroseconds((long)values[i + 2]),
                    (int)values[i + 3],
                    claimToken,
                    RedisTime.FromMicroseconds(claimedUntil)
                )
            );
        }

        return items;
    }

    public async ValueTask<bool> CompleteAsync(
        QueueItem item,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(item);

        return (long)
                await context
                    .EvaluateAsync(
                        CompleteScript,
                        context.Keys.Queue(item.Queue),
                        [item.Id, item.ClaimToken.ToString("N")],
                        cancellationToken
                    )
                    .ConfigureAwait(false) == 1;
    }

    public async ValueTask<bool> AbandonAsync(
        QueueItem item,
        DateTimeOffset? dueAt = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(item);

        return (long)
                await context
                    .EvaluateAsync(
                        AbandonScript,
                        context.Keys.Queue(item.Queue),
                        [
                            item.Id,
                            item.ClaimToken.ToString("N"),
                            dueAt is { } due ? RedisTime.ToMicroseconds(due) : "",
                        ],
                        cancellationToken
                    )
                    .ConfigureAwait(false) == 1;
    }

    public async ValueTask<bool> RemoveAsync(
        string queue,
        string id,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(queue);
        StorageGuard.ThrowIfInvalidKey(id);

        return (long)
                await context
                    .EvaluateAsync(RemoveScript, context.Keys.Queue(queue), [id], cancellationToken)
                    .ConfigureAwait(false) == 1;
    }

    public async ValueTask<long> CountAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(queue);

        return (long)
            await context
                .EvaluateAsync(CountScript, context.Keys.Queue(queue), [], cancellationToken)
                .ConfigureAwait(false);
    }

    public async ValueTask<DateTimeOffset?> GetNextDueAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(queue);

        var result = await context
            .EvaluateAsync(
                NextDueScript,
                context.Keys.Queue(queue),
                [context.Now()],
                cancellationToken
            )
            .ConfigureAwait(false);
        return result.IsNull ? null : RedisTime.FromMicroseconds((long)result);
    }
}
