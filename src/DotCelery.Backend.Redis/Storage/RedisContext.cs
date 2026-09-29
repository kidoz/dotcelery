using System.Collections.Concurrent;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.Storage;

/// <summary>
/// What the Redis primitives share: the connection, key names, and clock.
/// </summary>
internal sealed class RedisContext(
    IRedisConnectionProvider connections,
    RedisStorageOptions options,
    TimeProvider timeProvider
)
{
    private readonly ConcurrentDictionary<(string Registry, string Member), bool> _registered =
        new();

    public RedisKeys Keys { get; } = new(options.KeyPrefix);

    public long Now() => RedisTime.ToMicroseconds(timeProvider.GetUtcNow());

    public async Task<IDatabase> GetDatabaseAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var connection = await connections.GetConnectionAsync(options).ConfigureAwait(false);
        return connection.GetDatabase(options.Database);
    }

    public async Task<ISubscriber> GetSubscriberAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var connection = await connections.GetConnectionAsync(options).ConfigureAwait(false);
        return connection.GetSubscriber();
    }

    public async Task<RedisResult> EvaluateAsync(
        string script,
        RedisKey key,
        RedisValue[] values,
        CancellationToken cancellationToken
    )
    {
        var database = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        return await database.ScriptEvaluateAsync(script, [key], values).ConfigureAwait(false);
    }

    // Registries only grow, so each process adds a member once
    public async Task RegisterAsync(
        string registry,
        string member,
        CancellationToken cancellationToken
    )
    {
        if (_registered.ContainsKey((registry, member)))
        {
            return;
        }

        var database = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        await database.SetAddAsync(registry, member).ConfigureAwait(false);
        _registered.TryAdd((registry, member), true);
    }
}
