using DotCelery.Core.Migrations;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.Migrations;

/// <summary>
/// Redis implementation of <see cref="IMigrationContext"/>.
/// Supports Lua scripts and key operations.
/// </summary>
public sealed class RedisMigrationContext : IMigrationContext
{
    private readonly IConnectionMultiplexer _connection;
    private readonly int _database;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisMigrationContext"/> class.
    /// </summary>
    /// <param name="connection">The Redis connection.</param>
    /// <param name="database">The database index.</param>
    public RedisMigrationContext(IConnectionMultiplexer connection, int database = 0)
    {
        _connection = connection;
        _database = database;
    }

    /// <summary>
    /// Gets the Redis database for direct operations.
    /// </summary>
    public IDatabase Database => _connection.GetDatabase(_database);

    /// <inheritdoc />
    public async ValueTask ExecuteAsync(
        string command,
        CancellationToken cancellationToken = default
    )
    {
        // For Redis, commands are Lua scripts
        var db = _connection.GetDatabase(_database);
        await db.ScriptEvaluateAsync(command).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask ExecuteAsync(
        string command,
        IDictionary<string, object?> parameters,
        CancellationToken cancellationToken = default
    )
    {
        var db = _connection.GetDatabase(_database);

        // Convert parameters to Redis keys and values
        var keys = new List<RedisKey>();
        var values = new List<RedisValue>();

        foreach (var (name, value) in parameters)
        {
            if (name.StartsWith("key:", StringComparison.OrdinalIgnoreCase))
            {
                keys.Add(new RedisKey(value?.ToString() ?? string.Empty));
            }
            else
            {
                values.Add(value?.ToString() ?? RedisValue.Null);
            }
        }

        await db.ScriptEvaluateAsync(command, keys.ToArray(), values.ToArray())
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<bool> ExistsAsync(
        string name,
        CancellationToken cancellationToken = default
    )
    {
        var db = _connection.GetDatabase(_database);
        return await db.KeyExistsAsync(name).ConfigureAwait(false);
    }
}
