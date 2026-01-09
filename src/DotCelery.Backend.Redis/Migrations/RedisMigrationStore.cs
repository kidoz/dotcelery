using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Backend.Redis.Serialization;
using DotCelery.Core.Migrations;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.Migrations;

/// <summary>
/// Redis implementation of <see cref="IMigrationStore"/>.
/// Stores migration history in a Redis sorted set.
/// </summary>
public sealed class RedisMigrationStore : IMigrationStore
{
    private readonly IConnectionMultiplexer _connection;
    private readonly int _database;
    private readonly string _key;

    // AOT-friendly type info
    private static JsonTypeInfo<MigrationRecord> MigrationRecordTypeInfo =>
        RedisBackendJsonContext.Default.MigrationRecord;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisMigrationStore"/> class.
    /// </summary>
    /// <param name="connection">The Redis connection.</param>
    /// <param name="database">The database index.</param>
    /// <param name="key">The key for storing migration history.</param>
    public RedisMigrationStore(
        IConnectionMultiplexer connection,
        int database = 0,
        string key = "_migrations"
    )
    {
        _connection = connection;
        _database = database;
        _key = key;
    }

    /// <inheritdoc />
    public ValueTask InitializeAsync(CancellationToken cancellationToken = default)
    {
        // Redis doesn't need initialization - keys are created on first use
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<long>> GetAppliedVersionsAsync(
        CancellationToken cancellationToken = default
    )
    {
        var db = _connection.GetDatabase(_database);

        // Get all members from sorted set (versions are used as scores)
        var members = await db.SortedSetRangeByScoreAsync(_key).ConfigureAwait(false);

        var versions = new List<long>();
        foreach (var member in members)
        {
            var record = JsonSerializer.Deserialize(member.ToString(), MigrationRecordTypeInfo);
            if (record is not null)
            {
                versions.Add(record.Version);
            }
        }

        return versions.OrderBy(v => v).ToList();
    }

    /// <inheritdoc />
    public async ValueTask MarkAppliedAsync(
        long version,
        string description,
        CancellationToken cancellationToken = default
    )
    {
        var db = _connection.GetDatabase(_database);

        var record = new MigrationRecord
        {
            Version = version,
            Description = description,
            AppliedAt = DateTimeOffset.UtcNow,
        };

        var json = JsonSerializer.Serialize(record, MigrationRecordTypeInfo);

        // Use version as score for ordering, JSON as member
        await db.SortedSetAddAsync(_key, json, version).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask MarkRolledBackAsync(
        long version,
        CancellationToken cancellationToken = default
    )
    {
        var db = _connection.GetDatabase(_database);

        // Find and remove the entry with this version
        var members = await db.SortedSetRangeByScoreAsync(_key, version, version)
            .ConfigureAwait(false);

        foreach (var member in members)
        {
            await db.SortedSetRemoveAsync(_key, member).ConfigureAwait(false);
        }
    }
}
