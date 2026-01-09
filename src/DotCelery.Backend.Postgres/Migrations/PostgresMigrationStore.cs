using DotCelery.Core.Migrations;
using Npgsql;

namespace DotCelery.Backend.Postgres.Migrations;

/// <summary>
/// PostgreSQL implementation of <see cref="IMigrationStore"/>.
/// Stores migration history in a database table.
/// </summary>
public sealed class PostgresMigrationStore : IMigrationStore
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly string _schema;
    private readonly string _tableName;
    private readonly TimeSpan _commandTimeout;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresMigrationStore"/> class.
    /// </summary>
    /// <param name="dataSource">The Npgsql data source.</param>
    /// <param name="schema">The schema name.</param>
    /// <param name="tableName">The migration history table name.</param>
    /// <param name="commandTimeout">The command timeout.</param>
    public PostgresMigrationStore(
        NpgsqlDataSource dataSource,
        string schema = "public",
        string tableName = "_migrations",
        TimeSpan? commandTimeout = null
    )
    {
        _dataSource = dataSource;
        _schema = schema;
        _tableName = tableName;
        _commandTimeout = commandTimeout ?? TimeSpan.FromSeconds(30);
    }

    private string FullTableName => $"{_schema}.{_tableName}";

    /// <inheritdoc />
    public async ValueTask InitializeAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.CommandTimeout = (int)_commandTimeout.TotalSeconds;
        cmd.CommandText =
            $@"
            CREATE TABLE IF NOT EXISTS {FullTableName} (
                version BIGINT PRIMARY KEY,
                description VARCHAR(500) NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )";

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<long>> GetAppliedVersionsAsync(
        CancellationToken cancellationToken = default
    )
    {
        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.CommandTimeout = (int)_commandTimeout.TotalSeconds;
        cmd.CommandText = $"SELECT version FROM {FullTableName} ORDER BY version";

        var versions = new List<long>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            versions.Add(reader.GetInt64(0));
        }

        return versions;
    }

    /// <inheritdoc />
    public async ValueTask MarkAppliedAsync(
        long version,
        string description,
        CancellationToken cancellationToken = default
    )
    {
        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.CommandTimeout = (int)_commandTimeout.TotalSeconds;
        cmd.CommandText =
            $@"
            INSERT INTO {FullTableName} (version, description, applied_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (version) DO NOTHING";
        cmd.Parameters.AddWithValue(version);
        cmd.Parameters.AddWithValue(description);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask MarkRolledBackAsync(
        long version,
        CancellationToken cancellationToken = default
    )
    {
        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.CommandTimeout = (int)_commandTimeout.TotalSeconds;
        cmd.CommandText = $"DELETE FROM {FullTableName} WHERE version = $1";
        cmd.Parameters.AddWithValue(version);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
