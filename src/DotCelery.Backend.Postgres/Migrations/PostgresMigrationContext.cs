using DotCelery.Core.Migrations;
using Npgsql;

namespace DotCelery.Backend.Postgres.Migrations;

/// <summary>
/// PostgreSQL implementation of <see cref="IMigrationContext"/>.
/// </summary>
public sealed class PostgresMigrationContext : IMigrationContext
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly string _schema;
    private readonly TimeSpan _commandTimeout;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresMigrationContext"/> class.
    /// </summary>
    /// <param name="dataSource">The Npgsql data source.</param>
    /// <param name="schema">The schema name.</param>
    /// <param name="commandTimeout">The command timeout.</param>
    public PostgresMigrationContext(
        NpgsqlDataSource dataSource,
        string schema = "public",
        TimeSpan? commandTimeout = null
    )
    {
        _dataSource = dataSource;
        _schema = schema;
        _commandTimeout = commandTimeout ?? TimeSpan.FromSeconds(30);
    }

    /// <inheritdoc />
    public async ValueTask ExecuteAsync(
        string command,
        CancellationToken cancellationToken = default
    )
    {
        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = command;
        cmd.CommandTimeout = (int)_commandTimeout.TotalSeconds;
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask ExecuteAsync(
        string command,
        IDictionary<string, object?> parameters,
        CancellationToken cancellationToken = default
    )
    {
        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = command;
        cmd.CommandTimeout = (int)_commandTimeout.TotalSeconds;

        foreach (var (name, value) in parameters)
        {
            cmd.Parameters.AddWithValue(name, value ?? DBNull.Value);
        }

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<bool> ExistsAsync(
        string name,
        CancellationToken cancellationToken = default
    )
    {
        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.CommandTimeout = (int)_commandTimeout.TotalSeconds;
        cmd.CommandText =
            @"
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )";
        cmd.Parameters.AddWithValue(_schema);
        cmd.Parameters.AddWithValue(name);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is true;
    }
}
