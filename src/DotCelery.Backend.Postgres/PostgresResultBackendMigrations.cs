using DotCelery.Storage.Sql.Migrations;

namespace DotCelery.Backend.Postgres;

/// <summary>
/// Schema migrations for <see cref="PostgresResultBackend"/>.
/// </summary>
public static class PostgresResultBackendMigrations
{
    /// <summary>
    /// Creates the migration module for the table configured in <paramref name="options"/>.
    /// </summary>
    /// <param name="options">The backend options.</param>
    /// <returns>The migration module.</returns>
    public static SqlMigrationModule CreateModule(PostgresBackendOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new SqlMigrationModule(
            $"results/{options.TableName}",
            options.ConnectionString,
            options.Schema,
            [
                new SqlMigration(
                    1,
                    "Create task results table",
                    [
                        new SchemaOperation.ExecuteSql(
                            $"""
                            CREATE TABLE IF NOT EXISTS {options.Schema}.{options.TableName} (
                                task_id VARCHAR(255) PRIMARY KEY,
                                state VARCHAR(20) NOT NULL,
                                result BYTEA,
                                content_type VARCHAR(100),
                                exception JSONB,
                                completed_at TIMESTAMPTZ NOT NULL,
                                duration_ms BIGINT NOT NULL DEFAULT 0,
                                retries INT NOT NULL DEFAULT 0,
                                worker VARCHAR(255),
                                metadata JSONB,
                                expires_at TIMESTAMPTZ
                            )
                            """
                        ),
                        new SchemaOperation.ExecuteSql(
                            $"""
                            CREATE INDEX IF NOT EXISTS idx_{options.TableName}_expires_at
                                ON {options.Schema}.{options.TableName} (expires_at)
                                WHERE expires_at IS NOT NULL
                            """
                        ),
                    ]
                ),
            ]
        );
    }
}
