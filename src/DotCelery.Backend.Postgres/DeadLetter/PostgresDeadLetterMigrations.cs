using DotCelery.Storage.Sql.Migrations;

namespace DotCelery.Backend.Postgres.DeadLetter;

/// <summary>
/// Schema migrations for <see cref="PostgresDeadLetterStore"/>.
/// </summary>
public static class PostgresDeadLetterMigrations
{
    /// <summary>
    /// Creates the migration module for the tables configured in <paramref name="options"/>.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <returns>The migration module.</returns>
    public static SqlMigrationModule CreateModule(PostgresDeadLetterStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new SqlMigrationModule(
            $"dead-letters/{options.TableName}",
            options.ConnectionString,
            options.Schema,
            [
                new SqlMigration(
                    1,
                    "Create dead letter table",
                    [
                        new SchemaOperation.ExecuteSql(
                            $"""
                            CREATE TABLE IF NOT EXISTS {options.Schema}.{options.TableName} (
                                id VARCHAR(255) PRIMARY KEY,
                                task_id VARCHAR(255) NOT NULL,
                                task_name VARCHAR(255) NOT NULL,
                                queue VARCHAR(255) NOT NULL,
                                reason VARCHAR(50) NOT NULL,
                                original_message BYTEA NOT NULL,
                                exception_message TEXT,
                                exception_type VARCHAR(500),
                                stack_trace TEXT,
                                retry_count INTEGER NOT NULL DEFAULT 0,
                                timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                                expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                                worker VARCHAR(255)
                            )
                            """
                        ),
                        new SchemaOperation.ExecuteSql(
                            $"""
                            CREATE INDEX IF NOT EXISTS idx_{options.TableName}_expires_at
                                ON {options.Schema}.{options.TableName} (expires_at)
                            """
                        ),
                        new SchemaOperation.ExecuteSql(
                            $"""
                            CREATE INDEX IF NOT EXISTS idx_{options.TableName}_timestamp
                                ON {options.Schema}.{options.TableName} (timestamp DESC)
                            """
                        ),
                        new SchemaOperation.ExecuteSql(
                            $"""
                            CREATE INDEX IF NOT EXISTS idx_{options.TableName}_task_id
                                ON {options.Schema}.{options.TableName} (task_id)
                            """
                        ),
                    ]
                ),
            ]
        );
    }
}
