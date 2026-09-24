using DotCelery.Backend.Postgres.Migrations;

namespace DotCelery.Backend.Postgres.Partitioning;

/// <summary>
/// Schema migrations for <see cref="PostgresPartitionLockStore"/>.
/// </summary>
public static class PostgresPartitionLockMigrations
{
    /// <summary>
    /// Creates the migration module for the tables configured in <paramref name="options"/>.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <returns>The migration module.</returns>
    public static PostgresMigrationModule CreateModule(PostgresPartitionLockStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new PostgresMigrationModule(
            $"partition-locks/{options.TableName}",
            options.ConnectionString,
            options.Schema,
            [
                new PostgresMigration(
                    1,
                    "Create partition lock table",
                    [
                        $"""
                        CREATE TABLE IF NOT EXISTS {options.Schema}.{options.TableName} (
                            partition_key VARCHAR(255) PRIMARY KEY,
                            task_id VARCHAR(255) NOT NULL,
                            acquired_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                            expires_at TIMESTAMP WITH TIME ZONE NOT NULL
                        )
                        """,
                        $"""
                        CREATE INDEX IF NOT EXISTS idx_{options.TableName}_expires_at
                            ON {options.Schema}.{options.TableName} (expires_at)
                        """,
                    ]
                ),
            ]
        );
    }
}
