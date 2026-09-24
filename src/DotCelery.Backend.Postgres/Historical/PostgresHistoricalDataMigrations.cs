using DotCelery.Backend.Postgres.Migrations;

namespace DotCelery.Backend.Postgres.Historical;

/// <summary>
/// Schema migrations for <see cref="PostgresHistoricalDataStore"/>.
/// </summary>
public static class PostgresHistoricalDataMigrations
{
    /// <summary>
    /// Creates the migration module for the tables configured in <paramref name="options"/>.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <returns>The migration module.</returns>
    public static PostgresMigrationModule CreateModule(PostgresHistoricalDataStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new PostgresMigrationModule(
            $"historical-data/{options.SnapshotsTableName}",
            options.ConnectionString,
            options.Schema,
            [
                new PostgresMigration(
                    1,
                    "Create metrics snapshot table",
                    [
                        $"""
                        CREATE TABLE IF NOT EXISTS {options.Schema}.{options.SnapshotsTableName} (
                            id VARCHAR(64) PRIMARY KEY,
                            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                            task_name VARCHAR(255),
                            total_processed BIGINT NOT NULL DEFAULT 0,
                            success_count BIGINT NOT NULL DEFAULT 0,
                            failure_count BIGINT NOT NULL DEFAULT 0,
                            retry_count BIGINT NOT NULL DEFAULT 0,
                            revoked_count BIGINT NOT NULL DEFAULT 0,
                            avg_execution_time_ms DOUBLE PRECISION,
                            queue VARCHAR(255)
                        )
                        """,
                        $"""
                        CREATE INDEX IF NOT EXISTS idx_{options.SnapshotsTableName}_timestamp
                            ON {options.Schema}.{options.SnapshotsTableName} (timestamp)
                        """,
                        $"""
                        CREATE INDEX IF NOT EXISTS idx_{options.SnapshotsTableName}_task_name
                            ON {options.Schema}.{options.SnapshotsTableName} (task_name)
                            WHERE task_name IS NOT NULL
                        """,
                    ]
                ),
            ]
        );
    }
}
