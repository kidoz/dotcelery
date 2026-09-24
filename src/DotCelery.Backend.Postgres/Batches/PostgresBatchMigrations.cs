using DotCelery.Backend.Postgres.Migrations;

namespace DotCelery.Backend.Postgres.Batches;

/// <summary>
/// Schema migrations for <see cref="PostgresBatchStore"/>.
/// </summary>
public static class PostgresBatchMigrations
{
    /// <summary>
    /// Creates the migration module for the tables configured in <paramref name="options"/>.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <returns>The migration module.</returns>
    public static PostgresMigrationModule CreateModule(PostgresBatchStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new PostgresMigrationModule(
            $"batches/{options.BatchesTableName}",
            options.ConnectionString,
            options.Schema,
            [
                new PostgresMigration(
                    1,
                    "Create batch tables",
                    [
                        $"""
                        CREATE TABLE IF NOT EXISTS {options.Schema}.{options.BatchesTableName} (
                            id VARCHAR(255) PRIMARY KEY,
                            name VARCHAR(255),
                            state VARCHAR(50) NOT NULL,
                            callback_task_id VARCHAR(255),
                            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                            completed_at TIMESTAMP WITH TIME ZONE
                        )
                        """,
                        $"""
                        CREATE TABLE IF NOT EXISTS {options.Schema}.{options.BatchTasksTableName} (
                            batch_id VARCHAR(255) NOT NULL,
                            task_id VARCHAR(255) NOT NULL,
                            is_completed BOOLEAN NOT NULL DEFAULT FALSE,
                            is_failed BOOLEAN NOT NULL DEFAULT FALSE,
                            PRIMARY KEY (batch_id, task_id),
                            FOREIGN KEY (batch_id) REFERENCES {options.Schema}.{options.BatchesTableName}(id) ON DELETE CASCADE
                        )
                        """,
                        $"""
                        CREATE INDEX IF NOT EXISTS idx_{options.BatchTasksTableName}_task_id
                            ON {options.Schema}.{options.BatchTasksTableName} (task_id)
                        """,
                    ]
                ),
            ]
        );
    }
}
