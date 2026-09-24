using DotCelery.Backend.Postgres.Migrations;

namespace DotCelery.Backend.Postgres.Execution;

/// <summary>
/// Schema migrations for <see cref="PostgresTaskExecutionTracker"/>.
/// </summary>
public static class PostgresTaskExecutionTrackerMigrations
{
    /// <summary>
    /// Creates the migration module for the tables configured in <paramref name="options"/>.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <returns>The migration module.</returns>
    public static PostgresMigrationModule CreateModule(PostgresTaskExecutionTrackerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new PostgresMigrationModule(
            $"execution-tracking/{options.TableName}",
            options.ConnectionString,
            options.Schema,
            [
                new PostgresMigration(
                    1,
                    "Create task execution table",
                    [
                        $"""
                        CREATE TABLE IF NOT EXISTS {options.Schema}.{options.TableName} (
                            lock_key VARCHAR(511) PRIMARY KEY,
                            task_id VARCHAR(255) NOT NULL,
                            execution_key VARCHAR(255),
                            started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
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
