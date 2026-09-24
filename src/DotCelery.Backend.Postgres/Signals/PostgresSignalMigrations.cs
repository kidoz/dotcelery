using DotCelery.Backend.Postgres.Migrations;

namespace DotCelery.Backend.Postgres.Signals;

/// <summary>
/// Schema migrations for <see cref="PostgresSignalStore"/>.
/// </summary>
public static class PostgresSignalMigrations
{
    /// <summary>
    /// Creates the migration module for the tables configured in <paramref name="options"/>.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <returns>The migration module.</returns>
    public static PostgresMigrationModule CreateModule(PostgresSignalStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new PostgresMigrationModule(
            $"signals/{options.TableName}",
            options.ConnectionString,
            options.Schema,
            [
                new PostgresMigration(
                    1,
                    "Create signal table",
                    [
                        $"""
                        CREATE TABLE IF NOT EXISTS {options.Schema}.{options.TableName} (
                            id VARCHAR(255) PRIMARY KEY,
                            signal_type TEXT NOT NULL,
                            task_id VARCHAR(255) NOT NULL,
                            task_name VARCHAR(512) NOT NULL,
                            payload TEXT NOT NULL,
                            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                            status INTEGER NOT NULL DEFAULT 0,
                            visible_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                        )
                        """,
                        $"""
                        CREATE INDEX IF NOT EXISTS idx_{options.TableName}_status_visible
                            ON {options.Schema}.{options.TableName} (status, visible_at, created_at)
                            WHERE status = 0
                        """,
                        $"""
                        CREATE INDEX IF NOT EXISTS idx_{options.TableName}_task_id
                            ON {options.Schema}.{options.TableName} (task_id)
                        """,
                    ]
                ),
            ]
        );
    }
}
