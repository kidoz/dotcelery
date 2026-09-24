using DotCelery.Backend.Postgres.Migrations;

namespace DotCelery.Backend.Postgres.Outbox;

/// <summary>
/// Schema migrations for <see cref="PostgresOutboxStore"/>.
/// </summary>
public static class PostgresOutboxMigrations
{
    /// <summary>
    /// Creates the migration module for the tables configured in <paramref name="options"/>.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <returns>The migration module.</returns>
    public static PostgresMigrationModule CreateModule(PostgresOutboxStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new PostgresMigrationModule(
            $"outbox/{options.TableName}",
            options.ConnectionString,
            options.Schema,
            [
                new PostgresMigration(
                    1,
                    "Create outbox table",
                    [
                        $"""
                        CREATE SEQUENCE IF NOT EXISTS {options.Schema}.{options.TableName}_seq
                        """,
                        $"""
                        CREATE TABLE IF NOT EXISTS {options.Schema}.{options.TableName} (
                            id VARCHAR(255) PRIMARY KEY,
                            task_message JSONB NOT NULL,
                            status INTEGER NOT NULL DEFAULT 0,
                            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                            attempts INTEGER NOT NULL DEFAULT 0,
                            last_error TEXT,
                            dispatched_at TIMESTAMP WITH TIME ZONE,
                            sequence_number BIGINT NOT NULL DEFAULT nextval('{options.Schema}.{options.TableName}_seq')
                        )
                        """,
                        $"""
                        CREATE INDEX IF NOT EXISTS idx_{options.TableName}_status_seq
                            ON {options.Schema}.{options.TableName} (status, sequence_number)
                            WHERE status = 0
                        """,
                    ]
                ),
            ]
        );
    }
}
