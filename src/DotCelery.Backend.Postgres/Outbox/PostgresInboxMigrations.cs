using DotCelery.Backend.Postgres.Migrations;

namespace DotCelery.Backend.Postgres.Outbox;

/// <summary>
/// Schema migrations for <see cref="PostgresInboxStore"/>.
/// </summary>
public static class PostgresInboxMigrations
{
    /// <summary>
    /// Creates the migration module for the tables configured in <paramref name="options"/>.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <returns>The migration module.</returns>
    public static PostgresMigrationModule CreateModule(PostgresInboxStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new PostgresMigrationModule(
            $"inbox/{options.TableName}",
            options.ConnectionString,
            options.Schema,
            [
                new PostgresMigration(
                    1,
                    "Create inbox table",
                    [
                        $"""
                        CREATE TABLE IF NOT EXISTS {options.Schema}.{options.TableName} (
                            message_id VARCHAR(255) PRIMARY KEY,
                            processed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                        )
                        """,
                        $"""
                        CREATE INDEX IF NOT EXISTS idx_{options.TableName}_processed_at
                            ON {options.Schema}.{options.TableName} (processed_at)
                        """,
                    ]
                ),
            ]
        );
    }
}
