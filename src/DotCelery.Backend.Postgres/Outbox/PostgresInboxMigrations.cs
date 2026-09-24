using DotCelery.Storage.Sql.Migrations;

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
    public static SqlMigrationModule CreateModule(PostgresInboxStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new SqlMigrationModule(
            $"inbox/{options.TableName}",
            options.ConnectionString,
            options.Schema,
            [
                new SqlMigration(
                    1,
                    "Create inbox table",
                    [
                        new SchemaOperation.ExecuteSql(
                            $"""
                            CREATE TABLE IF NOT EXISTS {options.Schema}.{options.TableName} (
                                message_id VARCHAR(255) PRIMARY KEY,
                                processed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                            )
                            """
                        ),
                        new SchemaOperation.ExecuteSql(
                            $"""
                            CREATE INDEX IF NOT EXISTS idx_{options.TableName}_processed_at
                                ON {options.Schema}.{options.TableName} (processed_at)
                            """
                        ),
                    ]
                ),
            ]
        );
    }
}
