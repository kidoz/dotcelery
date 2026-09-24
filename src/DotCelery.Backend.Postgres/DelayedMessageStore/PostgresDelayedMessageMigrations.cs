using DotCelery.Storage.Sql.Migrations;

namespace DotCelery.Backend.Postgres.DelayedMessageStore;

/// <summary>
/// Schema migrations for <see cref="PostgresDelayedMessageStore"/>.
/// </summary>
public static class PostgresDelayedMessageMigrations
{
    /// <summary>
    /// Creates the migration module for the tables configured in <paramref name="options"/>.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <returns>The migration module.</returns>
    public static SqlMigrationModule CreateModule(PostgresDelayedMessageStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new SqlMigrationModule(
            $"delayed-messages/{options.TableName}",
            options.ConnectionString,
            options.Schema,
            [
                new SqlMigration(
                    1,
                    "Create delayed message table",
                    [
                        new SchemaOperation.ExecuteSql(
                            $"""
                            CREATE TABLE IF NOT EXISTS {options.Schema}.{options.TableName} (
                                task_id VARCHAR(255) PRIMARY KEY,
                                message JSONB NOT NULL,
                                delivery_time TIMESTAMP WITH TIME ZONE NOT NULL,
                                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                            )
                            """
                        ),
                        new SchemaOperation.ExecuteSql(
                            $"""
                            CREATE INDEX IF NOT EXISTS idx_{options.TableName}_delivery_time
                                ON {options.Schema}.{options.TableName} (delivery_time)
                            """
                        ),
                    ]
                ),
            ]
        );
    }
}
