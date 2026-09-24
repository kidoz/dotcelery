using DotCelery.Storage.Sql.Migrations;

namespace DotCelery.Backend.Postgres.RateLimiting;

/// <summary>
/// Schema migrations for <see cref="PostgresRateLimiter"/>.
/// </summary>
public static class PostgresRateLimiterMigrations
{
    /// <summary>
    /// Creates the migration module for the tables configured in <paramref name="options"/>.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <returns>The migration module.</returns>
    public static SqlMigrationModule CreateModule(PostgresRateLimiterOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new SqlMigrationModule(
            $"rate-limits/{options.TableName}",
            options.ConnectionString,
            options.Schema,
            [
                new SqlMigration(
                    1,
                    "Create rate limit table",
                    [
                        new SchemaOperation.ExecuteSql(
                            $"""
                            CREATE TABLE IF NOT EXISTS {options.Schema}.{options.TableName} (
                                id BIGSERIAL PRIMARY KEY,
                                resource_key VARCHAR(255) NOT NULL,
                                timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                            )
                            """
                        ),
                        new SchemaOperation.ExecuteSql(
                            $"""
                            CREATE INDEX IF NOT EXISTS idx_{options.TableName}_resource_timestamp
                                ON {options.Schema}.{options.TableName} (resource_key, timestamp)
                            """
                        ),
                    ]
                ),
            ]
        );
    }
}
