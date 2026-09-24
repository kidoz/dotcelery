using DotCelery.Backend.Postgres.Migrations;

namespace DotCelery.Backend.Postgres.Revocation;

/// <summary>
/// Schema migrations for <see cref="PostgresRevocationStore"/>.
/// </summary>
public static class PostgresRevocationMigrations
{
    /// <summary>
    /// Creates the migration module for the tables configured in <paramref name="options"/>.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <returns>The migration module.</returns>
    public static PostgresMigrationModule CreateModule(PostgresRevocationStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new PostgresMigrationModule(
            $"revocations/{options.TableName}",
            options.ConnectionString,
            options.Schema,
            [
                new PostgresMigration(
                    1,
                    "Create revocation table",
                    [
                        $"""
                        CREATE TABLE IF NOT EXISTS {options.Schema}.{options.TableName} (
                            task_id VARCHAR(255) PRIMARY KEY,
                            terminate BOOLEAN NOT NULL DEFAULT FALSE,
                            signal VARCHAR(50) NOT NULL DEFAULT 'Graceful',
                            revoked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
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
