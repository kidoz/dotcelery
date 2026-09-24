using DotCelery.Storage.Sql.Migrations;

namespace DotCelery.Backend.Postgres.Metrics;

/// <summary>
/// Schema migrations for <see cref="PostgresQueueMetrics"/>.
/// </summary>
public static class PostgresQueueMetricsMigrations
{
    /// <summary>
    /// Creates the migration module for the tables configured in <paramref name="options"/>.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <returns>The migration module.</returns>
    public static SqlMigrationModule CreateModule(PostgresQueueMetricsOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new SqlMigrationModule(
            $"queue-metrics/{options.MetricsTableName}",
            options.ConnectionString,
            options.Schema,
            [
                new SqlMigration(
                    1,
                    "Create queue metrics tables",
                    [
                        new SchemaOperation.ExecuteSql(
                            $"""
                            CREATE TABLE IF NOT EXISTS {options.Schema}.{options.MetricsTableName} (
                                queue VARCHAR(255) PRIMARY KEY,
                                waiting_count BIGINT NOT NULL DEFAULT 0,
                                running_count BIGINT NOT NULL DEFAULT 0,
                                processed_count BIGINT NOT NULL DEFAULT 0,
                                success_count BIGINT NOT NULL DEFAULT 0,
                                failure_count BIGINT NOT NULL DEFAULT 0,
                                consumer_count INTEGER NOT NULL DEFAULT 0,
                                total_duration_ms BIGINT NOT NULL DEFAULT 0,
                                completed_count BIGINT NOT NULL DEFAULT 0,
                                last_enqueued_at TIMESTAMP WITH TIME ZONE,
                                last_completed_at TIMESTAMP WITH TIME ZONE
                            )
                            """
                        ),
                        new SchemaOperation.ExecuteSql(
                            $"""
                            CREATE TABLE IF NOT EXISTS {options.Schema}.{options.RunningTasksTableName} (
                                task_id VARCHAR(255) PRIMARY KEY,
                                queue VARCHAR(255) NOT NULL,
                                started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                            )
                            """
                        ),
                        new SchemaOperation.ExecuteSql(
                            $"""
                            CREATE INDEX IF NOT EXISTS idx_{options.RunningTasksTableName}_queue
                                ON {options.Schema}.{options.RunningTasksTableName} (queue)
                            """
                        ),
                    ]
                ),
            ]
        );
    }
}
