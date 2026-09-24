using DotCelery.Storage.Sql.Migrations;

namespace DotCelery.Backend.Postgres.Sagas;

/// <summary>
/// Schema migrations for <see cref="PostgresSagaStore"/>.
/// </summary>
public static class PostgresSagaMigrations
{
    /// <summary>
    /// Creates the migration module for the tables configured in <paramref name="options"/>.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <returns>The migration module.</returns>
    public static SqlMigrationModule CreateModule(PostgresSagaStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new SqlMigrationModule(
            $"sagas/{options.SagasTableName}",
            options.ConnectionString,
            options.Schema,
            [
                new SqlMigration(
                    1,
                    "Create saga tables",
                    [
                        new SchemaOperation.ExecuteSql(
                            $"""
                            CREATE TABLE IF NOT EXISTS {options.Schema}.{options.SagasTableName} (
                                id VARCHAR(255) PRIMARY KEY,
                                name VARCHAR(255) NOT NULL,
                                state VARCHAR(50) NOT NULL,
                                current_step_index INTEGER NOT NULL DEFAULT 0,
                                failure_reason TEXT,
                                correlation_id VARCHAR(255),
                                metadata JSONB,
                                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                                started_at TIMESTAMP WITH TIME ZONE,
                                completed_at TIMESTAMP WITH TIME ZONE
                            )
                            """
                        ),
                        new SchemaOperation.ExecuteSql(
                            $"""
                            CREATE INDEX IF NOT EXISTS idx_{options.SagasTableName}_state
                                ON {options.Schema}.{options.SagasTableName} (state)
                            """
                        ),
                        new SchemaOperation.ExecuteSql(
                            $"""
                            CREATE INDEX IF NOT EXISTS idx_{options.SagasTableName}_correlation_id
                                ON {options.Schema}.{options.SagasTableName} (correlation_id)
                                WHERE correlation_id IS NOT NULL
                            """
                        ),
                        new SchemaOperation.ExecuteSql(
                            $"""
                            CREATE TABLE IF NOT EXISTS {options.Schema}.{options.SagaStepsTableName} (
                                saga_id VARCHAR(255) NOT NULL,
                                id VARCHAR(255) NOT NULL,
                                name VARCHAR(255) NOT NULL,
                                step_order INTEGER NOT NULL,
                                execute_task JSONB NOT NULL,
                                compensate_task JSONB,
                                state VARCHAR(50) NOT NULL,
                                execute_task_id VARCHAR(255),
                                compensate_task_id VARCHAR(255),
                                result JSONB,
                                error TEXT,
                                started_at TIMESTAMP WITH TIME ZONE,
                                completed_at TIMESTAMP WITH TIME ZONE,
                                PRIMARY KEY (saga_id, id),
                                FOREIGN KEY (saga_id) REFERENCES {options.Schema}.{options.SagasTableName}(id) ON DELETE CASCADE
                            )
                            """
                        ),
                        new SchemaOperation.ExecuteSql(
                            $"""
                            CREATE TABLE IF NOT EXISTS {options.Schema}.{options.TaskSagaTableName} (
                                task_id VARCHAR(255) PRIMARY KEY,
                                saga_id VARCHAR(255) NOT NULL,
                                FOREIGN KEY (saga_id) REFERENCES {options.Schema}.{options.SagasTableName}(id) ON DELETE CASCADE
                            )
                            """
                        ),
                    ]
                ),
            ]
        );
    }
}
