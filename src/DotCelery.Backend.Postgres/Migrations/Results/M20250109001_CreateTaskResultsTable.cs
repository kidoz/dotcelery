using DotCelery.Core.Migrations;

namespace DotCelery.Backend.Postgres.Migrations.Results;

/// <summary>
/// Creates the celery_task_results table for storing task execution results.
/// </summary>
[Migration(20250109001, "Create celery_task_results table")]
#pragma warning disable CA1707 // Identifiers should not contain underscores
public sealed class M20250109001_CreateTaskResultsTable : MigrationBase
#pragma warning restore CA1707
{
    /// <inheritdoc />
    public override async ValueTask UpAsync(
        IMigrationContext context,
        CancellationToken cancellationToken = default
    )
    {
        await context
            .ExecuteAsync(
                @"
            CREATE TABLE IF NOT EXISTS celery_task_results (
                task_id VARCHAR(255) PRIMARY KEY,
                state VARCHAR(20) NOT NULL,
                result BYTEA,
                content_type VARCHAR(100),
                exception JSONB,
                completed_at TIMESTAMPTZ NOT NULL,
                duration_ms BIGINT NOT NULL DEFAULT 0,
                retries INT NOT NULL DEFAULT 0,
                worker VARCHAR(255),
                metadata JSONB,
                expires_at TIMESTAMPTZ
            )",
                cancellationToken
            )
            .ConfigureAwait(false);

        await context
            .ExecuteAsync(
                @"
            CREATE INDEX IF NOT EXISTS idx_celery_task_results_expires_at
            ON celery_task_results(expires_at)
            WHERE expires_at IS NOT NULL",
                cancellationToken
            )
            .ConfigureAwait(false);

        await context
            .ExecuteAsync(
                @"
            CREATE INDEX IF NOT EXISTS idx_celery_task_results_state
            ON celery_task_results(state)",
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override async ValueTask DownAsync(
        IMigrationContext context,
        CancellationToken cancellationToken = default
    )
    {
        await context
            .ExecuteAsync("DROP TABLE IF EXISTS celery_task_results", cancellationToken)
            .ConfigureAwait(false);
    }
}
