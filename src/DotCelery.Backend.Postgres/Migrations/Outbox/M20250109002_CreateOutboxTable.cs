using DotCelery.Core.Migrations;

namespace DotCelery.Backend.Postgres.Migrations.Outbox;

/// <summary>
/// Creates the celery_outbox table for transactional outbox pattern.
/// </summary>
[Migration(20250109002, "Create celery_outbox table")]
#pragma warning disable CA1707 // Identifiers should not contain underscores
public sealed class M20250109002_CreateOutboxTable : MigrationBase
#pragma warning restore CA1707
{
    /// <inheritdoc />
    public override async ValueTask UpAsync(
        IMigrationContext context,
        CancellationToken cancellationToken = default
    )
    {
        await context
            .ExecuteAsync("CREATE SEQUENCE IF NOT EXISTS celery_outbox_seq", cancellationToken)
            .ConfigureAwait(false);

        await context
            .ExecuteAsync(
                @"
            CREATE TABLE IF NOT EXISTS celery_outbox (
                id VARCHAR(255) PRIMARY KEY,
                task_message JSONB NOT NULL,
                status INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                dispatched_at TIMESTAMPTZ,
                sequence_number BIGINT NOT NULL DEFAULT nextval('celery_outbox_seq')
            )",
                cancellationToken
            )
            .ConfigureAwait(false);

        await context
            .ExecuteAsync(
                @"
            CREATE INDEX IF NOT EXISTS idx_celery_outbox_status_seq
            ON celery_outbox(status, sequence_number)
            WHERE status = 0",
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
            .ExecuteAsync("DROP TABLE IF EXISTS celery_outbox", cancellationToken)
            .ConfigureAwait(false);

        await context
            .ExecuteAsync("DROP SEQUENCE IF EXISTS celery_outbox_seq", cancellationToken)
            .ConfigureAwait(false);
    }
}
