using DotCelery.Core.Migrations;

namespace DotCelery.Backend.Postgres.Migrations.Outbox;

/// <summary>
/// Creates the celery_inbox table for exactly-once message processing.
/// </summary>
[Migration(20250109003, "Create celery_inbox table")]
#pragma warning disable CA1707 // Identifiers should not contain underscores
public sealed class M20250109003_CreateInboxTable : MigrationBase
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
            CREATE TABLE IF NOT EXISTS celery_inbox (
                message_id VARCHAR(255) PRIMARY KEY,
                processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
                cancellationToken
            )
            .ConfigureAwait(false);

        await context
            .ExecuteAsync(
                @"
            CREATE INDEX IF NOT EXISTS idx_celery_inbox_processed_at
            ON celery_inbox(processed_at)",
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
            .ExecuteAsync("DROP TABLE IF EXISTS celery_inbox", cancellationToken)
            .ConfigureAwait(false);
    }
}
