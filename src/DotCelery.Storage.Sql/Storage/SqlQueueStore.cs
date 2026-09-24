using DotCelery.Core.Storage;
using DotCelery.Storage.Sql.Execution;

namespace DotCelery.Storage.Sql.Storage;

/// <summary>
/// <see cref="IQueueStore"/> over <see cref="SqlStorageStatements"/>.
/// </summary>
internal sealed class SqlQueueStore : IQueueStore
{
    private readonly SqlStorageStatements _statements;
    private readonly SqlExecutor _sql;
    private readonly TimeProvider _timeProvider;

    public SqlQueueStore(
        SqlStorageStatements statements,
        SqlExecutor sql,
        TimeProvider timeProvider
    )
    {
        _statements = statements;
        _sql = sql;
        _timeProvider = timeProvider;
    }

    public async ValueTask EnqueueAsync(
        string queue,
        string id,
        ReadOnlyMemory<byte> payload,
        DateTimeOffset dueAt,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(queue);
        StorageGuard.ThrowIfInvalidKey(id);

        await _sql.ExecuteAsync(
                _statements.QueueEnqueue,
                p =>
                    p.Text("queue", queue)
                        .Text("id", id)
                        .Binary("payload", payload)
                        .Timestamp("due_at", dueAt),
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    public async ValueTask<IReadOnlyList<QueueItem>> ClaimAsync(
        string queue,
        int maxItems,
        TimeSpan visibilityTimeout,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(queue);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxItems, 1);
        StorageGuard.ThrowIfNotPositive(visibilityTimeout);
        var now = _timeProvider.GetUtcNow();

        var items = await _sql.QueryAsync(
                _statements.QueueClaim,
                p =>
                    p.Text("queue", queue)
                        .Timestamp("now", now)
                        .Integer32("max_items", maxItems)
                        .Uuid("claim_token", Guid.NewGuid())
                        .Timestamp("claimed_until", now + visibilityTimeout),
                r => new QueueItem(
                    queue,
                    r.GetString(0),
                    r.GetFieldValue<byte[]>(1),
                    r.GetFieldValue<DateTimeOffset>(2),
                    r.GetInt32(3),
                    r.GetGuid(4),
                    r.GetFieldValue<DateTimeOffset>(5)
                ),
                cancellationToken
            )
            .ConfigureAwait(false);

        // The database may return claimed rows in any order
        return [.. items.OrderBy(i => i.DueAt).ThenBy(i => i.Id, StringComparer.Ordinal)];
    }

    public async ValueTask<bool> CompleteAsync(
        QueueItem item,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(item);

        var completed = await _sql.ExecuteAsync(
                _statements.QueueComplete,
                p =>
                    p.Text("queue", item.Queue)
                        .Text("id", item.Id)
                        .Uuid("claim_token", item.ClaimToken),
                cancellationToken
            )
            .ConfigureAwait(false);

        return completed > 0;
    }

    public async ValueTask<bool> AbandonAsync(
        QueueItem item,
        DateTimeOffset? dueAt = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(item);

        var abandoned = await _sql.ExecuteAsync(
                _statements.QueueAbandon,
                p =>
                    p.Text("queue", item.Queue)
                        .Text("id", item.Id)
                        .Uuid("claim_token", item.ClaimToken)
                        .Timestamp("due_at", dueAt ?? item.DueAt),
                cancellationToken
            )
            .ConfigureAwait(false);

        return abandoned > 0;
    }

    public async ValueTask<bool> RemoveAsync(
        string queue,
        string id,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(queue);
        StorageGuard.ThrowIfInvalidKey(id);

        var removed = await _sql.ExecuteAsync(
                _statements.QueueRemove,
                p => p.Text("queue", queue).Text("id", id),
                cancellationToken
            )
            .ConfigureAwait(false);

        return removed > 0;
    }

    public async ValueTask<long> CountAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(queue);

        return await _sql.QuerySingleAsync(
                _statements.QueueCount,
                p => p.Text("queue", queue),
                r => r.GetInt64(0),
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    public async ValueTask<DateTimeOffset?> GetNextDueAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(queue);

        return await _sql.QuerySingleAsync(
                _statements.QueueNextDue,
                p => p.Text("queue", queue).Timestamp("now", _timeProvider.GetUtcNow()),
                r => SqlRead.NullableTimestamp(r, 0),
                cancellationToken
            )
            .ConfigureAwait(false);
    }
}
