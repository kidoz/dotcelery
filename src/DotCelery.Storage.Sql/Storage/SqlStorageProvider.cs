using System.Data.Common;
using DotCelery.Core.Storage;
using DotCelery.Storage.Sql.Execution;

namespace DotCelery.Storage.Sql.Storage;

/// <summary>
/// The storage primitives on a SQL database, implemented once over the statements of a
/// dialect. The tables are created by the migrations in <see cref="SqlStorageSchema"/>.
/// </summary>
public sealed class SqlStorageProvider : IStorageProvider, ITransactionalStorage
{
    private readonly SqlStorageStatements _statements;
    private readonly SqlExecutor _sql;
    private readonly TimeProvider _timeProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="SqlStorageProvider"/> class.
    /// </summary>
    /// <param name="name">The provider name, such as <c>PostgreSQL</c>.</param>
    /// <param name="statements">The statements of the database's dialect.</param>
    /// <param name="executor">Runs statements against the database.</param>
    /// <param name="timeProvider">The clock for expiry, due times, and leases.</param>
    /// <param name="notifications">The notification channel, if the database supports one.</param>
    public SqlStorageProvider(
        string name,
        SqlStorageStatements statements,
        SqlExecutor executor,
        TimeProvider? timeProvider = null,
        INotificationChannel? notifications = null
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(statements);
        ArgumentNullException.ThrowIfNull(executor);

        Name = name;
        _statements = statements;
        _sql = executor;
        _timeProvider = timeProvider ?? TimeProvider.System;

        Documents = new SqlDocumentStore(statements, executor, _timeProvider);
        Leases = new SqlLeaseStore(statements, executor, _timeProvider);
        Queues = new SqlQueueStore(statements, executor, _timeProvider);
        Counters = new SqlCounterStore(statements, executor, _timeProvider);
        Notifications = notifications;
    }

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public IDocumentStore Documents { get; }

    /// <inheritdoc />
    public ILeaseStore Leases { get; }

    /// <inheritdoc />
    public IQueueStore Queues { get; }

    /// <inheritdoc />
    public ICounterStore Counters { get; }

    /// <inheritdoc />
    public INotificationChannel? Notifications { get; }

    /// <inheritdoc />
    public bool CanWriteIn(object transaction) =>
        transaction is DbTransaction { Connection: not null };

    /// <inheritdoc />
    /// <remarks>
    /// The statements run on the transaction's connection and the caller commits or rolls back.
    /// A failing statement does not retry, so a deadlock reaches the caller's transaction for it
    /// to roll back.
    /// </remarks>
    public ValueTask WriteInAsync(
        object transaction,
        Func<CancellationToken, ValueTask> work,
        CancellationToken cancellationToken = default
    )
    {
        if (transaction is not DbTransaction { Connection: not null } dbTransaction)
        {
            throw new ArgumentException(
                "The transaction must be a database transaction with an open connection.",
                nameof(transaction)
            );
        }

        return _sql.InCallerTransactionAsync(dbTransaction, work, cancellationToken);
    }

    /// <inheritdoc />
    /// <remarks>
    /// The transaction runs on its own connection. A deadlock or another retryable failure rolls
    /// it back and runs the work again, so the work must be safe to repeat.
    /// </remarks>
    public ValueTask RunInTransactionAsync(
        Func<CancellationToken, ValueTask> work,
        CancellationToken cancellationToken = default
    ) => _sql.RunInTransactionAsync(work, cancellationToken);

    /// <inheritdoc />
    public async ValueTask<long> PurgeExpiredAsync(CancellationToken cancellationToken = default)
    {
        var now = _timeProvider.GetUtcNow();
        long purged = 0;

        foreach (var statement in _statements.Purge)
        {
            purged += await _sql.ExecuteAsync(
                    statement,
                    p => p.Timestamp("now", now),
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        return purged;
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsHealthyAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await _sql.QuerySingleAsync(
                    _statements.HealthCheck,
                    bind: null,
                    _ => true,
                    cancellationToken
                )
                .ConfigureAwait(false);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return false;
        }
    }
}
