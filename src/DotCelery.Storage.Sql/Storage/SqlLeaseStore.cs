using System.Data.Common;
using System.Runtime.CompilerServices;
using DotCelery.Core.Storage;
using DotCelery.Storage.Sql.Execution;

namespace DotCelery.Storage.Sql.Storage;

/// <summary>
/// <see cref="ILeaseStore"/> over <see cref="SqlStorageStatements"/>.
/// </summary>
internal sealed class SqlLeaseStore : ILeaseStore
{
    private readonly SqlStorageStatements _statements;
    private readonly SqlExecutor _sql;
    private readonly TimeProvider _timeProvider;

    public SqlLeaseStore(
        SqlStorageStatements statements,
        SqlExecutor sql,
        TimeProvider timeProvider
    )
    {
        _statements = statements;
        _sql = sql;
        _timeProvider = timeProvider;
    }

    public async ValueTask<Lease?> TryAcquireAsync(
        string key,
        string owner,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidKey(key);
        StorageGuard.ThrowIfInvalidKey(owner);
        StorageGuard.ThrowIfNotPositive(duration);
        var now = _timeProvider.GetUtcNow();

        return await _sql.QuerySingleAsync(
                _statements.LeaseAcquire,
                p =>
                    p.Text("key", key)
                        .Text("owner", owner)
                        .Timestamp("expires_at", now + duration)
                        .Timestamp("now", now),
                ReadLease,
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    public async ValueTask<Lease?> RenewAsync(
        Lease lease,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(lease);
        StorageGuard.ThrowIfNotPositive(duration);
        var now = _timeProvider.GetUtcNow();

        return await _sql.QuerySingleAsync(
                _statements.LeaseRenew,
                p =>
                    p.Text("key", lease.Key)
                        .Integer64("token", lease.Token)
                        .Timestamp("expires_at", now + duration)
                        .Timestamp("now", now),
                ReadLease,
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    public async ValueTask<bool> ReleaseAsync(
        Lease lease,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(lease);

        var released = await _sql.ExecuteAsync(
                _statements.LeaseRelease,
                p => p.Text("key", lease.Key).Integer64("token", lease.Token),
                cancellationToken
            )
            .ConfigureAwait(false);

        return released > 0;
    }

    public async ValueTask<Lease?> GetAsync(
        string key,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidKey(key);

        return await _sql.QuerySingleAsync(
                _statements.LeaseGet,
                p => p.Text("key", key).Timestamp("now", _timeProvider.GetUtcNow()),
                ReadLease,
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    public IAsyncEnumerable<Lease> ListAsync(
        string keyPrefix,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidPrefix(keyPrefix);

        return ListIteratorAsync(keyPrefix, cancellationToken);
    }

    private async IAsyncEnumerable<Lease> ListIteratorAsync(
        string keyPrefix,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        var leases = await _sql.QueryAsync(
                _statements.LeaseList,
                p => p.Text("prefix", keyPrefix).Timestamp("now", _timeProvider.GetUtcNow()),
                ReadLease,
                cancellationToken
            )
            .ConfigureAwait(false);

        foreach (var lease in leases)
        {
            yield return lease;
        }
    }

    private static Lease ReadLease(DbDataReader reader) =>
        new(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetInt64(2),
            reader.GetFieldValue<DateTimeOffset>(3)
        );
}
