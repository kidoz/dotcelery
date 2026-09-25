using DotCelery.Core.Storage;
using DotCelery.Storage.Sql.Execution;

namespace DotCelery.Storage.Sql.Storage;

/// <summary>
/// <see cref="ICounterStore"/> over <see cref="SqlStorageStatements"/>.
/// </summary>
internal sealed class SqlCounterStore : ICounterStore
{
    private readonly SqlStorageStatements _statements;
    private readonly SqlExecutor _sql;
    private readonly TimeProvider _timeProvider;

    public SqlCounterStore(
        SqlStorageStatements statements,
        SqlExecutor sql,
        TimeProvider timeProvider
    )
    {
        _statements = statements;
        _sql = sql;
        _timeProvider = timeProvider;
    }

    public async ValueTask<long> IncrementAsync(
        string key,
        long delta = 1,
        TimeSpan? timeToLive = null,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidKey(key);
        if (timeToLive is { } ttl)
        {
            StorageGuard.ThrowIfNotPositive(ttl, nameof(timeToLive));
        }

        var now = _timeProvider.GetUtcNow();

        var value = await _sql.QuerySingleAsync<long?>(
                _statements.CounterIncrement,
                p =>
                    p.Text("key", key)
                        .Integer64("delta", delta)
                        .Timestamp("expires_at", now + timeToLive)
                        .Timestamp("now", now),
                r => r.GetInt64(0),
                cancellationToken
            )
            .ConfigureAwait(false);

        return value
            ?? throw new InvalidOperationException("The increment statement returned no value.");
    }

    public async ValueTask<long> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        StorageGuard.ThrowIfInvalidKey(key);

        var value = await _sql.QuerySingleAsync<long?>(
                _statements.CounterGet,
                p => p.Text("key", key).Timestamp("now", _timeProvider.GetUtcNow()),
                r => r.GetInt64(0),
                cancellationToken
            )
            .ConfigureAwait(false);

        return value ?? 0;
    }

    public async ValueTask<bool> DeleteAsync(
        string key,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidKey(key);

        var live = await _sql.QuerySingleAsync<bool?>(
                _statements.CounterDelete,
                p => p.Text("key", key).Timestamp("now", _timeProvider.GetUtcNow()),
                r => r.GetBoolean(0),
                cancellationToken
            )
            .ConfigureAwait(false);

        return live == true;
    }

    public async ValueTask<WindowResult> TryAddToWindowAsync(
        string key,
        int limit,
        TimeSpan window,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidKey(key);
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        StorageGuard.ThrowIfNotPositive(window);
        var now = _timeProvider.GetUtcNow();

        return await _sql.InTransactionAsync(
                async session =>
                {
                    // Locks the window row until commit, so checking and adding are atomic
                    await session
                        .ExecuteAsync(
                            _statements.WindowLock,
                            p =>
                                p.Text("key", key)
                                    .Integer64("window_ms", (long)window.TotalMilliseconds),
                            cancellationToken
                        )
                        .ConfigureAwait(false);

                    await session
                        .ExecuteAsync(
                            _statements.WindowPrune,
                            p => p.Text("key", key).Timestamp("window_start", now - window),
                            cancellationToken
                        )
                        .ConfigureAwait(false);

                    var (count, oldest) = await session
                        .QuerySingleAsync(
                            _statements.WindowState,
                            p => p.Text("key", key),
                            r => (r.GetInt64(0), SqlRead.NullableTimestamp(r, 1)),
                            cancellationToken
                        )
                        .ConfigureAwait(false);

                    if (count >= limit)
                    {
                        return new WindowResult(false, count, oldest + window - now);
                    }

                    await session
                        .ExecuteAsync(
                            _statements.WindowAdd,
                            p => p.Text("key", key).Timestamp("now", now),
                            cancellationToken
                        )
                        .ConfigureAwait(false);

                    return new WindowResult(true, count + 1, null);
                },
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    public async ValueTask<WindowSnapshot> GetWindowAsync(
        string key,
        TimeSpan window,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidKey(key);
        StorageGuard.ThrowIfNotPositive(window);

        return await _sql.QuerySingleAsync(
                _statements.WindowSnapshot,
                p =>
                    p.Text("key", key)
                        .Timestamp("window_start", _timeProvider.GetUtcNow() - window),
                r => new WindowSnapshot(r.GetInt64(0), SqlRead.NullableTimestamp(r, 1)),
                cancellationToken
            )
            .ConfigureAwait(false);
    }
}
