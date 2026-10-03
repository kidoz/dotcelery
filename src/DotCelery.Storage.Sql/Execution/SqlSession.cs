using System.Data;
using System.Data.Common;
using System.Runtime.InteropServices;

namespace DotCelery.Storage.Sql.Execution;

/// <summary>
/// Provides the data source for a connection string.
/// </summary>
public interface ISqlDataSourceProvider
{
    /// <summary>
    /// Gets the data source (connection pool) for a connection string.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The data source, shared by every caller with the same connection string.</returns>
    DbDataSource GetDataSource(string connectionString);
}

/// <summary>
/// Runs SQL statements, opening a connection for each call.
/// </summary>
public sealed class SqlExecutor
{
    private const int MaxAttempts = 3;

    private readonly DbDataSource _dataSource;
    private readonly TimeSpan _commandTimeout;
    private readonly Func<DbException, bool> _isTransient;
    private readonly AsyncLocal<DbTransaction?> _callerTransaction = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="SqlExecutor"/> class.
    /// </summary>
    /// <param name="dataSource">The data source of the database.</param>
    /// <param name="commandTimeout">How long a command may run.</param>
    /// <param name="isTransient">
    /// Identifies errors after which the whole operation can safely run again, such as a
    /// deadlock that rolled it back. Such operations are retried a few times.
    /// </param>
    public SqlExecutor(
        DbDataSource dataSource,
        TimeSpan commandTimeout,
        Func<DbException, bool>? isTransient = null
    )
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        _dataSource = dataSource;
        _commandTimeout = commandTimeout;
        _isTransient = isTransient ?? (_ => false);
    }

    /// <summary>
    /// Opens a session on a new connection, or on the caller's connection and transaction while
    /// <see cref="InCallerTransactionAsync"/> is running.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The session; dispose it to close a connection it owns.</returns>
    public async Task<SqlSession> OpenSessionAsync(CancellationToken cancellationToken)
    {
        if (_callerTransaction.Value is { } transaction)
        {
            if (transaction.Connection is not { } callerConnection)
            {
                throw new InvalidOperationException("The caller's transaction has completed.");
            }

            return SqlSession.InTransaction(callerConnection, _commandTimeout, transaction);
        }

        var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        return new SqlSession(connection, _commandTimeout);
    }

    /// <summary>
    /// Runs work with every statement of this executor written in the given transaction, which
    /// the caller owns and commits or rolls back.
    /// </summary>
    /// <param name="transaction">The caller's transaction.</param>
    /// <param name="work">The work to run in the transaction.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <remarks>
    /// A failing statement does not retry inside the transaction: a deadlock leaves the
    /// caller's transaction to roll back and run again, as the caller decides.
    /// </remarks>
    public async ValueTask InCallerTransactionAsync(
        DbTransaction transaction,
        Func<CancellationToken, ValueTask> work,
        CancellationToken cancellationToken
    )
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(work);

        var previous = _callerTransaction.Value;
        _callerTransaction.Value = transaction;

        try
        {
            await work(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _callerTransaction.Value = previous;
        }
    }

    /// <summary>
    /// Executes a statement on its own connection.
    /// </summary>
    /// <param name="sql">The statement.</param>
    /// <param name="bind">Binds the parameters.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of affected rows.</returns>
    public Task<int> ExecuteAsync(
        string sql,
        Action<SqlParameters>? bind,
        CancellationToken cancellationToken
    ) => RunAsync(session => session.ExecuteAsync(sql, bind, cancellationToken), cancellationToken);

    /// <summary>
    /// Runs a query on its own connection and maps its first row.
    /// </summary>
    /// <typeparam name="T">The mapped type.</typeparam>
    /// <param name="sql">The query.</param>
    /// <param name="bind">Binds the parameters.</param>
    /// <param name="map">Maps the row.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The mapped row, or the default value if there is none.</returns>
    public Task<T?> QuerySingleAsync<T>(
        string sql,
        Action<SqlParameters>? bind,
        Func<DbDataReader, T> map,
        CancellationToken cancellationToken
    ) =>
        RunAsync(
            session => session.QuerySingleAsync(sql, bind, map, cancellationToken),
            cancellationToken
        );

    /// <summary>
    /// Runs a query on its own connection and maps every row.
    /// </summary>
    /// <typeparam name="T">The mapped type.</typeparam>
    /// <param name="sql">The query.</param>
    /// <param name="bind">Binds the parameters.</param>
    /// <param name="map">Maps a row.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The mapped rows.</returns>
    public Task<List<T>> QueryAsync<T>(
        string sql,
        Action<SqlParameters>? bind,
        Func<DbDataReader, T> map,
        CancellationToken cancellationToken
    ) =>
        RunAsync(
            session => session.QueryAsync(sql, bind, map, cancellationToken),
            cancellationToken
        );

    /// <summary>
    /// Runs work in a read-committed transaction on its own connection, and commits it.
    /// </summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="work">The work, which may run again if a transient error rolls it back.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the work.</returns>
    public Task<T> InTransactionAsync<T>(
        Func<SqlSession, Task<T>> work,
        CancellationToken cancellationToken
    )
    {
        ArgumentNullException.ThrowIfNull(work);

        return RunAsync(
            session => session.InTransactionAsync(() => work(session), cancellationToken),
            cancellationToken
        );
    }

    private async Task<T> RunAsync<T>(
        Func<SqlSession, Task<T>> operation,
        CancellationToken cancellationToken
    )
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await using var session = await OpenSessionAsync(cancellationToken)
                    .ConfigureAwait(false);
                return await operation(session).ConfigureAwait(false);
            }
            // Some drivers report a cancelled command as a database error
            catch (DbException ex) when (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(ex.Message, ex, cancellationToken);
            }
            catch (DbException ex)
                when (attempt < MaxAttempts && _callerTransaction.Value is null && _isTransient(ex))
            {
                await Task.Delay(
                        TimeSpan.FromMilliseconds(Random.Shared.Next(5, 20) * attempt),
                        cancellationToken
                    )
                    .ConfigureAwait(false);
            }
        }
    }
}

/// <summary>
/// Runs SQL statements on one open connection.
/// </summary>
public sealed class SqlSession : IAsyncDisposable
{
    private readonly DbConnection _connection;
    private readonly TimeSpan _commandTimeout;
    private readonly bool _ownsConnection;
    private DbTransaction? _transaction;
    private bool _ownsTransaction;

    internal SqlSession(DbConnection connection, TimeSpan commandTimeout)
        : this(connection, commandTimeout, transaction: null, ownsConnection: true) { }

    private SqlSession(
        DbConnection connection,
        TimeSpan commandTimeout,
        DbTransaction? transaction,
        bool ownsConnection
    )
    {
        _connection = connection;
        _commandTimeout = commandTimeout;
        _transaction = transaction;
        _ownsConnection = ownsConnection;
    }

    /// <summary>
    /// Opens a session over the caller's open connection that writes in the caller's
    /// transaction. Disposing it leaves the connection and the transaction open.
    /// </summary>
    internal static SqlSession InTransaction(
        DbConnection connection,
        TimeSpan commandTimeout,
        DbTransaction transaction
    ) => new(connection, commandTimeout, transaction, ownsConnection: false);

    /// <summary>
    /// Runs work in a transaction that commits when the work completes and rolls back if it fails.
    /// </summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="work">The work; statements of this session run in the transaction.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the work.</returns>
    /// <remarks>
    /// Work inside a transaction the caller owns joins that transaction instead of starting
    /// another, and does not commit it.
    /// </remarks>
    public async Task<T> InTransactionAsync<T>(
        Func<Task<T>> work,
        CancellationToken cancellationToken
    )
    {
        ArgumentNullException.ThrowIfNull(work);
        if (_transaction is not null)
        {
            if (!_ownsTransaction)
            {
                return await work().ConfigureAwait(false);
            }

            throw new InvalidOperationException("The session is already in a transaction.");
        }

        await using var transaction = await _connection
            .BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
            .ConfigureAwait(false);
        _transaction = transaction;
        _ownsTransaction = true;

        try
        {
            var result = await work().ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return result;
        }
        finally
        {
            _transaction = null;
            _ownsTransaction = false;
        }
    }

    /// <summary>
    /// Runs a statement and returns the number of affected rows.
    /// </summary>
    /// <param name="sql">The statement.</param>
    /// <param name="bind">Binds the statement parameters.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of affected rows.</returns>
    public async Task<int> ExecuteAsync(
        string sql,
        Action<SqlParameters>? bind,
        CancellationToken cancellationToken
    )
    {
        await using var command = CreateCommand(sql, bind);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Runs a query and maps its first row.
    /// </summary>
    /// <typeparam name="T">The mapped type.</typeparam>
    /// <param name="sql">The query.</param>
    /// <param name="bind">Binds the query parameters.</param>
    /// <param name="map">Maps the row.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The mapped row, or the default value if the query returned no rows.</returns>
    public async Task<T?> QuerySingleAsync<T>(
        string sql,
        Action<SqlParameters>? bind,
        Func<DbDataReader, T> map,
        CancellationToken cancellationToken
    )
    {
        ArgumentNullException.ThrowIfNull(map);

        await using var command = CreateCommand(sql, bind);
        await using var reader = await command
            .ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? map(reader)
            : default;
    }

    /// <summary>
    /// Runs a query and maps every row.
    /// </summary>
    /// <typeparam name="T">The mapped type.</typeparam>
    /// <param name="sql">The query.</param>
    /// <param name="bind">Binds the query parameters.</param>
    /// <param name="map">Maps each row.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The mapped rows.</returns>
    public async Task<List<T>> QueryAsync<T>(
        string sql,
        Action<SqlParameters>? bind,
        Func<DbDataReader, T> map,
        CancellationToken cancellationToken
    )
    {
        ArgumentNullException.ThrowIfNull(map);

        await using var command = CreateCommand(sql, bind);
        await using var reader = await command
            .ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        var rows = new List<T>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            rows.Add(map(reader));
        }

        return rows;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() =>
        _ownsConnection ? _connection.DisposeAsync() : ValueTask.CompletedTask;

    private DbCommand CreateCommand(string sql, Action<SqlParameters>? bind)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);

        var command = _connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = _transaction;
        command.CommandTimeout = (int)Math.Ceiling(_commandTimeout.TotalSeconds);
        bind?.Invoke(new SqlParameters(command));
        return command;
    }
}

/// <summary>
/// Binds typed statement parameters. Referenced in SQL as <c>@name</c>.
/// </summary>
public sealed class SqlParameters
{
    private readonly DbCommand _command;

    internal SqlParameters(DbCommand command)
    {
        _command = command;
    }

    /// <summary>Binds a text value.</summary>
    /// <param name="name">The parameter name, without <c>@</c>.</param>
    /// <param name="value">The value.</param>
    /// <returns>This instance.</returns>
    public SqlParameters Text(string name, string? value) => Add(name, DbType.String, value);

    /// <summary>Binds binary data.</summary>
    /// <param name="name">The parameter name, without <c>@</c>.</param>
    /// <param name="value">The value.</param>
    /// <returns>This instance.</returns>
    public SqlParameters Binary(string name, ReadOnlyMemory<byte> value) =>
        Add(
            name,
            DbType.Binary,
            MemoryMarshal.TryGetArray(value, out var segment)
            && segment.Offset == 0
            && segment.Count == segment.Array!.Length
                ? segment.Array
                : value.ToArray()
        );

    /// <summary>Binds a 32-bit integer.</summary>
    /// <param name="name">The parameter name, without <c>@</c>.</param>
    /// <param name="value">The value.</param>
    /// <returns>This instance.</returns>
    public SqlParameters Integer32(string name, int? value) => Add(name, DbType.Int32, value);

    /// <summary>Binds a 64-bit integer.</summary>
    /// <param name="name">The parameter name, without <c>@</c>.</param>
    /// <param name="value">The value.</param>
    /// <returns>This instance.</returns>
    public SqlParameters Integer64(string name, long? value) => Add(name, DbType.Int64, value);

    /// <summary>Binds a timestamp, stored in UTC.</summary>
    /// <param name="name">The parameter name, without <c>@</c>.</param>
    /// <param name="value">The value.</param>
    /// <returns>This instance.</returns>
    public SqlParameters Timestamp(string name, DateTimeOffset? value) =>
        Add(name, DbType.DateTimeOffset, value?.ToUniversalTime());

    /// <summary>Binds a UUID.</summary>
    /// <param name="name">The parameter name, without <c>@</c>.</param>
    /// <param name="value">The value.</param>
    /// <returns>This instance.</returns>
    public SqlParameters Uuid(string name, Guid? value) => Add(name, DbType.Guid, value);

    private SqlParameters Add(string name, DbType type, object? value)
    {
        var parameter = _command.CreateParameter();
        parameter.ParameterName = name;
        parameter.DbType = type;
        parameter.Value = value ?? DBNull.Value;
        _command.Parameters.Add(parameter);
        return this;
    }
}
