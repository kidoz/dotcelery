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
    private readonly DbDataSource _dataSource;
    private readonly TimeSpan _commandTimeout;

    /// <summary>
    /// Initializes a new instance of the <see cref="SqlExecutor"/> class.
    /// </summary>
    /// <param name="dataSource">The data source.</param>
    /// <param name="commandTimeout">The timeout for each statement.</param>
    public SqlExecutor(DbDataSource dataSource, TimeSpan commandTimeout)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        _dataSource = dataSource;
        _commandTimeout = commandTimeout;
    }

    /// <summary>
    /// Opens a session on one connection, for work that spans several statements.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The session; dispose it to close the connection.</returns>
    public async Task<SqlSession> OpenSessionAsync(CancellationToken cancellationToken)
    {
        var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        return new SqlSession(connection, _commandTimeout);
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
        await using var session = await OpenSessionAsync(cancellationToken).ConfigureAwait(false);
        return await session.ExecuteAsync(sql, bind, cancellationToken).ConfigureAwait(false);
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
        await using var session = await OpenSessionAsync(cancellationToken).ConfigureAwait(false);
        return await session
            .QuerySingleAsync(sql, bind, map, cancellationToken)
            .ConfigureAwait(false);
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
        await using var session = await OpenSessionAsync(cancellationToken).ConfigureAwait(false);
        return await session.QueryAsync(sql, bind, map, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Runs work in a transaction that commits when the work completes.
    /// </summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="work">The work, given a session bound to the transaction.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the work.</returns>
    public async Task<T> InTransactionAsync<T>(
        Func<SqlSession, Task<T>> work,
        CancellationToken cancellationToken
    )
    {
        ArgumentNullException.ThrowIfNull(work);

        await using var session = await OpenSessionAsync(cancellationToken).ConfigureAwait(false);
        return await session
            .InTransactionAsync(() => work(session), cancellationToken)
            .ConfigureAwait(false);
    }
}

/// <summary>
/// Runs SQL statements on one open connection.
/// </summary>
public sealed class SqlSession : IAsyncDisposable
{
    private readonly DbConnection _connection;
    private readonly TimeSpan _commandTimeout;
    private DbTransaction? _transaction;

    internal SqlSession(DbConnection connection, TimeSpan commandTimeout)
    {
        _connection = connection;
        _commandTimeout = commandTimeout;
    }

    /// <summary>
    /// Runs work in a transaction that commits when the work completes and rolls back if it fails.
    /// </summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="work">The work; statements of this session run in the transaction.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the work.</returns>
    public async Task<T> InTransactionAsync<T>(
        Func<Task<T>> work,
        CancellationToken cancellationToken
    )
    {
        ArgumentNullException.ThrowIfNull(work);
        if (_transaction is not null)
        {
            throw new InvalidOperationException("The session is already in a transaction.");
        }

        await using var transaction = await _connection
            .BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
            .ConfigureAwait(false);
        _transaction = transaction;

        try
        {
            var result = await work().ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return result;
        }
        finally
        {
            _transaction = null;
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
    public ValueTask DisposeAsync() => _connection.DisposeAsync();

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
