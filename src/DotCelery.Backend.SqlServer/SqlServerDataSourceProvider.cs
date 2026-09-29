using System.Collections.Concurrent;
using System.Data.Common;
using DotCelery.Storage.Sql.Execution;
using Microsoft.Data.SqlClient;

namespace DotCelery.Backend.SqlServer;

/// <summary>
/// Shares one data source per connection string. SQL Server pools connections per connection
/// string, so the data sources only save repeated setup.
/// </summary>
public sealed class SqlServerDataSourceProvider : ISqlDataSourceProvider, IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, DbDataSource> _dataSources = new(
        StringComparer.Ordinal
    );
    private bool _disposed;

    /// <inheritdoc />
    public DbDataSource GetDataSource(string connectionString)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(connectionString);

        return _dataSources.GetOrAdd(
            connectionString,
            cs => SqlClientFactory.Instance.CreateDataSource(cs)
        );
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        foreach (var dataSource in _dataSources.Values)
        {
            await dataSource.DisposeAsync().ConfigureAwait(false);
        }

        _dataSources.Clear();
    }
}
