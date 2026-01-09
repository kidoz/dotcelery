using System.Collections.Concurrent;
using Npgsql;

namespace DotCelery.Backend.Postgres;

/// <summary>
/// Provides shared <see cref="NpgsqlDataSource"/> instances for PostgreSQL stores.
/// This helps prevent connection pool fragmentation when multiple stores are registered.
/// </summary>
public interface IPostgresDataSourceProvider : IAsyncDisposable
{
    /// <summary>
    /// Gets or creates a shared <see cref="NpgsqlDataSource"/> for the given connection string.
    /// </summary>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <returns>A shared data source instance.</returns>
    NpgsqlDataSource GetDataSource(string connectionString);
}

/// <summary>
/// Default implementation of <see cref="IPostgresDataSourceProvider"/>.
/// Caches data sources by connection string to enable connection pool sharing.
/// </summary>
public sealed class PostgresDataSourceProvider : IPostgresDataSourceProvider
{
    private readonly ConcurrentDictionary<string, NpgsqlDataSource> _dataSources = new();
    private bool _disposed;

    /// <inheritdoc />
    public NpgsqlDataSource GetDataSource(string connectionString)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(connectionString);

        return _dataSources.GetOrAdd(
            connectionString,
            cs =>
            {
                var builder = new NpgsqlDataSourceBuilder(cs);
                builder.EnableDynamicJson();
                return builder.Build();
            }
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
