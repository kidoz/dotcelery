using System.Collections.Concurrent;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.Storage;

/// <summary>
/// Shares one connection (multiplexer) per connection configuration.
/// </summary>
public interface IRedisConnectionProvider
{
    /// <summary>
    /// Gets the connection for the options' connection string, connecting on first use.
    /// </summary>
    /// <param name="options">The storage options.</param>
    /// <returns>The connection.</returns>
    Task<IConnectionMultiplexer> GetConnectionAsync(RedisStorageOptions options);
}

/// <summary>
/// The default <see cref="IRedisConnectionProvider"/>. A connection reconnects by itself, so
/// it is kept for the lifetime of the provider and disposed with it.
/// </summary>
public sealed class RedisConnectionProvider : IRedisConnectionProvider, IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, Lazy<Task<IConnectionMultiplexer>>> _connections =
        new(StringComparer.Ordinal);

    /// <inheritdoc />
    public async Task<IConnectionMultiplexer> GetConnectionAsync(RedisStorageOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var key =
            $"{options.ConnectionString}|{options.ConnectTimeout.Ticks}|{options.SyncTimeout.Ticks}";
        var connection = _connections.GetOrAdd(
            key,
            _ => new Lazy<Task<IConnectionMultiplexer>>(() => ConnectAsync(options))
        );

        try
        {
            return await connection.Value.ConfigureAwait(false);
        }
        catch
        {
            // Let the next caller try again rather than keep the failure
            _connections.TryRemove(KeyValuePair.Create(key, connection));
            throw;
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        foreach (var connection in _connections.Values)
        {
            if (connection.IsValueCreated && connection.Value.IsCompletedSuccessfully)
            {
                await connection.Value.Result.DisposeAsync().ConfigureAwait(false);
            }
        }

        _connections.Clear();
    }

    private static async Task<IConnectionMultiplexer> ConnectAsync(RedisStorageOptions options)
    {
        var configuration = ConfigurationOptions.Parse(options.ConnectionString);
        configuration.ConnectTimeout = (int)options.ConnectTimeout.TotalMilliseconds;
        configuration.SyncTimeout = (int)options.SyncTimeout.TotalMilliseconds;
        configuration.AbortOnConnectFail = false;

        return await ConnectionMultiplexer.ConnectAsync(configuration).ConfigureAwait(false);
    }
}
