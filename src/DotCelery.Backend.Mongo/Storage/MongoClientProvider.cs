using System.Collections.Concurrent;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.Storage;

/// <summary>
/// Shares one client (connection pool) per connection configuration.
/// </summary>
public interface IMongoClientProvider
{
    /// <summary>
    /// Gets the client for the options' connection string.
    /// </summary>
    /// <param name="options">The storage options.</param>
    /// <returns>The client.</returns>
    IMongoClient GetClient(MongoStorageOptions options);
}

/// <summary>
/// The default <see cref="IMongoClientProvider"/>. Clients are kept for the lifetime of the
/// provider and disposed with it.
/// </summary>
public sealed class MongoClientProvider : IMongoClientProvider, IDisposable
{
    private readonly ConcurrentDictionary<string, Lazy<MongoClient>> _clients = new(
        StringComparer.Ordinal
    );

    /// <inheritdoc />
    public IMongoClient GetClient(MongoStorageOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var key =
            $"{options.ConnectionString}|{options.ServerSelectionTimeout.Ticks}|{options.ConnectTimeout.Ticks}";
        return _clients
            .GetOrAdd(
                key,
                _ => new Lazy<MongoClient>(() =>
                {
                    var settings = MongoClientSettings.FromConnectionString(
                        options.ConnectionString
                    );
                    settings.ServerSelectionTimeout = options.ServerSelectionTimeout;
                    settings.ConnectTimeout = options.ConnectTimeout;
                    return new MongoClient(settings);
                })
            )
            .Value;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        foreach (var client in _clients.Values)
        {
            if (client.IsValueCreated)
            {
                client.Value.Dispose();
            }
        }

        _clients.Clear();
    }
}
