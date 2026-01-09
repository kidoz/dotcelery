using DotCelery.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace DotCelery.Backend.Mongo.Extensions;

/// <summary>
/// Extension methods for configuring MongoDB result backend.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds MongoDB result backend to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMongoBackend(
        this IServiceCollection services,
        Action<MongoBackendOptions>? configure = null
    )
    {
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddSingleton<IResultBackend, MongoResultBackend>();

        return services;
    }

    /// <summary>
    /// Adds MongoDB result backend with connection string.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <param name="databaseName">The database name.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMongoBackend(
        this IServiceCollection services,
        string connectionString,
        string databaseName = "celery"
    )
    {
        return services.AddMongoBackend(options =>
        {
            options.ConnectionString = connectionString;
            options.DatabaseName = databaseName;
        });
    }
}
