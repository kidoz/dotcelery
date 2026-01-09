using DotCelery.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace DotCelery.Backend.Postgres.Extensions;

/// <summary>
/// Extension methods for configuring PostgreSQL result backend.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the shared PostgreSQL data source provider to the service collection.
    /// This should be called before adding any PostgreSQL stores to enable connection pool sharing.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresDataSourceProvider(this IServiceCollection services)
    {
        services.TryAddSingleton<IPostgresDataSourceProvider, PostgresDataSourceProvider>();
        return services;
    }

    /// <summary>
    /// Adds PostgreSQL result backend to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresBackend(
        this IServiceCollection services,
        Action<PostgresBackendOptions>? configure = null
    )
    {
        // Ensure shared data source provider is registered
        services.AddPostgresDataSourceProvider();

        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddSingleton<IResultBackend, PostgresResultBackend>();

        return services;
    }

    /// <summary>
    /// Adds PostgreSQL result backend with connection string.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresBackend(
        this IServiceCollection services,
        string connectionString
    )
    {
        return services.AddPostgresBackend(options =>
        {
            options.ConnectionString = connectionString;
        });
    }
}
