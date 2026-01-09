using DotCelery.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace DotCelery.Broker.Redis.Extensions;

/// <summary>
/// Extension methods for configuring Redis Streams broker.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds Redis Streams message broker to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisBroker(
        this IServiceCollection services,
        Action<RedisBrokerOptions>? configure = null
    )
    {
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddSingleton<IMessageBroker, RedisBroker>();

        return services;
    }

    /// <summary>
    /// Adds Redis Streams message broker with connection string.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The Redis connection string.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisBroker(
        this IServiceCollection services,
        string connectionString
    )
    {
        return services.AddRedisBroker(options =>
        {
            options.ConnectionString = connectionString;
        });
    }
}
