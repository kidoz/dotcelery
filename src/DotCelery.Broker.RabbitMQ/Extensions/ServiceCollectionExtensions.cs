using DotCelery.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace DotCelery.Broker.RabbitMQ.Extensions;

/// <summary>
/// Extension methods for configuring RabbitMQ broker.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds RabbitMQ message broker to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRabbitMQBroker(
        this IServiceCollection services,
        Action<RabbitMQBrokerOptions>? configure = null
    )
    {
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddSingleton<IMessageBroker, RabbitMQBroker>();

        return services;
    }

    /// <summary>
    /// Adds RabbitMQ message broker with connection string.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The AMQP connection string.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRabbitMQBroker(
        this IServiceCollection services,
        string connectionString
    )
    {
        return services.AddRabbitMQBroker(options =>
        {
            options.ConnectionString = connectionString;
        });
    }
}
