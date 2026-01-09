using DotCelery.Core.Abstractions;
using DotCelery.Core.Extensions;
using Microsoft.Extensions.DependencyInjection;

namespace DotCelery.Broker.RabbitMQ.Extensions;

/// <summary>
/// Extension methods for configuring RabbitMQ broker with DotCeleryBuilder.
/// </summary>
public static class DotCeleryBuilderExtensions
{
    /// <summary>
    /// Configures the RabbitMQ message broker.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The builder for chaining.</returns>
    public static DotCeleryBuilder UseRabbitMQ(
        this DotCeleryBuilder builder,
        Action<RabbitMQBrokerOptions>? configure = null
    )
    {
        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        // Remove any existing broker registration
        var existingBroker = builder.Services.FirstOrDefault(d =>
            d.ServiceType == typeof(IMessageBroker)
        );
        if (existingBroker is not null)
        {
            builder.Services.Remove(existingBroker);
        }

        builder.Services.AddSingleton<IMessageBroker, RabbitMQBroker>();

        return builder;
    }

    /// <summary>
    /// Configures the RabbitMQ message broker with a connection string.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="connectionString">The AMQP connection string.</param>
    /// <returns>The builder for chaining.</returns>
    public static DotCeleryBuilder UseRabbitMQ(
        this DotCeleryBuilder builder,
        string connectionString
    )
    {
        return builder.UseRabbitMQ(options =>
        {
            options.ConnectionString = connectionString;
        });
    }
}
