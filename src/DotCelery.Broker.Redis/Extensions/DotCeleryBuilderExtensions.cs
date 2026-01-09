using DotCelery.Core.Abstractions;
using DotCelery.Core.Extensions;
using Microsoft.Extensions.DependencyInjection;

namespace DotCelery.Broker.Redis.Extensions;

/// <summary>
/// Extension methods for configuring Redis Streams broker with DotCeleryBuilder.
/// </summary>
public static class DotCeleryBuilderExtensions
{
    /// <summary>
    /// Configures the Redis Streams message broker.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The builder for chaining.</returns>
    public static DotCeleryBuilder UseRedisBroker(
        this DotCeleryBuilder builder,
        Action<RedisBrokerOptions>? configure = null
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

        builder.Services.AddSingleton<IMessageBroker, RedisBroker>();

        return builder;
    }

    /// <summary>
    /// Configures the Redis Streams message broker with a connection string.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="connectionString">The Redis connection string.</param>
    /// <returns>The builder for chaining.</returns>
    public static DotCeleryBuilder UseRedisBroker(
        this DotCeleryBuilder builder,
        string connectionString
    )
    {
        return builder.UseRedisBroker(options =>
        {
            options.ConnectionString = connectionString;
        });
    }
}
