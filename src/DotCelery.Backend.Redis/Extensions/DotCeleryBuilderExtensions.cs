using DotCelery.Backend.Redis.Storage;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Extensions;

namespace DotCelery.Backend.Redis.Extensions;

/// <summary>
/// Extension methods for configuring the Redis backend with DotCeleryBuilder.
/// </summary>
public static class DotCeleryBuilderExtensions
{
    /// <summary>
    /// Configures the Redis result backend.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The builder for chaining.</returns>
    public static DotCeleryBuilder UseRedis(
        this DotCeleryBuilder builder,
        Action<RedisStorageOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Remove any existing backend registration
        var existingBackend = builder.Services.FirstOrDefault(d =>
            d.ServiceType == typeof(IResultBackend)
        );
        if (existingBackend is not null)
        {
            builder.Services.Remove(existingBackend);
        }

        builder.Services.AddRedisBackend(configure);

        return builder;
    }

    /// <summary>
    /// Configures the Redis result backend with a connection string.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="connectionString">The Redis connection string.</param>
    /// <returns>The builder for chaining.</returns>
    public static DotCeleryBuilder UseRedis(
        this DotCeleryBuilder builder,
        string connectionString
    ) => builder.UseRedis(options => options.ConnectionString = connectionString);
}
