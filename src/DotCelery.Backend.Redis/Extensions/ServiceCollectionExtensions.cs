using DotCelery.Backend.Redis.Batches;
using DotCelery.Backend.Redis.DelayedMessageStore;
using DotCelery.Backend.Redis.RateLimiting;
using DotCelery.Backend.Redis.Revocation;
using DotCelery.Backend.Redis.Signals;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Batches;
using Microsoft.Extensions.DependencyInjection;

namespace DotCelery.Backend.Redis.Extensions;

/// <summary>
/// Extension methods for configuring Redis backend services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds Redis result backend to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisBackend(
        this IServiceCollection services,
        Action<RedisBackendOptions>? configure = null
    )
    {
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddSingleton<IResultBackend, RedisResultBackend>();

        return services;
    }

    /// <summary>
    /// Adds Redis result backend with connection string.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The Redis connection string.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisBackend(
        this IServiceCollection services,
        string connectionString
    )
    {
        return services.AddRedisBackend(options =>
        {
            options.ConnectionString = connectionString;
        });
    }

    /// <summary>
    /// Adds Redis delayed message store to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisDelayedMessageStore(
        this IServiceCollection services,
        Action<RedisDelayedMessageStoreOptions>? configure = null
    )
    {
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddSingleton<IDelayedMessageStore, RedisDelayedMessageStore>();

        return services;
    }

    /// <summary>
    /// Adds Redis delayed message store with connection string.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The Redis connection string.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisDelayedMessageStore(
        this IServiceCollection services,
        string connectionString
    )
    {
        return services.AddRedisDelayedMessageStore(options =>
        {
            options.ConnectionString = connectionString;
        });
    }

    /// <summary>
    /// Adds Redis revocation store to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisRevocationStore(
        this IServiceCollection services,
        Action<RedisRevocationStoreOptions>? configure = null
    )
    {
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddSingleton<IRevocationStore, RedisRevocationStore>();

        return services;
    }

    /// <summary>
    /// Adds Redis revocation store with connection string.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The Redis connection string.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisRevocationStore(
        this IServiceCollection services,
        string connectionString
    )
    {
        return services.AddRedisRevocationStore(options =>
        {
            options.ConnectionString = connectionString;
        });
    }

    /// <summary>
    /// Adds Redis rate limiter to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisRateLimiter(
        this IServiceCollection services,
        Action<RedisRateLimiterOptions>? configure = null
    )
    {
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddSingleton<IRateLimiter, RedisRateLimiter>();

        return services;
    }

    /// <summary>
    /// Adds Redis rate limiter with connection string.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The Redis connection string.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisRateLimiter(
        this IServiceCollection services,
        string connectionString
    )
    {
        return services.AddRedisRateLimiter(options =>
        {
            options.ConnectionString = connectionString;
        });
    }

    /// <summary>
    /// Adds all Redis high-priority feature implementations.
    /// This includes delayed message store, revocation store, and rate limiter.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The Redis connection string.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisHighPriorityFeatures(
        this IServiceCollection services,
        string connectionString
    )
    {
        return services
            .AddRedisDelayedMessageStore(connectionString)
            .AddRedisRevocationStore(connectionString)
            .AddRedisRateLimiter(connectionString);
    }

    /// <summary>
    /// Adds Redis batch store to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisBatchStore(
        this IServiceCollection services,
        Action<RedisBatchStoreOptions>? configure = null
    )
    {
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddSingleton<IBatchStore, RedisBatchStore>();

        return services;
    }

    /// <summary>
    /// Adds Redis batch store with connection string.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The Redis connection string.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisBatchStore(
        this IServiceCollection services,
        string connectionString
    )
    {
        return services.AddRedisBatchStore(options =>
        {
            options.ConnectionString = connectionString;
        });
    }

    /// <summary>
    /// Adds Redis signal queue to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisSignalStore(
        this IServiceCollection services,
        Action<RedisSignalStoreOptions>? configure = null
    )
    {
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddSingleton<ISignalStore, RedisSignalStore>();

        return services;
    }

    /// <summary>
    /// Adds Redis signal queue with connection string.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The Redis connection string.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisSignalStore(
        this IServiceCollection services,
        string connectionString
    )
    {
        return services.AddRedisSignalStore(options =>
        {
            options.ConnectionString = connectionString;
        });
    }
}
