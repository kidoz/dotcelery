using DotCelery.Backend.Redis.Services;
using DotCelery.Backend.Redis.Storage;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Batches;
using DotCelery.Core.Execution;
using DotCelery.Core.Extensions;
using DotCelery.Core.Partitioning;
using DotCelery.Core.Sagas;
using DotCelery.Core.Storage;
using DotCelery.Core.Storage.Stores;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace DotCelery.Backend.Redis.Extensions;

/// <summary>
/// Extension methods for registering Redis stores.
/// </summary>
/// <remarks>
/// Every store keeps its data in the Redis storage primitives added by
/// <see cref="AddRedisStorage"/>, so all stores share one <see cref="RedisStorageOptions"/>
/// whichever registration configures it. Retention and timing are configured with
/// <see cref="StorageStoreOptions"/>.
/// </remarks>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the provider that shares one connection per connection string.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisConnectionProvider(this IServiceCollection services)
    {
        services.TryAddSingleton<IRedisConnectionProvider, RedisConnectionProvider>();
        return services;
    }

    /// <summary>
    /// Adds the Redis storage primitives (<see cref="IStorageProvider"/>) and the
    /// <see cref="StoragePurgeService"/> that deletes expired entries.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisStorage(
        this IServiceCollection services,
        Action<RedisStorageOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddRedisConnectionProvider();
        services.TryAddSingleton<IStorageProvider>(sp => new RedisStorageProvider(
            sp.GetRequiredService<IRedisConnectionProvider>(),
            sp.GetRequiredService<IOptions<RedisStorageOptions>>().Value,
            sp.GetService<TimeProvider>()
        ));
        services.AddStoragePurge();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, RedisBackendInsecureDefaultsCheck>()
        );

        return services;
    }

    /// <summary>
    /// Adds the Redis result backend.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisBackend(
        this IServiceCollection services,
        Action<RedisStorageOptions>? configure = null
    ) => services.AddRedisStorageStore<IResultBackend, ResultBackend>(configure);

    /// <summary>
    /// Adds the Redis result backend with a connection string.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The Redis connection string.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisBackend(
        this IServiceCollection services,
        string connectionString
    ) => services.AddRedisBackend(options => options.ConnectionString = connectionString);

    /// <summary>
    /// Adds the Redis delayed message store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisDelayedMessageStore(
        this IServiceCollection services,
        Action<RedisStorageOptions>? configure = null
    ) => services.AddRedisStorageStore<IDelayedMessageStore, DelayedMessageStore>(configure);

    /// <summary>
    /// Adds the Redis revocation store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisRevocationStore(
        this IServiceCollection services,
        Action<RedisStorageOptions>? configure = null
    ) => services.AddRedisStorageStore<IRevocationStore, RevocationStore>(configure);

    /// <summary>
    /// Adds the Redis rate limiter.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisRateLimiter(
        this IServiceCollection services,
        Action<RedisStorageOptions>? configure = null
    ) => services.AddRedisStorageStore<IRateLimiter, WindowRateLimiter>(configure);

    /// <summary>
    /// Adds the Redis delayed message store, revocation store, and rate limiter.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisHighPriorityFeatures(
        this IServiceCollection services,
        Action<RedisStorageOptions>? configure = null
    ) =>
        services
            .AddRedisDelayedMessageStore(configure)
            .AddRedisRevocationStore()
            .AddRedisRateLimiter();

    /// <summary>
    /// Adds the Redis outbox store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisOutboxStore(
        this IServiceCollection services,
        Action<RedisStorageOptions>? configure = null
    ) => services.AddRedisStorageStore<IOutboxStore, OutboxStore>(configure);

    /// <summary>
    /// Adds the Redis inbox store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisInboxStore(
        this IServiceCollection services,
        Action<RedisStorageOptions>? configure = null
    ) => services.AddRedisStorageStore<IInboxStore, InboxStore>(configure);

    /// <summary>
    /// Adds the Redis dead letter store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisDeadLetterStore(
        this IServiceCollection services,
        Action<RedisStorageOptions>? configure = null
    ) => services.AddRedisStorageStore<IDeadLetterStore, DeadLetterStore>(configure);

    /// <summary>
    /// Adds the Redis signal store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisSignalStore(
        this IServiceCollection services,
        Action<RedisStorageOptions>? configure = null
    ) => services.AddRedisStorageStore<ISignalStore, SignalStore>(configure);

    /// <summary>
    /// Adds the Redis batch store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisBatchStore(
        this IServiceCollection services,
        Action<RedisStorageOptions>? configure = null
    ) => services.AddRedisStorageStore<IBatchStore, BatchStore>(configure);

    /// <summary>
    /// Adds the Redis saga store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisSagaStore(
        this IServiceCollection services,
        Action<RedisStorageOptions>? configure = null
    ) => services.AddRedisStorageStore<ISagaStore, SagaStore>(configure);

    /// <summary>
    /// Adds the Redis partition lock store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisPartitionLockStore(
        this IServiceCollection services,
        Action<RedisStorageOptions>? configure = null
    ) => services.AddRedisStorageStore<IPartitionLockStore, PartitionLockStore>(configure);

    /// <summary>
    /// Adds the Redis task execution tracker.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisTaskExecutionTracker(
        this IServiceCollection services,
        Action<RedisStorageOptions>? configure = null
    ) => services.AddRedisStorageStore<ITaskExecutionTracker, TaskExecutionTracker>(configure);

    /// <summary>
    /// Adds the Redis queue metrics store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisQueueMetrics(
        this IServiceCollection services,
        Action<RedisStorageOptions>? configure = null
    ) => services.AddRedisStorageStore<IQueueMetrics, QueueMetrics>(configure);

    /// <summary>
    /// Adds the Redis historical data store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRedisHistoricalDataStore(
        this IServiceCollection services,
        Action<RedisStorageOptions>? configure = null
    ) => services.AddRedisStorageStore<IHistoricalDataStore, HistoricalDataStore>(configure);

    private static IServiceCollection AddRedisStorageStore<TService, TImplementation>(
        this IServiceCollection services,
        Action<RedisStorageOptions>? configure
    )
        where TService : class
        where TImplementation : class, TService
    {
        services.AddRedisStorage(configure);
        services.AddSingleton<TService, TImplementation>();

        return services;
    }
}
