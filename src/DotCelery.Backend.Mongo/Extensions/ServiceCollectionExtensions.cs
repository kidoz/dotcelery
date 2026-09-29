using DotCelery.Backend.Mongo.Storage;
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
using Microsoft.Extensions.Options;

namespace DotCelery.Backend.Mongo.Extensions;

/// <summary>
/// Extension methods for registering MongoDB stores.
/// </summary>
/// <remarks>
/// Every store keeps its data in the MongoDB storage primitives added by
/// <see cref="AddMongoStorage"/>, so all stores share one <see cref="MongoStorageOptions"/>
/// whichever registration configures it. Retention and timing are configured with
/// <see cref="StorageStoreOptions"/>.
/// </remarks>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the provider that shares one client (connection pool) per connection string.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMongoClientProvider(this IServiceCollection services)
    {
        services.TryAddSingleton<IMongoClientProvider, MongoClientProvider>();
        return services;
    }

    /// <summary>
    /// Adds the MongoDB storage primitives (<see cref="IStorageProvider"/>) and the
    /// <see cref="StoragePurgeService"/> that deletes expired entries.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMongoStorage(
        this IServiceCollection services,
        Action<MongoStorageOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddMongoClientProvider();
        services.TryAddSingleton<IStorageProvider>(sp => new MongoStorageProvider(
            sp.GetRequiredService<IMongoClientProvider>(),
            sp.GetRequiredService<IOptions<MongoStorageOptions>>().Value,
            sp.GetService<TimeProvider>()
        ));
        services.AddStoragePurge();

        return services;
    }

    /// <summary>
    /// Adds the MongoDB result backend.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMongoBackend(
        this IServiceCollection services,
        Action<MongoStorageOptions>? configure = null
    ) => services.AddMongoStorageStore<IResultBackend, ResultBackend>(configure);

    /// <summary>
    /// Adds the MongoDBDB result backend with a connection string.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <param name="databaseName">The database name.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMongoBackend(
        this IServiceCollection services,
        string connectionString,
        string databaseName = "dotcelery"
    ) =>
        services.AddMongoBackend(options =>
        {
            options.ConnectionString = connectionString;
            options.DatabaseName = databaseName;
        });

    /// <summary>
    /// Adds the MongoDB delayed message store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMongoDelayedMessageStore(
        this IServiceCollection services,
        Action<MongoStorageOptions>? configure = null
    ) => services.AddMongoStorageStore<IDelayedMessageStore, DelayedMessageStore>(configure);

    /// <summary>
    /// Adds the MongoDB revocation store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMongoRevocationStore(
        this IServiceCollection services,
        Action<MongoStorageOptions>? configure = null
    ) => services.AddMongoStorageStore<IRevocationStore, RevocationStore>(configure);

    /// <summary>
    /// Adds the MongoDB rate limiter.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMongoRateLimiter(
        this IServiceCollection services,
        Action<MongoStorageOptions>? configure = null
    ) => services.AddMongoStorageStore<IRateLimiter, WindowRateLimiter>(configure);

    /// <summary>
    /// Adds the MongoDB delayed message store, revocation store, and rate limiter.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMongoHighPriorityFeatures(
        this IServiceCollection services,
        Action<MongoStorageOptions>? configure = null
    ) =>
        services
            .AddMongoDelayedMessageStore(configure)
            .AddMongoRevocationStore()
            .AddMongoRateLimiter();

    /// <summary>
    /// Adds the MongoDB outbox store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMongoOutboxStore(
        this IServiceCollection services,
        Action<MongoStorageOptions>? configure = null
    ) => services.AddMongoStorageStore<IOutboxStore, OutboxStore>(configure);

    /// <summary>
    /// Adds the MongoDB inbox store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMongoInboxStore(
        this IServiceCollection services,
        Action<MongoStorageOptions>? configure = null
    ) => services.AddMongoStorageStore<IInboxStore, InboxStore>(configure);

    /// <summary>
    /// Adds the MongoDB dead letter store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMongoDeadLetterStore(
        this IServiceCollection services,
        Action<MongoStorageOptions>? configure = null
    ) => services.AddMongoStorageStore<IDeadLetterStore, DeadLetterStore>(configure);

    /// <summary>
    /// Adds the MongoDB signal store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMongoSignalStore(
        this IServiceCollection services,
        Action<MongoStorageOptions>? configure = null
    ) => services.AddMongoStorageStore<ISignalStore, SignalStore>(configure);

    /// <summary>
    /// Adds the MongoDB batch store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMongoBatchStore(
        this IServiceCollection services,
        Action<MongoStorageOptions>? configure = null
    ) => services.AddMongoStorageStore<IBatchStore, BatchStore>(configure);

    /// <summary>
    /// Adds the MongoDB saga store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMongoSagaStore(
        this IServiceCollection services,
        Action<MongoStorageOptions>? configure = null
    ) => services.AddMongoStorageStore<ISagaStore, SagaStore>(configure);

    /// <summary>
    /// Adds the MongoDB partition lock store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMongoPartitionLockStore(
        this IServiceCollection services,
        Action<MongoStorageOptions>? configure = null
    ) => services.AddMongoStorageStore<IPartitionLockStore, PartitionLockStore>(configure);

    /// <summary>
    /// Adds the MongoDB task execution tracker.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMongoTaskExecutionTracker(
        this IServiceCollection services,
        Action<MongoStorageOptions>? configure = null
    ) => services.AddMongoStorageStore<ITaskExecutionTracker, TaskExecutionTracker>(configure);

    /// <summary>
    /// Adds the MongoDB queue metrics store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMongoQueueMetrics(
        this IServiceCollection services,
        Action<MongoStorageOptions>? configure = null
    ) => services.AddMongoStorageStore<IQueueMetrics, QueueMetrics>(configure);

    /// <summary>
    /// Adds the MongoDB historical data store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMongoHistoricalDataStore(
        this IServiceCollection services,
        Action<MongoStorageOptions>? configure = null
    ) => services.AddMongoStorageStore<IHistoricalDataStore, HistoricalDataStore>(configure);

    private static IServiceCollection AddMongoStorageStore<TService, TImplementation>(
        this IServiceCollection services,
        Action<MongoStorageOptions>? configure
    )
        where TService : class
        where TImplementation : class, TService
    {
        services.AddMongoStorage(configure);
        services.AddSingleton<TService, TImplementation>();

        return services;
    }
}
