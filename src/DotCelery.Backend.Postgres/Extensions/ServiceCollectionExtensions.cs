using DotCelery.Backend.Postgres.Batches;
using DotCelery.Backend.Postgres.DeadLetter;
using DotCelery.Backend.Postgres.DelayedMessageStore;
using DotCelery.Backend.Postgres.Execution;
using DotCelery.Backend.Postgres.Historical;
using DotCelery.Backend.Postgres.Metrics;
using DotCelery.Backend.Postgres.Outbox;
using DotCelery.Backend.Postgres.Partitioning;
using DotCelery.Backend.Postgres.RateLimiting;
using DotCelery.Backend.Postgres.Revocation;
using DotCelery.Backend.Postgres.Sagas;
using DotCelery.Backend.Postgres.Signals;
using DotCelery.Backend.Postgres.Storage;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Batches;
using DotCelery.Core.Execution;
using DotCelery.Core.Partitioning;
using DotCelery.Core.Sagas;
using DotCelery.Core.Storage;
using DotCelery.Storage.Sql;
using DotCelery.Storage.Sql.Execution;
using DotCelery.Storage.Sql.Extensions;
using DotCelery.Storage.Sql.Migrations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace DotCelery.Backend.Postgres.Extensions;

/// <summary>
/// Extension methods for registering PostgreSQL stores.
/// </summary>
/// <remarks>
/// Each store registration also registers the migrations for the store's tables. Pending
/// migrations are applied when the host starts (see <see cref="AddPostgresMigrations"/>).
/// Stores registered another way have no tables unless their migrations are registered too.
/// </remarks>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the provider that shares one data source (connection pool) per connection string.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresDataSourceProvider(this IServiceCollection services)
    {
        services.TryAddSingleton<IPostgresDataSourceProvider, PostgresDataSourceProvider>();
        return services;
    }

    /// <summary>
    /// Adds the migrator that applies the registered migration modules with the PostgreSQL
    /// dialect, and runs it when the host starts unless
    /// <see cref="SqlMigrationOptions.RunAtStartup"/> is disabled. Without a host, resolve
    /// <see cref="SqlMigrator"/> and call <see cref="SqlMigrator.MigrateAsync"/> before using
    /// the stores.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional migration options configuration.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresMigrations(
        this IServiceCollection services,
        Action<SqlMigrationOptions>? configure = null
    )
    {
        services.AddPostgresDataSourceProvider();
        services.TryAddSingleton<SqlDialect>(PostgresDialect.Instance);
        services.TryAddSingleton<ISqlDataSourceProvider>(sp =>
            sp.GetRequiredService<IPostgresDataSourceProvider>()
        );
        services.AddSqlMigrations(configure);

        return services;
    }

    /// <summary>
    /// Adds the PostgreSQL storage primitives (<see cref="IStorageProvider"/>) and the
    /// migrations for their tables.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresStorage(
        this IServiceCollection services,
        Action<PostgresStorageOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddPostgresMigrations();
        services.TryAddSingleton(sp =>
        {
            var options = sp.GetRequiredService<IOptions<PostgresStorageOptions>>().Value;
            return PostgresStorage.CreateProvider(
                sp.GetRequiredService<IPostgresDataSourceProvider>()
                    .GetDataSource(options.ConnectionString),
                options,
                sp.GetService<TimeProvider>()
            );
        });
        services.AddSingleton(sp =>
            PostgresStorage.CreateModule(
                sp.GetRequiredService<IOptions<PostgresStorageOptions>>().Value
            )
        );

        return services;
    }

    /// <summary>
    /// Adds the PostgreSQL result backend.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresBackend(
        this IServiceCollection services,
        Action<PostgresBackendOptions>? configure = null
    ) =>
        services.AddPostgresStore<IResultBackend, PostgresResultBackend, PostgresBackendOptions>(
            configure,
            PostgresResultBackendMigrations.CreateModule
        );

    /// <summary>
    /// Adds the PostgreSQL result backend with a connection string.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresBackend(
        this IServiceCollection services,
        string connectionString
    ) =>
        services.AddPostgresBackend(options =>
        {
            options.ConnectionString = connectionString;
        });

    /// <summary>
    /// Adds the PostgreSQL outbox store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresOutboxStore(
        this IServiceCollection services,
        Action<PostgresOutboxStoreOptions>? configure = null
    ) =>
        services.AddPostgresStore<IOutboxStore, PostgresOutboxStore, PostgresOutboxStoreOptions>(
            configure,
            PostgresOutboxMigrations.CreateModule
        );

    /// <summary>
    /// Adds the PostgreSQL inbox store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresInboxStore(
        this IServiceCollection services,
        Action<PostgresInboxStoreOptions>? configure = null
    ) =>
        services.AddPostgresStore<IInboxStore, PostgresInboxStore, PostgresInboxStoreOptions>(
            configure,
            PostgresInboxMigrations.CreateModule
        );

    /// <summary>
    /// Adds the PostgreSQL dead letter store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresDeadLetterStore(
        this IServiceCollection services,
        Action<PostgresDeadLetterStoreOptions>? configure = null
    ) =>
        services.AddPostgresStore<
            IDeadLetterStore,
            PostgresDeadLetterStore,
            PostgresDeadLetterStoreOptions
        >(configure, PostgresDeadLetterMigrations.CreateModule);

    /// <summary>
    /// Adds the PostgreSQL delayed message store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresDelayedMessageStore(
        this IServiceCollection services,
        Action<PostgresDelayedMessageStoreOptions>? configure = null
    ) =>
        services.AddPostgresStore<
            IDelayedMessageStore,
            PostgresDelayedMessageStore,
            PostgresDelayedMessageStoreOptions
        >(configure, PostgresDelayedMessageMigrations.CreateModule);

    /// <summary>
    /// Adds the PostgreSQL revocation store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresRevocationStore(
        this IServiceCollection services,
        Action<PostgresRevocationStoreOptions>? configure = null
    ) =>
        services.AddPostgresStore<
            IRevocationStore,
            PostgresRevocationStore,
            PostgresRevocationStoreOptions
        >(configure, PostgresRevocationMigrations.CreateModule);

    /// <summary>
    /// Adds the PostgreSQL rate limiter.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresRateLimiter(
        this IServiceCollection services,
        Action<PostgresRateLimiterOptions>? configure = null
    ) =>
        services.AddPostgresStore<IRateLimiter, PostgresRateLimiter, PostgresRateLimiterOptions>(
            configure,
            PostgresRateLimiterMigrations.CreateModule
        );

    /// <summary>
    /// Adds the PostgreSQL signal store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresSignalStore(
        this IServiceCollection services,
        Action<PostgresSignalStoreOptions>? configure = null
    ) =>
        services.AddPostgresStore<ISignalStore, PostgresSignalStore, PostgresSignalStoreOptions>(
            configure,
            PostgresSignalMigrations.CreateModule
        );

    /// <summary>
    /// Adds the PostgreSQL batch store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresBatchStore(
        this IServiceCollection services,
        Action<PostgresBatchStoreOptions>? configure = null
    ) =>
        services.AddPostgresStore<IBatchStore, PostgresBatchStore, PostgresBatchStoreOptions>(
            configure,
            PostgresBatchMigrations.CreateModule
        );

    /// <summary>
    /// Adds the PostgreSQL saga store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresSagaStore(
        this IServiceCollection services,
        Action<PostgresSagaStoreOptions>? configure = null
    ) =>
        services.AddPostgresStore<ISagaStore, PostgresSagaStore, PostgresSagaStoreOptions>(
            configure,
            PostgresSagaMigrations.CreateModule
        );

    /// <summary>
    /// Adds the PostgreSQL partition lock store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresPartitionLockStore(
        this IServiceCollection services,
        Action<PostgresPartitionLockStoreOptions>? configure = null
    ) =>
        services.AddPostgresStore<
            IPartitionLockStore,
            PostgresPartitionLockStore,
            PostgresPartitionLockStoreOptions
        >(configure, PostgresPartitionLockMigrations.CreateModule);

    /// <summary>
    /// Adds the PostgreSQL task execution tracker.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresTaskExecutionTracker(
        this IServiceCollection services,
        Action<PostgresTaskExecutionTrackerOptions>? configure = null
    ) =>
        services.AddPostgresStore<
            ITaskExecutionTracker,
            PostgresTaskExecutionTracker,
            PostgresTaskExecutionTrackerOptions
        >(configure, PostgresTaskExecutionTrackerMigrations.CreateModule);

    /// <summary>
    /// Adds the PostgreSQL queue metrics store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresQueueMetrics(
        this IServiceCollection services,
        Action<PostgresQueueMetricsOptions>? configure = null
    ) =>
        services.AddPostgresStore<IQueueMetrics, PostgresQueueMetrics, PostgresQueueMetricsOptions>(
            configure,
            PostgresQueueMetricsMigrations.CreateModule
        );

    /// <summary>
    /// Adds the PostgreSQL historical data store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddPostgresHistoricalDataStore(
        this IServiceCollection services,
        Action<PostgresHistoricalDataStoreOptions>? configure = null
    ) =>
        services.AddPostgresStore<
            IHistoricalDataStore,
            PostgresHistoricalDataStore,
            PostgresHistoricalDataStoreOptions
        >(configure, PostgresHistoricalDataMigrations.CreateModule);

    private static IServiceCollection AddPostgresStore<TService, TImplementation, TOptions>(
        this IServiceCollection services,
        Action<TOptions>? configure,
        Func<TOptions, SqlMigrationModule> createModule
    )
        where TService : class
        where TImplementation : class, TService
        where TOptions : class
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddPostgresMigrations();
        services.AddSingleton<TService, TImplementation>();
        services.AddSingleton(sp =>
            createModule(sp.GetRequiredService<IOptions<TOptions>>().Value)
        );

        return services;
    }
}
