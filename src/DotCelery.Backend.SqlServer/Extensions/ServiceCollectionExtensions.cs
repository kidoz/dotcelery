using DotCelery.Backend.SqlServer.Storage;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Batches;
using DotCelery.Core.Execution;
using DotCelery.Core.Extensions;
using DotCelery.Core.Partitioning;
using DotCelery.Core.Sagas;
using DotCelery.Core.Storage;
using DotCelery.Core.Storage.Stores;
using DotCelery.Storage.Sql;
using DotCelery.Storage.Sql.Execution;
using DotCelery.Storage.Sql.Extensions;
using DotCelery.Storage.Sql.Migrations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace DotCelery.Backend.SqlServer.Extensions;

/// <summary>
/// Extension methods for registering SQL Server stores.
/// </summary>
/// <remarks>
/// <para>
/// Each store registration also registers the migrations for the store's tables. Pending
/// migrations are applied when the host starts (see <see cref="AddSqlServerMigrations"/>).
/// Stores registered another way have no tables unless their migrations are registered too.
/// </para>
/// <para>
/// The delayed message, revocation, outbox, inbox, signal, partition lock, execution tracking
/// and rate limiting stores share the storage primitives added by
/// <see cref="AddSqlServerStorage"/>, so they share one <see cref="SqlServerStorageOptions"/>
/// whichever registration configures it. Their retention and timing are configured with
/// <see cref="StorageStoreOptions"/>.
/// </para>
/// </remarks>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the provider that shares one data source (connection pool) per connection string.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddSqlServerDataSourceProvider(
        this IServiceCollection services
    )
    {
        services.TryAddSingleton<SqlServerDataSourceProvider>();
        return services;
    }

    /// <summary>
    /// Adds the migrator that applies the registered migration modules with the SQL Server
    /// dialect, and runs it when the host starts unless
    /// <see cref="SqlMigrationOptions.RunAtStartup"/> is disabled. Without a host, resolve
    /// <see cref="SqlMigrator"/> and call <see cref="SqlMigrator.MigrateAsync"/> before using
    /// the stores.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional migration options configuration.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddSqlServerMigrations(
        this IServiceCollection services,
        Action<SqlMigrationOptions>? configure = null
    )
    {
        services.AddSqlServerDataSourceProvider();
        services.TryAddSingleton<SqlDialect>(SqlServerDialect.Instance);
        services.TryAddSingleton<ISqlDataSourceProvider>(sp =>
            sp.GetRequiredService<SqlServerDataSourceProvider>()
        );
        services.AddSqlMigrations(configure);

        return services;
    }

    /// <summary>
    /// Adds the SQL Server storage primitives (<see cref="IStorageProvider"/>), the migrations
    /// for their tables, and the <see cref="StoragePurgeService"/> that deletes expired rows.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddSqlServerStorage(
        this IServiceCollection services,
        Action<SqlServerStorageOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddSqlServerMigrations();
        services.TryAddSingleton(sp =>
        {
            var options = sp.GetRequiredService<IOptions<SqlServerStorageOptions>>().Value;
            return SqlServerStorage.CreateProvider(
                sp.GetRequiredService<SqlServerDataSourceProvider>()
                    .GetDataSource(options.ConnectionString),
                options,
                sp.GetService<TimeProvider>()
            );
        });
        services.AddSingleton(sp =>
            SqlServerStorage.CreateModule(
                sp.GetRequiredService<IOptions<SqlServerStorageOptions>>().Value
            )
        );
        services.AddStoragePurge();

        return services;
    }

    /// <summary>
    /// Adds the SQL Server result backend.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddSqlServerBackend(
        this IServiceCollection services,
        Action<SqlServerStorageOptions>? configure = null
    ) => services.AddSqlServerStorageStore<IResultBackend, ResultBackend>(configure);

    /// <summary>
    /// Adds the SQL Server result backend with a connection string.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The SQL Server connection string.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddSqlServerBackend(
        this IServiceCollection services,
        string connectionString
    ) =>
        services.AddSqlServerBackend(options =>
        {
            options.ConnectionString = connectionString;
        });

    /// <summary>
    /// Adds the SQL Server outbox store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddSqlServerOutboxStore(
        this IServiceCollection services,
        Action<SqlServerStorageOptions>? configure = null
    ) => services.AddSqlServerStorageStore<IOutboxStore, OutboxStore>(configure);

    /// <summary>
    /// Adds the SQL Server inbox store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddSqlServerInboxStore(
        this IServiceCollection services,
        Action<SqlServerStorageOptions>? configure = null
    ) => services.AddSqlServerStorageStore<IInboxStore, InboxStore>(configure);

    /// <summary>
    /// Adds the SQL Server dead letter store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddSqlServerDeadLetterStore(
        this IServiceCollection services,
        Action<SqlServerStorageOptions>? configure = null
    ) => services.AddSqlServerStorageStore<IDeadLetterStore, DeadLetterStore>(configure);

    /// <summary>
    /// Adds the SQL Server delayed message store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddSqlServerDelayedMessageStore(
        this IServiceCollection services,
        Action<SqlServerStorageOptions>? configure = null
    ) => services.AddSqlServerStorageStore<IDelayedMessageStore, DelayedMessageStore>(configure);

    /// <summary>
    /// Adds the SQL Server revocation store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddSqlServerRevocationStore(
        this IServiceCollection services,
        Action<SqlServerStorageOptions>? configure = null
    ) => services.AddSqlServerStorageStore<IRevocationStore, RevocationStore>(configure);

    /// <summary>
    /// Adds the SQL Server rate limiter.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddSqlServerRateLimiter(
        this IServiceCollection services,
        Action<SqlServerStorageOptions>? configure = null
    ) => services.AddSqlServerStorageStore<IRateLimiter, WindowRateLimiter>(configure);

    /// <summary>
    /// Adds the SQL Server signal store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddSqlServerSignalStore(
        this IServiceCollection services,
        Action<SqlServerStorageOptions>? configure = null
    ) => services.AddSqlServerStorageStore<ISignalStore, SignalStore>(configure);

    /// <summary>
    /// Adds the SQL Server batch store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddSqlServerBatchStore(
        this IServiceCollection services,
        Action<SqlServerStorageOptions>? configure = null
    ) => services.AddSqlServerStorageStore<IBatchStore, BatchStore>(configure);

    /// <summary>
    /// Adds the SQL Server saga store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddSqlServerSagaStore(
        this IServiceCollection services,
        Action<SqlServerStorageOptions>? configure = null
    ) => services.AddSqlServerStorageStore<ISagaStore, SagaStore>(configure);

    /// <summary>
    /// Adds the SQL Server partition lock store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddSqlServerPartitionLockStore(
        this IServiceCollection services,
        Action<SqlServerStorageOptions>? configure = null
    ) => services.AddSqlServerStorageStore<IPartitionLockStore, PartitionLockStore>(configure);

    /// <summary>
    /// Adds the SQL Server task execution tracker.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddSqlServerTaskExecutionTracker(
        this IServiceCollection services,
        Action<SqlServerStorageOptions>? configure = null
    ) => services.AddSqlServerStorageStore<ITaskExecutionTracker, TaskExecutionTracker>(configure);

    /// <summary>
    /// Adds the SQL Server queue metrics store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddSqlServerQueueMetrics(
        this IServiceCollection services,
        Action<SqlServerStorageOptions>? configure = null
    ) => services.AddSqlServerStorageStore<IQueueMetrics, QueueMetrics>(configure);

    /// <summary>
    /// Adds the SQL Server historical data store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddSqlServerHistoricalDataStore(
        this IServiceCollection services,
        Action<SqlServerStorageOptions>? configure = null
    ) => services.AddSqlServerStorageStore<IHistoricalDataStore, HistoricalDataStore>(configure);

    private static IServiceCollection AddSqlServerStorageStore<TService, TImplementation>(
        this IServiceCollection services,
        Action<SqlServerStorageOptions>? configure
    )
        where TService : class
        where TImplementation : class, TService
    {
        services.AddSqlServerStorage(configure);
        services.AddSingleton<TService, TImplementation>();

        return services;
    }
}
