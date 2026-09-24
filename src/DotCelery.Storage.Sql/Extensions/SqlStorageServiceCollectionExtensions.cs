using DotCelery.Storage.Sql.Migrations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace DotCelery.Storage.Sql.Extensions;

/// <summary>
/// Extension methods for registering SQL storage services.
/// </summary>
public static class SqlStorageServiceCollectionExtensions
{
    /// <summary>
    /// Adds the migrator that applies the registered <see cref="SqlMigrationModule"/>s, and
    /// runs it when the host starts unless <see cref="SqlMigrationOptions.RunAtStartup"/> is
    /// disabled. Without a host, resolve <see cref="SqlMigrator"/> and call
    /// <see cref="SqlMigrator.MigrateAsync"/> before using the stores.
    /// </summary>
    /// <remarks>
    /// A database package registers the <see cref="SqlDialect"/> and the
    /// <see cref="Execution.ISqlDataSourceProvider"/> that the migrator uses.
    /// </remarks>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional migration options configuration.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddSqlMigrations(
        this IServiceCollection services,
        Action<SqlMigrationOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions<SqlMigrationOptions>();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.TryAddSingleton<SqlMigrator>();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, SqlMigrationHostedService>()
        );

        return services;
    }
}
