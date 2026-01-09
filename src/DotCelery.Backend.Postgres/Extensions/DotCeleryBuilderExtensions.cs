using DotCelery.Core.Abstractions;
using DotCelery.Core.Extensions;
using Microsoft.Extensions.DependencyInjection;

namespace DotCelery.Backend.Postgres.Extensions;

/// <summary>
/// Extension methods for configuring PostgreSQL backend with DotCeleryBuilder.
/// </summary>
public static class DotCeleryBuilderExtensions
{
    /// <summary>
    /// Configures the PostgreSQL result backend.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The builder for chaining.</returns>
    public static DotCeleryBuilder UsePostgres(
        this DotCeleryBuilder builder,
        Action<PostgresBackendOptions>? configure = null
    )
    {
        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        // Remove any existing backend registration
        var existingBackend = builder.Services.FirstOrDefault(d =>
            d.ServiceType == typeof(IResultBackend)
        );
        if (existingBackend is not null)
        {
            builder.Services.Remove(existingBackend);
        }

        builder.Services.AddSingleton<IResultBackend, PostgresResultBackend>();

        return builder;
    }

    /// <summary>
    /// Configures the PostgreSQL result backend with a connection string.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <returns>The builder for chaining.</returns>
    public static DotCeleryBuilder UsePostgres(
        this DotCeleryBuilder builder,
        string connectionString
    )
    {
        return builder.UsePostgres(options =>
        {
            options.ConnectionString = connectionString;
        });
    }
}
