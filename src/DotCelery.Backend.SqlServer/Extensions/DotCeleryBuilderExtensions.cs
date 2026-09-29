using DotCelery.Backend.SqlServer.Storage;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Extensions;
using Microsoft.Extensions.DependencyInjection;

namespace DotCelery.Backend.SqlServer.Extensions;

/// <summary>
/// Extension methods for configuring SQL Server backend with DotCeleryBuilder.
/// </summary>
public static class DotCeleryBuilderExtensions
{
    /// <summary>
    /// Configures the SQL Server result backend.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The builder for chaining.</returns>
    public static DotCeleryBuilder UseSqlServer(
        this DotCeleryBuilder builder,
        Action<SqlServerStorageOptions>? configure = null
    )
    {
        // Remove any existing backend registration
        var existingBackend = builder.Services.FirstOrDefault(d =>
            d.ServiceType == typeof(IResultBackend)
        );
        if (existingBackend is not null)
        {
            builder.Services.Remove(existingBackend);
        }

        builder.Services.AddSqlServerBackend(configure);

        return builder;
    }

    /// <summary>
    /// Configures the SQL Server result backend with a connection string.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="connectionString">The SQL Server connection string.</param>
    /// <returns>The builder for chaining.</returns>
    public static DotCeleryBuilder UseSqlServer(
        this DotCeleryBuilder builder,
        string connectionString
    )
    {
        return builder.UseSqlServer(options =>
        {
            options.ConnectionString = connectionString;
        });
    }
}
