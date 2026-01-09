using DotCelery.Core.Abstractions;
using DotCelery.Core.Extensions;
using Microsoft.Extensions.DependencyInjection;

namespace DotCelery.Backend.Mongo.Extensions;

/// <summary>
/// Extension methods for configuring MongoDB backend with DotCeleryBuilder.
/// </summary>
public static class DotCeleryBuilderExtensions
{
    /// <summary>
    /// Configures the MongoDB result backend.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The builder for chaining.</returns>
    public static DotCeleryBuilder UseMongo(
        this DotCeleryBuilder builder,
        Action<MongoBackendOptions>? configure = null
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

        builder.Services.AddSingleton<IResultBackend, MongoResultBackend>();

        return builder;
    }

    /// <summary>
    /// Configures the MongoDB result backend with a connection string.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <param name="databaseName">The database name.</param>
    /// <returns>The builder for chaining.</returns>
    public static DotCeleryBuilder UseMongo(
        this DotCeleryBuilder builder,
        string connectionString,
        string databaseName = "celery"
    )
    {
        return builder.UseMongo(options =>
        {
            options.ConnectionString = connectionString;
            options.DatabaseName = databaseName;
        });
    }
}
