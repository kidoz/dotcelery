using DotCelery.Core.Dashboard;
using DotCelery.Dashboard.Hubs;
using DotCelery.Dashboard.Middleware;
using DotCelery.Dashboard.Routing;
using DotCelery.Dashboard.Security;
using DotCelery.Dashboard.Services;

namespace DotCelery.Dashboard.Extensions;

/// <summary>
/// Extension methods for configuring DotCelery Dashboard.
/// </summary>
public static class DashboardExtensions
{
    /// <summary>
    /// Adds DotCelery Dashboard services to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddDotCeleryDashboard(
        this IServiceCollection services,
        Action<DashboardOptions>? configure = null
    )
    {
        var routeOptions = CreateConfiguredOptions(configure);

        services.AddOptions<DashboardOptions>();

        if (configure is not null)
        {
            services.Configure(configure);
        }

        // Register dashboard services
        services.AddSingleton<IWorkerRegistry, InMemoryWorkerRegistry>();
        services.AddScoped<IDashboardDataProvider, DashboardDataService>();
        services.AddSingleton<DashboardNotificationService>();
        services.AddScoped<DashboardAuthorizationFilter>();

        // Add SignalR
        services.AddSignalR();

        // Add controllers for API endpoints
        services
            .AddControllers(options =>
            {
                options.Conventions.Add(
                    new DashboardRoutePrefixConvention(routeOptions.PathPrefix)
                );
                options.Filters.AddService<DashboardAuthorizationFilter>();
            })
            .AddApplicationPart(typeof(DashboardExtensions).Assembly);

        return services;
    }

    /// <summary>
    /// Adds DotCelery Dashboard services with a custom worker registry.
    /// </summary>
    /// <typeparam name="TWorkerRegistry">The worker registry implementation type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddDotCeleryDashboard<TWorkerRegistry>(
        this IServiceCollection services,
        Action<DashboardOptions>? configure = null
    )
        where TWorkerRegistry : class, IWorkerRegistry
    {
        var routeOptions = CreateConfiguredOptions(configure);

        services.AddOptions<DashboardOptions>();

        if (configure is not null)
        {
            services.Configure(configure);
        }

        // Register dashboard services with custom worker registry
        services.AddSingleton<IWorkerRegistry, TWorkerRegistry>();
        services.AddScoped<IDashboardDataProvider, DashboardDataService>();
        services.AddSingleton<DashboardNotificationService>();
        services.AddScoped<DashboardAuthorizationFilter>();

        // Add SignalR
        services.AddSignalR();

        // Add controllers for API endpoints
        services
            .AddControllers(options =>
            {
                options.Conventions.Add(
                    new DashboardRoutePrefixConvention(routeOptions.PathPrefix)
                );
                options.Filters.AddService<DashboardAuthorizationFilter>();
            })
            .AddApplicationPart(typeof(DashboardExtensions).Assembly);

        return services;
    }

    /// <summary>
    /// Maps the DotCelery Dashboard middleware and endpoints.
    /// </summary>
    /// <param name="app">The application builder.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The application builder for chaining.</returns>
    public static IApplicationBuilder UseDotCeleryDashboard(
        this IApplicationBuilder app,
        Action<DashboardOptions>? configure = null
    )
    {
        if (configure is not null)
        {
            var options =
                app.ApplicationServices.GetService<Microsoft.Extensions.Options.IOptions<DashboardOptions>>();
            if (options is not null)
            {
                configure(options.Value);
            }
        }

        // Use the dashboard middleware
        app.UseMiddleware<DashboardMiddleware>();

        return app;
    }

    /// <summary>
    /// Maps the DotCelery Dashboard SignalR hub.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <param name="pathPrefix">The path prefix for the dashboard (default: /celery).</param>
    /// <returns>The endpoint route builder for chaining.</returns>
    public static IEndpointRouteBuilder MapDotCeleryDashboardHub(
        this IEndpointRouteBuilder endpoints,
        string pathPrefix = "/celery"
    )
    {
        var normalizedPrefix = pathPrefix.TrimEnd('/');
        endpoints.MapHub<DashboardHub>($"{normalizedPrefix}/hub");

        return endpoints;
    }

    /// <summary>
    /// Maps all DotCelery Dashboard endpoints (API controllers and SignalR hub).
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <param name="pathPrefix">The path prefix for the dashboard (default: /celery).</param>
    /// <returns>The endpoint route builder for chaining.</returns>
    public static IEndpointRouteBuilder MapDotCeleryDashboard(
        this IEndpointRouteBuilder endpoints,
        string pathPrefix = "/celery"
    )
    {
        var normalizedPrefix = pathPrefix.TrimEnd('/');

        // Map SignalR hub
        endpoints.MapHub<DashboardHub>($"{normalizedPrefix}/hub");

        // Map API controllers (they're already registered via AddControllers)
        endpoints.MapControllers();

        return endpoints;
    }

    private static DashboardOptions CreateConfiguredOptions(Action<DashboardOptions>? configure)
    {
        var options = new DashboardOptions();
        configure?.Invoke(options);
        return options;
    }
}
