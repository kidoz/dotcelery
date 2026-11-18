using Asp.Versioning;
using DotCelery.Core.Dashboard;
using DotCelery.Core.MultiTenancy;
using DotCelery.Core.Signals;
using DotCelery.Dashboard.Controllers;
using DotCelery.Dashboard.Hubs;
using DotCelery.Dashboard.Middleware;
using DotCelery.Dashboard.Routing;
using DotCelery.Dashboard.Security;
using DotCelery.Dashboard.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Controllers;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

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
        services.AddOptions<DashboardOptions>();

        if (configure is not null)
        {
            services.Configure(configure);
        }

        // Register dashboard services. A registry the application already registered, such as a
        // shared one, wins over the in-process default
        services.TryAddSingleton<IWorkerRegistry, InMemoryWorkerRegistry>();
        AddDashboardServices(services);

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
        services.AddOptions<DashboardOptions>();

        if (configure is not null)
        {
            services.Configure(configure);
        }

        // Register dashboard services with custom worker registry
        services.AddSingleton<IWorkerRegistry, TWorkerRegistry>();
        AddDashboardServices(services);

        return services;
    }

    private static void AddDashboardServices(IServiceCollection services)
    {
        services.AddScoped<IDashboardDataProvider, DashboardDataService>();
        services.AddSingleton<DashboardNotificationService>();
        services.AddScoped<DashboardAuthorizationFilter>();
        services.AddScoped<DashboardTenantFilter>();
        services.AddSingleton<DashboardTenantHubFilter>();
        services.AddScoped<DashboardRequestVerificationFilter>();

        // The task record store indexes the tasks this host runs, so the dashboard can list and
        // count them, and the task metrics store keeps their per-name totals for the history;
        // the providers serve queue stats, task queries, and metrics from the stores the
        // application already has (it does not need to implement them)
        services.AddSingleton<TaskRecordStore>();
        services.AddSingleton<TaskMetricsStore>();
        services.AddSingleton<ITaskSummaryProvider, DashboardTaskSummaryProvider>();
        services.AddSingleton<IQueueStatsProvider, DashboardQueueStatsProvider>();
        services.AddSingleton<IMetricsProvider, DashboardMetricsProvider>();

        // Task signals feed the records and the SignalR notifications; the monitor watches the
        // worker registry and announces workers coming and going
        services.AddScoped<DashboardTaskSignalHandler>();
        services.AddScoped<ITaskSignalHandler<AfterTaskPublishSignal>>(sp =>
            sp.GetRequiredService<DashboardTaskSignalHandler>()
        );
        services.AddScoped<ITaskSignalHandler<TaskPreRunSignal>>(sp =>
            sp.GetRequiredService<DashboardTaskSignalHandler>()
        );
        services.AddScoped<ITaskSignalHandler<TaskPostRunSignal>>(sp =>
            sp.GetRequiredService<DashboardTaskSignalHandler>()
        );
        services.AddScoped<ITaskSignalHandler<TaskFailureSignal>>(sp =>
            sp.GetRequiredService<DashboardTaskSignalHandler>()
        );
        services.AddScoped<ITaskSignalHandler<TaskRejectedSignal>>(sp =>
            sp.GetRequiredService<DashboardTaskSignalHandler>()
        );
        services.AddHostedService<TenantHostedService<DashboardWorkerMonitor>>();
        services.AddHostedService<TenantHostedService<DashboardMetricsCollector>>();

        // Add SignalR
        services.AddSignalR(options => options.AddFilter<DashboardTenantHubFilter>());

        // Add controllers for API endpoints
        services.AddProblemDetails();
        services
            .AddApiVersioning(options =>
                options.ApiVersionReader = new UrlSegmentApiVersionReader()
            )
            .AddMvc()
            .AddApiExplorer(options =>
            {
                options.GroupNameFormat = "'v'VVV";
                options.SubstituteApiVersionInUrl = true;
            });

        services
            .AddControllers(options =>
            {
                options.Filters.AddService<DashboardRequestVerificationFilter>();
                options.Filters.AddService<DashboardAuthorizationFilter>();
                options.Filters.AddService<DashboardTenantFilter>();
            })
            .AddApplicationPart(typeof(DashboardExtensions).Assembly);

        services
            .AddOptions<MvcOptions>()
            .Configure<IOptions<DashboardOptions>>(
                (mvc, dashboard) =>
                    mvc.Conventions.Add(
                        new DashboardRoutePrefixConvention(dashboard.Value.PathPrefix)
                    )
            );
    }

    /// <summary>Registers the dashboard UI middleware using the configured options.</summary>
    /// <param name="app">The application builder.</param>
    /// <returns>The application builder for chaining.</returns>
    public static IApplicationBuilder UseDotCeleryDashboard(this IApplicationBuilder app) =>
        app.UseMiddleware<DashboardMiddleware>();

    /// <summary>
    /// Maps the DotCelery Dashboard middleware and endpoints.
    /// </summary>
    /// <param name="app">The application builder.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The application builder for chaining.</returns>
    [Obsolete("Configure the dashboard in AddDotCeleryDashboard before building the application.")]
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

        return app.UseDotCeleryDashboard();
    }

    /// <summary>Maps the SignalR hub under the configured dashboard prefix.</summary>
    public static HubEndpointConventionBuilder MapDotCeleryDashboardHub(
        this IEndpointRouteBuilder endpoints
    ) => endpoints.MapHub<DashboardHub>($"{ConfiguredPrefix(endpoints)}/hub");

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
    /// <returns>The dashboard API and hub endpoint conventions.</returns>
    /// <remarks>Registers attribute-routed controllers in the host through <c>MapControllers()</c>.</remarks>
    public static IEndpointConventionBuilder MapDotCeleryDashboard(
        this IEndpointRouteBuilder endpoints
    )
    {
        var controllers = endpoints.MapControllers();
        return new DashboardEndpointConventions(controllers, endpoints.MapDotCeleryDashboardHub());
    }

    /// <summary>Maps the dashboard API and the hub using an explicit hub prefix.</summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <param name="pathPrefix">The path prefix for the dashboard (default: /celery).</param>
    /// <returns>The endpoint route builder for chaining.</returns>
    /// <remarks>Registers attribute-routed controllers in the host through <c>MapControllers()</c>.</remarks>
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

    private static string ConfiguredPrefix(IEndpointRouteBuilder endpoints) =>
        endpoints
            .ServiceProvider.GetRequiredService<IOptions<DashboardOptions>>()
            .Value.NormalizedPathPrefix;

    private sealed class DashboardEndpointConventions(
        IEndpointConventionBuilder controllers,
        IEndpointConventionBuilder hub
    ) : IEndpointConventionBuilder
    {
        public void Add(Action<EndpointBuilder> convention)
        {
            hub.Add(convention);
            controllers.Add(endpoint => ApplyToDashboard(endpoint, convention));
        }

        public void Finally(Action<EndpointBuilder> convention)
        {
            hub.Finally(convention);
            controllers.Finally(endpoint => ApplyToDashboard(endpoint, convention));
        }

        private static void ApplyToDashboard(
            EndpointBuilder endpoint,
            Action<EndpointBuilder> convention
        )
        {
            if (
                endpoint
                    .Metadata.OfType<ControllerActionDescriptor>()
                    .Any(action =>
                        action.ControllerTypeInfo.AsType() == typeof(DashboardApiController)
                    )
            )
                convention(endpoint);
        }
    }
}
