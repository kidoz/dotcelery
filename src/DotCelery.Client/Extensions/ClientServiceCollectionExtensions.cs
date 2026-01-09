using DotCelery.Client.Batches;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Extensions;
using DotCelery.Core.Routing;
using DotCelery.Core.Signals;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace DotCelery.Client.Extensions;

/// <summary>
/// Extension methods for configuring DotCelery client services.
/// </summary>
public static class ClientServiceCollectionExtensions
{
    /// <summary>
    /// Adds the Celery client to the service collection.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddClient(
        this DotCeleryBuilder builder,
        Action<CeleryClientOptions>? configure = null
    )
    {
        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }
        else
        {
            builder.Services.Configure<CeleryClientOptions>(_ => { });
        }

        // Register signal dispatcher if not already registered
        builder.Services.TryAddSingleton<ITaskSignalDispatcher, TaskSignalDispatcher>();

        // Register task router with configuration support
        builder.Services.TryAddSingleton<ITaskRouter>(sp =>
        {
            var router = new TaskRouter();
            var configurators = sp.GetServices<IConfigureTaskRouter>();
            foreach (var configurator in configurators)
            {
                configurator.Configure(router);
            }
            return router;
        });

        builder.Services.AddSingleton<ICeleryClient, CeleryClient>();

        return builder;
    }

    /// <summary>
    /// Adds a signal handler for task lifecycle events.
    /// </summary>
    /// <typeparam name="TSignal">The signal type to handle.</typeparam>
    /// <typeparam name="THandler">The handler implementation type.</typeparam>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddSignalHandler<TSignal, THandler>(
        this DotCeleryBuilder builder
    )
        where TSignal : ITaskSignal
        where THandler : class, ITaskSignalHandler<TSignal>
    {
        builder.Services.AddScoped<ITaskSignalHandler<TSignal>, THandler>();
        return builder;
    }

    /// <summary>
    /// Adds a signal handler for task lifecycle events using a factory.
    /// </summary>
    /// <typeparam name="TSignal">The signal type to handle.</typeparam>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="handlerFactory">Factory to create the handler.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddSignalHandler<TSignal>(
        this DotCeleryBuilder builder,
        Func<IServiceProvider, ITaskSignalHandler<TSignal>> handlerFactory
    )
        where TSignal : ITaskSignal
    {
        builder.Services.AddScoped(handlerFactory);
        return builder;
    }

    /// <summary>
    /// Adds a route for a specific task type to a queue.
    /// </summary>
    /// <typeparam name="TTask">The task type.</typeparam>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="queue">The target queue.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddRoute<TTask>(this DotCeleryBuilder builder, string queue)
        where TTask : ITask
    {
        // Ensure router is registered
        builder.Services.TryAddSingleton<ITaskRouter, TaskRouter>();

        // Configure route after router is built
        builder.Services.AddSingleton<IConfigureTaskRouter>(new ConfigureTaskRoute<TTask>(queue));

        return builder;
    }

    /// <summary>
    /// Adds a route pattern for tasks matching a pattern.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="pattern">The pattern to match. Use * for single segment, ** for multiple segments.</param>
    /// <param name="queue">The target queue.</param>
    /// <returns>The builder.</returns>
    /// <remarks>
    /// Pattern examples:
    /// - "reports.*" matches "reports.daily", "reports.weekly" (single segment)
    /// - "reports.**" matches "reports.email.send", "reports.pdf.generate" (multiple segments)
    /// </remarks>
    public static DotCeleryBuilder AddRoute(
        this DotCeleryBuilder builder,
        string pattern,
        string queue
    )
    {
        // Ensure router is registered
        builder.Services.TryAddSingleton<ITaskRouter, TaskRouter>();

        // Configure route after router is built
        builder.Services.AddSingleton<IConfigureTaskRouter>(
            new ConfigurePatternRoute(pattern, queue)
        );

        return builder;
    }

    /// <summary>
    /// Configures task routing with a configuration action.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Configuration action for the router.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder ConfigureRouting(
        this DotCeleryBuilder builder,
        Action<ITaskRouter> configure
    )
    {
        // Ensure router is registered
        builder.Services.TryAddSingleton<ITaskRouter, TaskRouter>();

        // Configure route after router is built
        builder.Services.AddSingleton<IConfigureTaskRouter>(
            new ConfigureTaskRouterAction(configure)
        );

        return builder;
    }

    /// <summary>
    /// Adds the batch client for creating and managing task batches.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    /// <remarks>
    /// Requires an IBatchStore implementation to be registered for batch state tracking.
    /// </remarks>
    public static DotCeleryBuilder AddBatchClient(this DotCeleryBuilder builder)
    {
        builder.Services.AddSingleton<IBatchClient, BatchClient>();
        return builder;
    }
}

/// <summary>
/// Marker interface for task router configuration.
/// </summary>
internal interface IConfigureTaskRouter
{
    void Configure(ITaskRouter router);
}

internal sealed class ConfigureTaskRoute<TTask> : IConfigureTaskRouter
    where TTask : ITask
{
    private readonly string _queue;

    public ConfigureTaskRoute(string queue) => _queue = queue;

    public void Configure(ITaskRouter router) => router.AddRoute<TTask>(_queue);
}

internal sealed class ConfigurePatternRoute : IConfigureTaskRouter
{
    private readonly string _pattern;
    private readonly string _queue;

    public ConfigurePatternRoute(string pattern, string queue)
    {
        _pattern = pattern;
        _queue = queue;
    }

    public void Configure(ITaskRouter router) => router.AddRoute(_pattern, _queue);
}

internal sealed class ConfigureTaskRouterAction : IConfigureTaskRouter
{
    private readonly Action<ITaskRouter> _configure;

    public ConfigureTaskRouterAction(Action<ITaskRouter> configure) => _configure = configure;

    public void Configure(ITaskRouter router) => _configure(router);
}
