using DotCelery.Core.Abstractions;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.DependencyInjection;

namespace DotCelery.Core.Extensions;

/// <summary>
/// Extension methods for configuring DotCelery services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds DotCelery core services.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configuration action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddDotCelery(
        this IServiceCollection services,
        Action<DotCeleryBuilder> configure
    )
    {
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new DotCeleryBuilder(services);
        configure(builder);
        return services;
    }
}

/// <summary>
/// Builder for configuring DotCelery services.
/// </summary>
public sealed class DotCeleryBuilder
{
    private readonly List<Type> _taskTypes = [];

    /// <summary>
    /// Initializes a new instance of the <see cref="DotCeleryBuilder"/> class.
    /// </summary>
    /// <param name="services">The service collection.</param>
    public DotCeleryBuilder(IServiceCollection services)
    {
        Services = services;

        // Register default serializer
        services.AddSingleton<IMessageSerializer, JsonMessageSerializer>();
    }

    /// <summary>
    /// Gets the service collection.
    /// </summary>
    public IServiceCollection Services { get; }

    /// <summary>
    /// Gets the registered task types.
    /// </summary>
    public IReadOnlyList<Type> TaskTypes => _taskTypes;

    /// <summary>
    /// Configures the message broker.
    /// </summary>
    /// <typeparam name="TBroker">The broker type.</typeparam>
    /// <returns>The builder.</returns>
    public DotCeleryBuilder UseBroker<TBroker>()
        where TBroker : class, IMessageBroker
    {
        Services.AddSingleton<IMessageBroker, TBroker>();
        return this;
    }

    /// <summary>
    /// Configures the message broker with a factory.
    /// </summary>
    /// <typeparam name="TBroker">The broker type.</typeparam>
    /// <param name="factory">The factory function.</param>
    /// <returns>The builder.</returns>
    public DotCeleryBuilder UseBroker<TBroker>(Func<IServiceProvider, TBroker> factory)
        where TBroker : class, IMessageBroker
    {
        Services.AddSingleton<IMessageBroker>(factory);
        return this;
    }

    /// <summary>
    /// Configures the result backend.
    /// </summary>
    /// <typeparam name="TBackend">The backend type.</typeparam>
    /// <returns>The builder.</returns>
    public DotCeleryBuilder UseBackend<TBackend>()
        where TBackend : class, IResultBackend
    {
        Services.AddSingleton<IResultBackend, TBackend>();
        return this;
    }

    /// <summary>
    /// Configures the result backend with a factory.
    /// </summary>
    /// <typeparam name="TBackend">The backend type.</typeparam>
    /// <param name="factory">The factory function.</param>
    /// <returns>The builder.</returns>
    public DotCeleryBuilder UseBackend<TBackend>(Func<IServiceProvider, TBackend> factory)
        where TBackend : class, IResultBackend
    {
        Services.AddSingleton<IResultBackend>(factory);
        return this;
    }

    /// <summary>
    /// Configures the message serializer.
    /// </summary>
    /// <typeparam name="TSerializer">The serializer type.</typeparam>
    /// <returns>The builder.</returns>
    public DotCeleryBuilder UseSerializer<TSerializer>()
        where TSerializer : class, IMessageSerializer
    {
        // Remove default serializer and add custom one
        var descriptor = Services.FirstOrDefault(d => d.ServiceType == typeof(IMessageSerializer));
        if (descriptor is not null)
        {
            Services.Remove(descriptor);
        }

        Services.AddSingleton<IMessageSerializer, TSerializer>();
        return this;
    }

    /// <summary>
    /// Registers a task type.
    /// </summary>
    /// <typeparam name="TTask">The task type.</typeparam>
    /// <returns>The builder.</returns>
    public DotCeleryBuilder AddTask<TTask>()
        where TTask : class, ITask
    {
        Services.AddTransient<TTask>();
        _taskTypes.Add(typeof(TTask));
        return this;
    }

    /// <summary>
    /// Registers task types from an assembly.
    /// </summary>
    /// <param name="assembly">The assembly to scan.</param>
    /// <returns>The builder.</returns>
    public DotCeleryBuilder AddTasksFromAssembly(System.Reflection.Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);

        var taskTypes = assembly
            .GetTypes()
            .Where(t => !t.IsAbstract && !t.IsInterface)
            .Where(t =>
                t.GetInterfaces()
                    .Any(i =>
                        i == typeof(ITask)
                        || (
                            i.IsGenericType
                            && (
                                i.GetGenericTypeDefinition() == typeof(ITask<>)
                                || i.GetGenericTypeDefinition() == typeof(ITask<,>)
                            )
                        )
                    )
            );

        foreach (var taskType in taskTypes)
        {
            Services.AddTransient(taskType);
            _taskTypes.Add(taskType);
        }

        return this;
    }
}
