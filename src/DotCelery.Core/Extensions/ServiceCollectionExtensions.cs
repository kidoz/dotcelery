using System.Diagnostics.CodeAnalysis;
using DotCelery.Core.Abstractions;
using DotCelery.Core.MultiTenancy;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage;
using DotCelery.Core.Storage.Stores;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

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

    /// <summary>
    /// Adds the <see cref="StoragePurgeService"/>, which deletes expired entries from the
    /// registered <see cref="IStorageProvider"/>, unless it is already added.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddStoragePurge(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, StoragePurgeService>()
        );
        return services;
    }

    /// <summary>
    /// Adds tenant queue routing, which maps a task's tenant to its queue.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Optional configuration action for multi-tenancy options.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddTenantRouting(
        this DotCeleryBuilder builder,
        Action<MultiTenancyOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        builder.Services.AddSingleton<ITenantRouter, TenantRouter>();
        builder.Services.AddSingleton<ITenantContext>(TenantContext.Current);
        return builder;
    }
}

/// <summary>
/// Builder for configuring DotCelery services.
/// </summary>
public sealed class DotCeleryBuilder
{
    private readonly List<TaskTypeRegistration> _taskTypes = [];

    /// <summary>
    /// Initializes a new instance of the <see cref="DotCeleryBuilder"/> class.
    /// </summary>
    /// <param name="services">The service collection.</param>
    public DotCeleryBuilder(IServiceCollection services)
    {
        Services = services;

        // Register default serializer, configured through JsonMessageSerializerOptions
        services.AddSingleton<IMessageSerializer>(provider => new JsonMessageSerializer(
            jsonOptions: null,
            provider.GetService<IOptions<JsonMessageSerializerOptions>>()?.Value
        ));
    }

    /// <summary>
    /// Gets the service collection.
    /// </summary>
    public IServiceCollection Services { get; }

    /// <summary>
    /// Gets the registered task types.
    /// </summary>
    public IReadOnlyList<Type> TaskTypes => [.. _taskTypes.Select(task => task.TaskType)];

    /// <summary>
    /// Gets the registered task types with the names they route by, so the worker's registry is
    /// built without reading a name through reflection.
    /// </summary>
    public IReadOnlyList<TaskTypeRegistration> TaskRegistrations => _taskTypes;

    /// <summary>
    /// Configures the message broker.
    /// </summary>
    /// <typeparam name="TBroker">The broker type.</typeparam>
    /// <returns>The builder.</returns>
    public DotCeleryBuilder UseBroker<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TBroker
    >()
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
    public DotCeleryBuilder UseBackend<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TBackend
    >()
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
    public DotCeleryBuilder UseSerializer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TSerializer
    >()
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
    /// <exception cref="InvalidOperationException">
    /// Thrown when the task type's static <see cref="ITask.TaskName"/> is null or empty.
    /// </exception>
    public DotCeleryBuilder AddTask<
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicConstructors
                | DynamicallyAccessedMemberTypes.Interfaces
        )]
            TTask
    >()
        where TTask : class, ITask
    {
        if (string.IsNullOrEmpty(TTask.TaskName))
        {
            throw new InvalidOperationException(
                $"Task type '{typeof(TTask).FullName}' cannot be registered: its static "
                    + $"{nameof(ITask)}.{nameof(ITask.TaskName)} property must return a non-empty name."
            );
        }

        return AddTask<TTask>(TTask.TaskName);
    }

    /// <summary>
    /// Registers a task type under the name it routes by.
    /// </summary>
    /// <remarks>
    /// A source-generated registration calls this overload with the task's static
    /// <see cref="ITask.TaskName"/>, so the name is known without reading it through reflection,
    /// with the generated adapter that invokes the task without expression compilation, and
    /// with the input and output types it knows at generation time, so the worker's registry
    /// does not inspect the task's interfaces.
    /// </remarks>
    /// <typeparam name="TTask">The task type.</typeparam>
    /// <param name="taskName">The name the task routes by.</param>
    /// <param name="invoker">The typed adapter that invokes the task, when one was generated.</param>
    /// <param name="inputType">
    /// The input type, when it is known without inspecting the task type; null lets the registry
    /// resolve it.
    /// </param>
    /// <param name="outputType">
    /// The output type, when it is known without inspecting the task type; null lets the registry
    /// resolve it.
    /// </param>
    /// <returns>The builder.</returns>
    public DotCeleryBuilder AddTask<
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicConstructors
                | DynamicallyAccessedMemberTypes.Interfaces
        )]
            TTask
    >(
        string taskName,
        ITaskInvoker? invoker = null,
        Type? inputType = null,
        Type? outputType = null
    )
        where TTask : class, ITask
    {
        ArgumentException.ThrowIfNullOrEmpty(taskName);

        Services.AddTransient<TTask>();
        _taskTypes.Add(
            new TaskTypeRegistration(typeof(TTask), taskName, invoker, inputType, outputType)
        );
        return this;
    }

    /// <summary>
    /// Registers task types from an assembly.
    /// </summary>
    /// <param name="assembly">The assembly to scan.</param>
    /// <returns>The builder.</returns>
    /// <remarks>
    /// The scan reads the assembly's types through reflection, so a Native AOT or trimmed
    /// application registers its tasks with the source-generated registration instead.
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    /// Thrown when a discovered task type's static <see cref="ITask.TaskName"/> is missing,
    /// null, or empty.
    /// </exception>
    [RequiresUnreferencedCode(
        "Scanning an assembly for task types needs reflection; a Native AOT or trimmed application uses the source-generated task registration."
    )]
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
            var taskName =
                taskType
                    .GetProperty(
                        nameof(ITask.TaskName),
                        System.Reflection.BindingFlags.Public
                            | System.Reflection.BindingFlags.Static
                            | System.Reflection.BindingFlags.FlattenHierarchy
                    )
                    ?.GetValue(null) as string;

            if (string.IsNullOrEmpty(taskName))
            {
                throw new InvalidOperationException(
                    $"Task type '{taskType.FullName}' in assembly '{assembly.GetName().Name}' "
                        + $"cannot be registered: its static {nameof(ITask)}.{nameof(ITask.TaskName)} "
                        + "property must return a non-empty name."
                );
            }

            Services.AddTransient(taskType);
            _taskTypes.Add(new TaskTypeRegistration(taskType, taskName));
        }

        return this;
    }
}
