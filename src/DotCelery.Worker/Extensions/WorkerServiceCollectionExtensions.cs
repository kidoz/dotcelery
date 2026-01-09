using DotCelery.Core.Abstractions;
using DotCelery.Core.Batches;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Execution;
using DotCelery.Core.Extensions;
using DotCelery.Core.Filters;
using DotCelery.Core.MultiTenancy;
using DotCelery.Core.Outbox;
using DotCelery.Core.Partitioning;
using DotCelery.Core.Security;
using DotCelery.Core.Signals;
using DotCelery.Worker.Batches;
using DotCelery.Worker.DeadLetter;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Filters;
using DotCelery.Worker.Registry;
using DotCelery.Worker.Resilience;
using DotCelery.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DotCelery.Worker.Extensions;

/// <summary>
/// Extension methods for configuring DotCelery worker services.
/// </summary>
public static class WorkerServiceCollectionExtensions
{
    /// <summary>
    /// Adds the Celery worker to the service collection.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddWorker(
        this DotCeleryBuilder builder,
        Action<WorkerOptions>? configure = null
    )
    {
        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }
        else
        {
            builder.Services.Configure<WorkerOptions>(_ => { });
        }

        // Configure filter options
        builder.Services.Configure<TaskFilterOptions>(_ => { });

        // Configure dead letter options
        builder.Services.Configure<DeadLetterOptions>(_ => { });

        // Register task registry with all registered tasks
        builder.Services.AddSingleton(sp =>
        {
            var logger = sp.GetService<ILoggerFactory>()?.CreateLogger<TaskRegistry>();
            var registry = new TaskRegistry(logger);

            foreach (var taskType in builder.TaskTypes)
            {
                // Get task name using static interface member
                var taskNameProperty = taskType.GetProperty(
                    "TaskName",
                    System.Reflection.BindingFlags.Public
                        | System.Reflection.BindingFlags.Static
                        | System.Reflection.BindingFlags.FlattenHierarchy
                );

                if (taskNameProperty is not null)
                {
                    var taskName = (string)taskNameProperty.GetValue(null)!;
                    registry.Register(taskType, taskName);
                }
            }

            return registry;
        });

        // Register core worker services
        builder.Services.AddSingleton<TaskFilterPipeline>();
        builder.Services.AddSingleton<ITaskSignalDispatcher, TaskSignalDispatcher>();
        builder.Services.AddSingleton<TimeLimitEnforcer>();
        builder.Services.AddSingleton<IDeadLetterHandler, DeadLetterHandler>();
        builder.Services.AddSingleton<IGracefulShutdownHandler, GracefulShutdownHandler>();
        builder.Services.AddSingleton<TaskExecutor>();
        builder.Services.AddSingleton<RevocationManager>();
        builder.Services.AddHostedService(sp => sp.GetRequiredService<RevocationManager>());
        builder.Services.AddHostedService<CeleryWorkerService>();

        return builder;
    }

    /// <summary>
    /// Adds a global task filter that applies to all tasks.
    /// </summary>
    /// <typeparam name="TFilter">The filter type.</typeparam>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddTaskFilter<TFilter>(this DotCeleryBuilder builder)
        where TFilter : class
    {
        // Register filter type for DI
        builder.Services.AddScoped<TFilter>();

        // Register filter with pipeline (will be added when pipeline is resolved)
        builder.Services.Configure<TaskFilterOptions>(options =>
        {
            options.GlobalFilterTypes.Add(typeof(TFilter));
        });

        return builder;
    }

    /// <summary>
    /// Adds a global task filter that applies to all tasks.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="filterType">The filter type.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddTaskFilter(this DotCeleryBuilder builder, Type filterType)
    {
        // Register filter type for DI
        builder.Services.AddScoped(filterType);

        // Register filter with pipeline
        builder.Services.Configure<TaskFilterOptions>(options =>
        {
            options.GlobalFilterTypes.Add(filterType);
        });

        return builder;
    }

    /// <summary>
    /// Adds the delayed message dispatcher to the worker.
    /// This enables efficient handling of ETA/countdown tasks using a delay store.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    /// <remarks>
    /// Requires an IDelayedMessageStore implementation to be registered.
    /// Enable by setting WorkerOptions.UseDelayQueue = true.
    /// </remarks>
    public static DotCeleryBuilder AddDelayedMessageDispatcher(this DotCeleryBuilder builder)
    {
        builder.Services.AddHostedService<DelayedMessageDispatcher>();
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
    /// Configures the dead letter queue options.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Configuration action for dead letter options.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder ConfigureDeadLetterQueue(
        this DotCeleryBuilder builder,
        Action<DeadLetterOptions> configure
    )
    {
        builder.Services.Configure(configure);
        return builder;
    }

    /// <summary>
    /// Enables batch completion tracking.
    /// This registers signal handlers that update batch state when tasks complete.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    /// <remarks>
    /// Requires an IBatchStore implementation to be registered.
    /// </remarks>
    public static DotCeleryBuilder AddBatchSupport(this DotCeleryBuilder builder)
    {
        // Register the batch completion handler
        builder.Services.AddScoped<BatchCompletionHandler>();

        // Register as signal handlers for relevant signals
        builder.Services.AddScoped<ITaskSignalHandler<TaskSuccessSignal>>(sp =>
            sp.GetRequiredService<BatchCompletionHandler>()
        );
        builder.Services.AddScoped<ITaskSignalHandler<TaskFailureSignal>>(sp =>
            sp.GetRequiredService<BatchCompletionHandler>()
        );
        builder.Services.AddScoped<ITaskSignalHandler<TaskRevokedSignal>>(sp =>
            sp.GetRequiredService<BatchCompletionHandler>()
        );
        builder.Services.AddScoped<ITaskSignalHandler<TaskRejectedSignal>>(sp =>
            sp.GetRequiredService<BatchCompletionHandler>()
        );

        return builder;
    }

    /// <summary>
    /// Adds kill switch functionality to the worker.
    /// The kill switch automatically stops consuming new messages when a failure threshold is reached.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder UseKillSwitch(
        this DotCeleryBuilder builder,
        Action<KillSwitchOptions>? configure = null
    )
    {
        builder.Services.Configure<KillSwitchOptions>(configure ?? (_ => { }));
        builder.Services.AddSingleton<IKillSwitch, KillSwitch>();
        return builder;
    }

    /// <summary>
    /// Adds circuit breaker functionality to the worker.
    /// Circuit breakers protect against cascading failures by temporarily disabling
    /// consumption when a queue experiences too many failures.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder UseCircuitBreaker(
        this DotCeleryBuilder builder,
        Action<CircuitBreakerOptions>? configure = null
    )
    {
        builder.Services.Configure<CircuitBreakerOptions>(configure ?? (_ => { }));
        builder.Services.AddSingleton<ICircuitBreakerFactory, CircuitBreakerFactory>();
        return builder;
    }

    /// <summary>
    /// Adds transactional outbox functionality to the worker.
    /// The outbox pattern ensures exactly-once message delivery by storing messages
    /// in the same transaction as the business operation.
    /// </summary>
    /// <typeparam name="TOutboxStore">The outbox store implementation type.</typeparam>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder UseOutbox<TOutboxStore>(
        this DotCeleryBuilder builder,
        Action<OutboxOptions>? configure = null
    )
        where TOutboxStore : class, IOutboxStore
    {
        builder.Services.Configure<OutboxOptions>(configure ?? (_ => { }));
        builder.Services.AddSingleton<IOutboxStore, TOutboxStore>();
        builder.Services.AddHostedService<OutboxDispatcher>();
        return builder;
    }

    /// <summary>
    /// Adds inbox deduplication functionality to the worker.
    /// The inbox pattern ensures exactly-once processing by tracking processed message IDs.
    /// </summary>
    /// <typeparam name="TInboxStore">The inbox store implementation type.</typeparam>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder UseInbox<TInboxStore>(
        this DotCeleryBuilder builder,
        Action<InboxOptions>? configure = null
    )
        where TInboxStore : class, IInboxStore
    {
        builder.Services.Configure<InboxOptions>(configure ?? (_ => { }));
        builder.Services.AddSingleton<IInboxStore, TInboxStore>();
        return builder;
    }

    /// <summary>
    /// Adds partitioned execution support to the worker.
    /// Messages with the same partition key will be processed sequentially.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The builder.</returns>
    /// <remarks>
    /// Requires an IPartitionLockStore implementation to be registered.
    /// </remarks>
    public static DotCeleryBuilder UsePartitionedExecution(
        this DotCeleryBuilder builder,
        Action<PartitionOptions>? configure = null
    )
    {
        builder.Services.Configure<PartitionOptions>(configure ?? (_ => { }));
        builder.AddTaskFilter<PartitionedExecutionFilter>();
        return builder;
    }

    /// <summary>
    /// Adds prevent-overlapping execution support to the worker.
    /// Tasks marked with <see cref="Core.Attributes.PreventOverlappingAttribute"/> will be skipped
    /// if another instance is already running.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    /// <remarks>
    /// Requires an ITaskExecutionTracker implementation to be registered.
    /// </remarks>
    public static DotCeleryBuilder UsePreventOverlapping(this DotCeleryBuilder builder)
    {
        builder.AddTaskFilter<PreventOverlappingFilter>();
        return builder;
    }

    /// <summary>
    /// Adds tenant context filter to the worker.
    /// Sets the tenant context for task execution based on message tenant ID.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder UseTenantContext(
        this DotCeleryBuilder builder,
        Action<MultiTenancyOptions>? configure = null
    )
    {
        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        builder.AddTaskFilter<TenantContextFilter>();
        return builder;
    }

    /// <summary>
    /// Adds queue metrics tracking to the worker.
    /// Tracks waiting, running, and processed counts per queue.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    /// <remarks>
    /// Requires an IQueueMetrics implementation to be registered.
    /// </remarks>
    public static DotCeleryBuilder UseQueueMetrics(this DotCeleryBuilder builder)
    {
        builder.AddTaskFilter<QueueMetricsFilter>();
        return builder;
    }

    /// <summary>
    /// Adds all Phase 8 advanced pattern filters to the worker.
    /// This includes partitioned execution, prevent-overlapping, tenant context, and queue metrics.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    /// <remarks>
    /// Requires the corresponding store implementations to be registered.
    /// Call AddInMemoryAdvancedPatterns() on the backend builder to register in-memory stores.
    /// </remarks>
    public static DotCeleryBuilder UseAdvancedPatternFilters(this DotCeleryBuilder builder)
    {
        return builder
            .UsePartitionedExecution()
            .UsePreventOverlapping()
            .UseTenantContext()
            .UseQueueMetrics();
    }

    /// <summary>
    /// Enables queued signal dispatch instead of inline signal handling.
    /// Signals will be enqueued to a signal queue and processed asynchronously by
    /// the SignalQueueProcessor background service.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The builder.</returns>
    /// <remarks>
    /// Requires an ISignalStore implementation to be registered (e.g., AddInMemorySignalQueue
    /// or AddRedisSignalQueue).
    /// </remarks>
    public static DotCeleryBuilder UseQueuedSignalDispatch(
        this DotCeleryBuilder builder,
        Action<SignalQueueProcessorOptions>? configure = null
    )
    {
        builder.Services.Configure<SignalQueueProcessorOptions>(configure ?? (_ => { }));

        // Replace the default signal dispatcher with the queued version
        var existingDispatcher = builder.Services.FirstOrDefault(d =>
            d.ServiceType == typeof(ITaskSignalDispatcher)
        );
        if (existingDispatcher is not null)
        {
            builder.Services.Remove(existingDispatcher);
        }

        builder.Services.AddSingleton<ITaskSignalDispatcher, QueuedTaskSignalDispatcher>();
        builder.Services.AddHostedService<SignalQueueProcessor>();

        return builder;
    }

    /// <summary>
    /// Enables message security validation for the worker.
    /// This adds payload size limits, task allowlists, schema version validation,
    /// and optional message signing.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Configuration action for security options.</param>
    /// <returns>The builder.</returns>
    /// <remarks>
    /// <para>
    /// Security features include:
    /// </para>
    /// <list type="bullet">
    /// <item>Payload size limits (DoS prevention)</item>
    /// <item>Task name allowlist (type confusion prevention)</item>
    /// <item>Schema version validation (wire-compatibility)</item>
    /// <item>Optional HMAC message signing (integrity verification)</item>
    /// </list>
    /// </remarks>
    public static DotCeleryBuilder UseMessageSecurity(
        this DotCeleryBuilder builder,
        Action<MessageSecurityOptions>? configure = null
    )
    {
        builder.Services.Configure<MessageSecurityOptions>(options =>
        {
            configure?.Invoke(options);
            options.Validate();
        });

        builder.Services.AddSingleton<IMessageSecurityValidator, MessageSecurityValidator>();
        builder.AddTaskFilter<SecurityValidationFilter>();

        return builder;
    }

    /// <summary>
    /// Enables inbox-based message deduplication for the worker.
    /// Messages that have already been processed will be skipped to provide
    /// at-most-once processing semantics.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    /// <remarks>
    /// Requires an IInboxStore implementation to be registered (e.g., UseInbox&lt;T&gt;).
    /// </remarks>
    public static DotCeleryBuilder UseInboxDeduplication(this DotCeleryBuilder builder)
    {
        builder.AddTaskFilter<InboxDeduplicationFilter>();
        return builder;
    }
}
