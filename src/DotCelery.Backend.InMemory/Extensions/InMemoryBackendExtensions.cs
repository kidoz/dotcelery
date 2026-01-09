using DotCelery.Backend.InMemory.Batches;
using DotCelery.Backend.InMemory.DelayedMessageStore;
using DotCelery.Backend.InMemory.Execution;
using DotCelery.Backend.InMemory.Metrics;
using DotCelery.Backend.InMemory.Outbox;
using DotCelery.Backend.InMemory.Partitioning;
using DotCelery.Backend.InMemory.Revocation;
using DotCelery.Backend.InMemory.Sagas;
using DotCelery.Backend.InMemory.Signals;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Batches;
using DotCelery.Core.Execution;
using DotCelery.Core.Extensions;
using DotCelery.Core.MultiTenancy;
using DotCelery.Core.Partitioning;
using DotCelery.Core.RateLimiting;
using DotCelery.Core.Sagas;
using Microsoft.Extensions.DependencyInjection;

namespace DotCelery.Backend.InMemory.Extensions;

/// <summary>
/// Extension methods for configuring the in-memory backend.
/// </summary>
public static class InMemoryBackendExtensions
{
    /// <summary>
    /// Uses the in-memory backend for testing and development.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder UseInMemoryBackend(this DotCeleryBuilder builder)
    {
        return builder.UseBackend<InMemoryResultBackend>();
    }

    /// <summary>
    /// Adds the in-memory delayed message store for ETA/countdown support.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddInMemoryDelayedMessageStore(this DotCeleryBuilder builder)
    {
        builder.Services.AddSingleton<IDelayedMessageStore, InMemoryDelayedMessageStore>();
        return builder;
    }

    /// <summary>
    /// Adds the in-memory revocation store for task cancellation support.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddInMemoryRevocationStore(this DotCeleryBuilder builder)
    {
        builder.Services.AddSingleton<IRevocationStore, InMemoryRevocationStore>();
        return builder;
    }

    /// <summary>
    /// Adds the in-memory rate limiter for task rate limiting support.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddInMemoryRateLimiter(this DotCeleryBuilder builder)
    {
        builder.Services.AddSingleton<IRateLimiter, InMemoryRateLimiter>();
        return builder;
    }

    /// <summary>
    /// Adds the in-memory batch store for batch tracking support.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddInMemoryBatchStore(this DotCeleryBuilder builder)
    {
        builder.Services.AddSingleton<IBatchStore, InMemoryBatchStore>();
        return builder;
    }

    /// <summary>
    /// Adds all in-memory high-priority feature implementations.
    /// This includes delayed message store, revocation store, and rate limiter.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddInMemoryHighPriorityFeatures(this DotCeleryBuilder builder)
    {
        return builder
            .AddInMemoryDelayedMessageStore()
            .AddInMemoryRevocationStore()
            .AddInMemoryRateLimiter();
    }

    /// <summary>
    /// Adds the in-memory outbox store for transactional outbox pattern.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddInMemoryOutboxStore(this DotCeleryBuilder builder)
    {
        builder.Services.AddSingleton<IOutboxStore, InMemoryOutboxStore>();
        return builder;
    }

    /// <summary>
    /// Adds the in-memory inbox store for message deduplication.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddInMemoryInboxStore(this DotCeleryBuilder builder)
    {
        builder.Services.AddSingleton<IInboxStore, InMemoryInboxStore>();
        return builder;
    }

    /// <summary>
    /// Adds the in-memory saga store for saga orchestration.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddInMemorySagaStore(this DotCeleryBuilder builder)
    {
        builder.Services.AddSingleton<ISagaStore, InMemorySagaStore>();
        return builder;
    }

    /// <summary>
    /// Adds the in-memory signal queue for queued signal dispatch.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddInMemorySignalStore(this DotCeleryBuilder builder)
    {
        builder.Services.AddSingleton<ISignalStore, InMemorySignalStore>();
        return builder;
    }

    /// <summary>
    /// Adds the in-memory partition lock store for partitioned messaging.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddInMemoryPartitionLockStore(this DotCeleryBuilder builder)
    {
        builder.Services.AddSingleton<IPartitionLockStore, InMemoryPartitionLockStore>();
        return builder;
    }

    /// <summary>
    /// Adds the in-memory task execution tracker for preventing overlapping executions.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddInMemoryTaskExecutionTracker(this DotCeleryBuilder builder)
    {
        builder.Services.AddSingleton<ITaskExecutionTracker, InMemoryTaskExecutionTracker>();
        return builder;
    }

    /// <summary>
    /// Adds the in-memory queue metrics for monitoring.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddInMemoryQueueMetrics(this DotCeleryBuilder builder)
    {
        builder.Services.AddSingleton<IQueueMetrics, InMemoryQueueMetrics>();
        return builder;
    }

    /// <summary>
    /// Adds the tenant router for multi-tenancy support.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Optional configuration action for multi-tenancy options.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddMultiTenancy(
        this DotCeleryBuilder builder,
        Action<MultiTenancyOptions>? configure = null
    )
    {
        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        builder.Services.AddSingleton<ITenantRouter, TenantRouter>();
        builder.Services.AddSingleton<ITenantContext>(TenantContext.Current);
        return builder;
    }

    /// <summary>
    /// Adds all in-memory Phase 8 advanced pattern implementations.
    /// This includes partition locking, task execution tracking, queue metrics, and multi-tenancy.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder AddInMemoryAdvancedPatterns(this DotCeleryBuilder builder)
    {
        return builder
            .AddInMemoryPartitionLockStore()
            .AddInMemoryTaskExecutionTracker()
            .AddInMemoryQueueMetrics()
            .AddMultiTenancy();
    }
}
