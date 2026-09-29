using DotCelery.Backend.Redis.Extensions;
using DotCelery.Backend.Redis.Storage;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Batches;
using DotCelery.Core.Execution;
using DotCelery.Core.Models;
using DotCelery.Core.Partitioning;
using DotCelery.Core.RateLimiting;
using DotCelery.Core.Sagas;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage;
using DotCelery.Core.Storage.Stores;
using DotCelery.Tests.Conformance.Storage;
using DotCelery.Tests.Conformance.Stores;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.Redis;

namespace DotCelery.Tests.Integration.Redis;

/// <summary>
/// A Redis container shared by all storage tests. Tests use unique names, so they can share it.
/// </summary>
public sealed class RedisStorageFixture : IAsyncLifetime
{
    private readonly RedisContainer _container = new RedisBuilder("redis:7-alpine").Build();

    public RedisConnectionProvider Connections { get; } = new();

    public RedisStorageOptions Options { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        Options = new RedisStorageOptions
        {
            ConnectionString = _container.GetConnectionString(),
            KeyPrefix = "storage-tests:",
        };
    }

    public IStorageProvider CreateProvider(TimeProvider timeProvider) =>
        new RedisStorageProvider(Connections, Options, timeProvider);

    public async ValueTask DisposeAsync()
    {
        await Connections.DisposeAsync();
        await _container.DisposeAsync();
    }
}

[CollectionDefinition(Name)]
public sealed class RedisStorageTestGroup : ICollectionFixture<RedisStorageFixture>
{
    public const string Name = "RedisStorage";
}

[Collection(RedisStorageTestGroup.Name)]
public sealed class RedisDocumentStoreTests(RedisStorageFixture fixture)
    : DocumentStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(RedisStorageTestGroup.Name)]
public sealed class RedisLeaseStoreTests(RedisStorageFixture fixture) : LeaseStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(RedisStorageTestGroup.Name)]
public sealed class RedisQueueStoreTests(RedisStorageFixture fixture) : QueueStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(RedisStorageTestGroup.Name)]
public sealed class RedisCounterStoreTests(RedisStorageFixture fixture)
    : CounterStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(RedisStorageTestGroup.Name)]
public sealed class RedisNotificationChannelTests(RedisStorageFixture fixture)
    : NotificationChannelConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(RedisStorageTestGroup.Name)]
public sealed class RedisStorageProviderTests(RedisStorageFixture fixture)
    : StorageProviderConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(RedisStorageTestGroup.Name)]
public sealed class RedisDelayedMessageStoreTests(RedisStorageFixture fixture)
    : DelayedMessageStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(RedisStorageTestGroup.Name)]
public sealed class RedisOutboxStoreTests(RedisStorageFixture fixture) : OutboxStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(RedisStorageTestGroup.Name)]
public sealed class RedisSignalStoreTests(RedisStorageFixture fixture) : SignalStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(RedisStorageTestGroup.Name)]
public sealed class RedisRevocationStoreTests(RedisStorageFixture fixture)
    : RevocationStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(RedisStorageTestGroup.Name)]
public sealed class RedisLeaseBackedStoreTests(RedisStorageFixture fixture)
    : LeaseBackedStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(RedisStorageTestGroup.Name)]
public sealed class RedisWindowRateLimiterTests(RedisStorageFixture fixture)
    : WindowRateLimiterConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(RedisStorageTestGroup.Name)]
public sealed class RedisResultBackendTests(RedisStorageFixture fixture)
    : ResultBackendConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(RedisStorageTestGroup.Name)]
public sealed class RedisBatchStoreTests(RedisStorageFixture fixture) : BatchStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(RedisStorageTestGroup.Name)]
public sealed class RedisSagaStoreTests(RedisStorageFixture fixture) : SagaStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(RedisStorageTestGroup.Name)]
public sealed class RedisDeadLetterStoreTests(RedisStorageFixture fixture)
    : DeadLetterStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(RedisStorageTestGroup.Name)]
public sealed class RedisQueueMetricsTests(RedisStorageFixture fixture)
    : QueueMetricsConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(RedisStorageTestGroup.Name)]
public sealed class RedisHistoricalDataStoreTests(RedisStorageFixture fixture)
    : HistoricalDataStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(RedisStorageTestGroup.Name)]
public sealed class RedisStoreRegistrationTests(RedisStorageFixture fixture)
{
    [Fact]
    public async Task AddRedisStores_ShareOneStorageProviderAndWork()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IHostEnvironment>(new TestHostEnvironment());
        services.AddSingleton<IMessageBroker, RecordingBroker>();
        services.AddSingleton<IMessageSerializer, JsonMessageSerializer>();
        services.Configure<StorageStoreOptions>(o => o.Prefix = $"di-{Guid.NewGuid():N}");
        Action<RedisStorageOptions> configure = o =>
        {
            o.ConnectionString = fixture.Options.ConnectionString;
            o.KeyPrefix = fixture.Options.KeyPrefix;
        };
        services
            .AddRedisBackend(configure)
            .AddRedisBatchStore(configure)
            .AddRedisSagaStore(configure)
            .AddRedisDeadLetterStore(configure)
            .AddRedisQueueMetrics(configure)
            .AddRedisHistoricalDataStore(configure)
            .AddRedisDelayedMessageStore(configure)
            .AddRedisRevocationStore(configure)
            .AddRedisRateLimiter(configure)
            .AddRedisOutboxStore(configure)
            .AddRedisInboxStore(configure)
            .AddRedisSignalStore(configure)
            .AddRedisPartitionLockStore(configure)
            .AddRedisTaskExecutionTracker(configure);
        await using var provider = services.BuildServiceProvider();

        Assert.Single(provider.GetServices<IStorageProvider>());
        Assert.Single(provider.GetServices<IHostedService>().OfType<StoragePurgeService>());
        Assert.True(await provider.GetRequiredService<IStorageProvider>().IsHealthyAsync());

        await provider
            .GetRequiredService<IResultBackend>()
            .UpdateStateAsync("task", TaskState.Started);
        Assert.Equal(
            TaskState.Started,
            await provider.GetRequiredService<IResultBackend>().GetStateAsync("task")
        );
        Assert.Null(await provider.GetRequiredService<IBatchStore>().GetAsync("batch"));
        Assert.Null(await provider.GetRequiredService<ISagaStore>().GetAsync("saga"));
        Assert.Equal(0, await provider.GetRequiredService<IDeadLetterStore>().GetCountAsync());
        await provider.GetRequiredService<IQueueMetrics>().RecordEnqueuedAsync("celery");
        Assert.Equal(
            1,
            await provider.GetRequiredService<IQueueMetrics>().GetWaitingCountAsync("celery")
        );
        Assert.Equal(
            0,
            await provider.GetRequiredService<IHistoricalDataStore>().GetSnapshotCountAsync()
        );
        Assert.Equal(
            0,
            await provider.GetRequiredService<IDelayedMessageStore>().GetPendingCountAsync()
        );
        await provider.GetRequiredService<IRevocationStore>().RevokeAsync("task");
        Assert.True(await provider.GetRequiredService<IRevocationStore>().IsRevokedAsync("task"));
        Assert.True(
            (
                await provider
                    .GetRequiredService<IRateLimiter>()
                    .TryAcquireAsync("api", RateLimitPolicy.PerMinute(1))
            ).IsAcquired
        );
        Assert.Equal(0, await provider.GetRequiredService<IOutboxStore>().GetPendingCountAsync());
        await provider.GetRequiredService<IInboxStore>().MarkProcessedAsync("message");
        Assert.True(await provider.GetRequiredService<IInboxStore>().IsProcessedAsync("message"));
        Assert.Equal(0, await provider.GetRequiredService<ISignalStore>().GetPendingCountAsync());
        Assert.True(
            await provider
                .GetRequiredService<IPartitionLockStore>()
                .TryAcquireAsync("partition", "task", TimeSpan.FromMinutes(1))
        );
        Assert.True(
            await provider
                .GetRequiredService<ITaskExecutionTracker>()
                .TryStartAsync("tests.task", "task")
        );
    }

    private sealed class TestHostEnvironment : IHostEnvironment
    {
        public string EnvironmentName { get; set; } = Environments.Development;

        public string ApplicationName { get; set; } = "tests";

        public string ContentRootPath { get; set; } = AppContext.BaseDirectory;

        public Microsoft.Extensions.FileProviders.IFileProvider ContentRootFileProvider { get; set; } =
            new Microsoft.Extensions.FileProviders.NullFileProvider();
    }
}
