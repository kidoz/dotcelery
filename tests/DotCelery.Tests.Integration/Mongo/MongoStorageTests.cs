using DotCelery.Backend.Mongo.Extensions;
using DotCelery.Backend.Mongo.Storage;
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
using Testcontainers.MongoDb;

namespace DotCelery.Tests.Integration.Mongo;

/// <summary>
/// A MongoDB container shared by all storage tests. Tests use unique names, so they can share it.
/// </summary>
public sealed class MongoStorageFixture : IAsyncLifetime
{
    private readonly MongoDbContainer _container = new MongoDbBuilder("mongo:7").Build();

    public MongoClientProvider Clients { get; } = new();

    public MongoStorageOptions Options { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        Options = new MongoStorageOptions
        {
            ConnectionString = _container.GetConnectionString(),
            DatabaseName = "storage_tests",
        };
    }

    public IStorageProvider CreateProvider(TimeProvider timeProvider) =>
        new MongoStorageProvider(Clients, Options, timeProvider);

    public async ValueTask DisposeAsync()
    {
        Clients.Dispose();
        await _container.DisposeAsync();
    }
}

[CollectionDefinition(Name)]
public sealed class MongoStorageTestGroup : ICollectionFixture<MongoStorageFixture>
{
    public const string Name = "MongoStorage";
}

[Collection(MongoStorageTestGroup.Name)]
public sealed class MongoDocumentStoreTests(MongoStorageFixture fixture)
    : DocumentStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(MongoStorageTestGroup.Name)]
public sealed class MongoLeaseStoreTests(MongoStorageFixture fixture) : LeaseStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(MongoStorageTestGroup.Name)]
public sealed class MongoQueueStoreTests(MongoStorageFixture fixture) : QueueStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(MongoStorageTestGroup.Name)]
public sealed class MongoCounterStoreTests(MongoStorageFixture fixture)
    : CounterStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(MongoStorageTestGroup.Name)]
public sealed class MongoNotificationChannelTests(MongoStorageFixture fixture)
    : NotificationChannelConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(MongoStorageTestGroup.Name)]
public sealed class MongoStorageProviderTests(MongoStorageFixture fixture)
    : StorageProviderConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(MongoStorageTestGroup.Name)]
public sealed class MongoDelayedMessageStoreTests(MongoStorageFixture fixture)
    : DelayedMessageStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(MongoStorageTestGroup.Name)]
public sealed class MongoOutboxStoreTests(MongoStorageFixture fixture) : OutboxStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(MongoStorageTestGroup.Name)]
public sealed class MongoSignalStoreTests(MongoStorageFixture fixture) : SignalStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(MongoStorageTestGroup.Name)]
public sealed class MongoRevocationStoreTests(MongoStorageFixture fixture)
    : RevocationStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(MongoStorageTestGroup.Name)]
public sealed class MongoLeaseBackedStoreTests(MongoStorageFixture fixture)
    : LeaseBackedStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(MongoStorageTestGroup.Name)]
public sealed class MongoWindowRateLimiterTests(MongoStorageFixture fixture)
    : WindowRateLimiterConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(MongoStorageTestGroup.Name)]
public sealed class MongoResultBackendTests(MongoStorageFixture fixture)
    : ResultBackendConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(MongoStorageTestGroup.Name)]
public sealed class MongoBatchStoreTests(MongoStorageFixture fixture) : BatchStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(MongoStorageTestGroup.Name)]
public sealed class MongoSagaStoreTests(MongoStorageFixture fixture) : SagaStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(MongoStorageTestGroup.Name)]
public sealed class MongoDeadLetterStoreTests(MongoStorageFixture fixture)
    : DeadLetterStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(MongoStorageTestGroup.Name)]
public sealed class MongoQueueMetricsTests(MongoStorageFixture fixture)
    : QueueMetricsConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(MongoStorageTestGroup.Name)]
public sealed class MongoHistoricalDataStoreTests(MongoStorageFixture fixture)
    : HistoricalDataStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(MongoStorageTestGroup.Name)]
public sealed class MongoStoreRegistrationTests(MongoStorageFixture fixture)
{
    [Fact]
    public async Task UseNotificationsDisabled_ProviderWorksWithoutNotifications()
    {
        var provider = new MongoStorageProvider(
            fixture.Clients,
            new MongoStorageOptions
            {
                ConnectionString = fixture.Options.ConnectionString,
                DatabaseName = $"no_notifications_{Guid.NewGuid():N}",
                UseNotifications = false,
            }
        );

        Assert.Null(provider.Notifications);
        Assert.True(await provider.IsHealthyAsync());
        Assert.Equal(1, await provider.Counters.IncrementAsync("counter"));
    }

    [Fact]
    public async Task AddMongoStores_ShareOneStorageProviderAndWork()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IMessageBroker, RecordingBroker>();
        services.AddSingleton<IMessageSerializer, JsonMessageSerializer>();
        services.Configure<StorageStoreOptions>(o => o.Prefix = $"di-{Guid.NewGuid():N}");
        Action<MongoStorageOptions> configure = o =>
        {
            o.ConnectionString = fixture.Options.ConnectionString;
            o.DatabaseName = fixture.Options.DatabaseName;
        };
        services
            .AddMongoBackend(configure)
            .AddMongoBatchStore(configure)
            .AddMongoSagaStore(configure)
            .AddMongoDeadLetterStore(configure)
            .AddMongoQueueMetrics(configure)
            .AddMongoHistoricalDataStore(configure)
            .AddMongoDelayedMessageStore(configure)
            .AddMongoRevocationStore(configure)
            .AddMongoRateLimiter(configure)
            .AddMongoOutboxStore(configure)
            .AddMongoInboxStore(configure)
            .AddMongoSignalStore(configure)
            .AddMongoPartitionLockStore(configure)
            .AddMongoTaskExecutionTracker(configure);
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
}
