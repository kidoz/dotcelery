using System.Reflection;
using DotCelery.Backend.InMemory.Extensions;
using DotCelery.Broker.InMemory.Extensions;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Batches;
using DotCelery.Core.Dashboard;
using DotCelery.Core.Execution;
using DotCelery.Core.Extensions;
using DotCelery.Core.Partitioning;
using DotCelery.Core.Sagas;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DotCelery.Tests.Integration.Registration;

/// <summary>
/// Holds every provider to the same set of store registrations: a store added to one provider
/// must be added to all of them, so an application can switch providers without discovering a
/// missing registration at run time.
/// </summary>
/// <remarks>
/// These cases need no container; they run in the integration suite because it references every
/// provider.
/// </remarks>
public sealed class ProviderRegistrationParityTests
{
    private static readonly string[] Stores =
    [
        "BatchStore",
        "DeadLetterStore",
        "DelayedMessageStore",
        "HistoricalDataStore",
        "InboxStore",
        "OutboxStore",
        "PartitionLockStore",
        "QueueMetrics",
        "RateLimiter",
        "RevocationStore",
        "SagaStore",
        "SignalStore",
        "TaskExecutionTracker",
        "WorkerRegistry",
    ];

    private static readonly Type[] StoreInterfaces =
    [
        typeof(IBatchStore),
        typeof(IDeadLetterStore),
        typeof(IDelayedMessageStore),
        typeof(IHistoricalDataStore),
        typeof(IInboxStore),
        typeof(IOutboxStore),
        typeof(IPartitionLockStore),
        typeof(IQueueMetrics),
        typeof(IRateLimiter),
        typeof(IRevocationStore),
        typeof(ISagaStore),
        typeof(ISignalStore),
        typeof(ITaskExecutionTracker),
        typeof(IWorkerRegistry),
    ];

    [Theory]
    [InlineData("DotCelery.Backend.InMemory", "AddInMemory")]
    [InlineData("DotCelery.Backend.Mongo", "AddMongo")]
    [InlineData("DotCelery.Backend.Postgres", "AddPostgres")]
    [InlineData("DotCelery.Backend.Redis", "AddRedis")]
    [InlineData("DotCelery.Backend.SqlServer", "AddSqlServer")]
    public void EveryProviderExposesTheSameStoreRegistrations(string assemblyName, string prefix)
    {
        var names = Assembly
            .Load(assemblyName)
            .GetExportedTypes()
            .SelectMany(type => type.GetMethods(BindingFlags.Public | BindingFlags.Static))
            .Select(method => method.Name)
            .ToHashSet(StringComparer.Ordinal);

        var missing = Stores
            .Where(store => !names.Contains(prefix + store, StringComparer.Ordinal))
            .ToArray();

        Assert.True(
            missing.Length == 0,
            $"{assemblyName} does not register: {string.Join(", ", missing)}"
        );
    }

    [Fact]
    public async Task InMemoryRegistrationsResolveEveryStore()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDotCelery(builder =>
            builder
                .UseInMemoryBroker()
                .AddInMemoryBatchStore()
                .AddInMemoryDeadLetterStore()
                .AddInMemoryDelayedMessageStore()
                .AddInMemoryHistoricalDataStore()
                .AddInMemoryInboxStore()
                .AddInMemoryOutboxStore()
                .AddInMemoryPartitionLockStore()
                .AddInMemoryQueueMetrics()
                .AddInMemoryRateLimiter()
                .AddInMemoryRevocationStore()
                .AddInMemorySagaStore()
                .AddInMemorySignalStore()
                .AddInMemoryTaskExecutionTracker()
                .AddInMemoryWorkerRegistry()
        );

        await using var provider = services.BuildServiceProvider();

        foreach (var store in StoreInterfaces)
        {
            Assert.NotNull(provider.GetService(store));
        }
    }
}
