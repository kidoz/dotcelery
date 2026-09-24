namespace DotCelery.Tests.Unit.Storage;

using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Storage;
using DotCelery.Tests.Conformance.Storage;

public sealed class InMemoryDocumentStoreTests : DocumentStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult<IStorageProvider>(new InMemoryStorageProvider(timeProvider));
}

public sealed class InMemoryLeaseStoreTests : LeaseStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult<IStorageProvider>(new InMemoryStorageProvider(timeProvider));
}

public sealed class InMemoryQueueStoreTests : QueueStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult<IStorageProvider>(new InMemoryStorageProvider(timeProvider));
}

public sealed class InMemoryCounterStoreTests : CounterStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult<IStorageProvider>(new InMemoryStorageProvider(timeProvider));
}

public sealed class InMemoryNotificationChannelTests : NotificationChannelConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult<IStorageProvider>(new InMemoryStorageProvider(timeProvider));
}

public sealed class InMemoryStorageProviderTests : StorageProviderConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult<IStorageProvider>(new InMemoryStorageProvider(timeProvider));
}
