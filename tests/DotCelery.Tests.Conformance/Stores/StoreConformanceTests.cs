using DotCelery.Core.Models;
using DotCelery.Core.Storage;
using DotCelery.Core.Storage.Stores;
using DotCelery.Tests.Conformance.Storage;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Conformance.Stores;

/// <summary>
/// Base class for conformance tests of the stores built on the storage primitives, so each
/// store is held to the same behavior on every provider.
/// </summary>
/// <remarks>
/// Each test uses its own store name prefix, so a provider may share one database across tests.
/// </remarks>
public abstract class StoreConformanceTests : StorageConformanceTests
{
    protected static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    protected StorageStoreOptions StoreOptions { get; } = new() { Prefix = Unique("app") };

    protected IOptions<StorageStoreOptions> CreateOptions() => Options.Create(StoreOptions);

    protected static TaskMessage CreateTaskMessage(string id) =>
        new()
        {
            Id = id,
            Task = "tests.task",
            Args = [1, 2, 3],
            ContentType = "application/json",
            Timestamp = Start,
        };

    /// <summary>
    /// Advances the clock in steps until <paramref name="task"/> completes. Stores may start
    /// their timers after the test advances the clock, so a single advance can be missed.
    /// </summary>
    protected async Task<T> AdvanceUntilAsync<T>(Task<T> task, TimeSpan step)
    {
        using var timeout = new CancellationTokenSource(Timeout);

        while (!task.IsCompleted)
        {
            timeout.Token.ThrowIfCancellationRequested();
            Time.Advance(step);
            await Task.WhenAny(task, Task.Delay(20, CancellationToken.None));
        }

        return await task;
    }

    /// <summary>
    /// The provider under test without its notifications, for stores that fall back to polling.
    /// </summary>
    protected sealed class WithoutNotifications(IStorageProvider inner) : IStorageProvider
    {
        public string Name => inner.Name;

        public IDocumentStore Documents => inner.Documents;

        public ILeaseStore Leases => inner.Leases;

        public IQueueStore Queues => inner.Queues;

        public ICounterStore Counters => inner.Counters;

        public INotificationChannel? Notifications => null;

        public ValueTask<long> PurgeExpiredAsync(CancellationToken cancellationToken = default) =>
            inner.PurgeExpiredAsync(cancellationToken);

        public ValueTask<bool> IsHealthyAsync(CancellationToken cancellationToken = default) =>
            inner.IsHealthyAsync(cancellationToken);
    }
}
