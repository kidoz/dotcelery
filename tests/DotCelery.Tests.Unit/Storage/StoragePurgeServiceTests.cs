namespace DotCelery.Tests.Unit.Storage;

using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Storage;
using DotCelery.Core.Storage.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;

public sealed class StoragePurgeServiceTests
{
    private static readonly TimeSpan Interval = TimeSpan.FromMinutes(5);

    private readonly FakeTimeProvider _time = new(
        new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)
    );

    [Fact]
    public async Task ExecuteAsync_DeletesExpiredEntriesEveryInterval()
    {
        var storage = new CountingStorage(new InMemoryStorageProvider(_time));
        await storage.Documents.UpsertAsync(
            "items",
            "a",
            new byte[] { 1 },
            new DocumentWriteOptions { TimeToLive = TimeSpan.FromMinutes(1) }
        );
        using var service = CreateService(storage);

        await service.StartAsync(CancellationToken.None);
        await AdvanceUntilAsync(() => storage.Purges >= 1);
        await AdvanceUntilAsync(() => storage.Purges >= 2);
        await service.StopAsync(CancellationToken.None);

        Assert.Equal(1, storage.Purged);
    }

    [Fact]
    public async Task ExecuteAsync_PurgeFails_KeepsPurging()
    {
        var storage = new CountingStorage(new InMemoryStorageProvider(_time)) { FailFirst = true };
        using var service = CreateService(storage);

        await service.StartAsync(CancellationToken.None);
        await AdvanceUntilAsync(() => storage.Purges >= 2);
        await service.StopAsync(CancellationToken.None);

        Assert.True(service.ExecuteTask!.IsCompletedSuccessfully);
    }

    private StoragePurgeService CreateService(IStorageProvider storage) =>
        new(
            storage,
            Options.Create(new StorageStoreOptions { PurgeInterval = Interval }),
            NullLogger<StoragePurgeService>.Instance,
            _time
        );

    // The service may start its timer after the clock is advanced, so keep advancing
    private async Task AdvanceUntilAsync(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!condition())
        {
            _time.Advance(Interval);
            await Task.Delay(20, timeout.Token);
        }
    }

    private sealed class CountingStorage(IStorageProvider inner) : IStorageProvider
    {
        private int _purges;
        private long _purged;

        public bool FailFirst { get; init; }

        public int Purges => Volatile.Read(ref _purges);

        public long Purged => Interlocked.Read(ref _purged);

        public string Name => inner.Name;

        public IDocumentStore Documents => inner.Documents;

        public ILeaseStore Leases => inner.Leases;

        public IQueueStore Queues => inner.Queues;

        public ICounterStore Counters => inner.Counters;

        public INotificationChannel? Notifications => inner.Notifications;

        public async ValueTask<long> PurgeExpiredAsync(
            CancellationToken cancellationToken = default
        )
        {
            if (Interlocked.Increment(ref _purges) == 1 && FailFirst)
            {
                throw new InvalidOperationException("Storage unavailable");
            }

            var purged = await inner.PurgeExpiredAsync(cancellationToken);
            Interlocked.Add(ref _purged, purged);
            return purged;
        }

        public ValueTask<bool> IsHealthyAsync(CancellationToken cancellationToken = default) =>
            inner.IsHealthyAsync(cancellationToken);
    }
}
