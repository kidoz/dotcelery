using DotCelery.Core.Storage;

namespace DotCelery.Tests.Conformance.Storage;

/// <summary>
/// Conformance tests for <see cref="IStorageProvider"/> maintenance operations.
/// </summary>
public abstract class StorageProviderConformanceTests : StorageConformanceTests
{
    [Fact]
    public async Task PurgeExpiredAsync_RemovesOnlyExpiredRecords()
    {
        var collection = Unique("docs");
        await Provider.Documents.TryInsertAsync(
            collection,
            "temporary",
            Bytes("t"),
            new DocumentWriteOptions { TimeToLive = TimeSpan.FromMinutes(1) }
        );
        await Provider.Documents.TryInsertAsync(collection, "permanent", Bytes("p"));
        await Provider.Leases.TryAcquireAsync(Unique("lease"), "owner", TimeSpan.FromSeconds(30));
        await Provider.Counters.IncrementAsync(Unique("counter"), 1, TimeSpan.FromMinutes(1));

        Time.Advance(TimeSpan.FromMinutes(1));
        var purged = await Provider.PurgeExpiredAsync();

        Assert.True(purged >= 3, $"Expected at least 3 purged records, got {purged}.");
        Assert.NotNull(await Provider.Documents.GetAsync(collection, "permanent"));
    }

    [Fact]
    public async Task IsHealthyAsync_ReachableBackend_ReturnsTrue()
    {
        Assert.True(await Provider.IsHealthyAsync());
    }
}
