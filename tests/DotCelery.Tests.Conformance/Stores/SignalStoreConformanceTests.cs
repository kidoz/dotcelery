using DotCelery.Core.Signals;
using DotCelery.Core.Storage.Stores;

namespace DotCelery.Tests.Conformance.Stores;

/// <summary>
/// Conformance tests for <see cref="SignalStore"/>.
/// </summary>
public abstract class SignalStoreConformanceTests : StoreConformanceTests
{
    private SignalStore CreateStore() => new(Provider, CreateOptions(), Time);

    [Fact]
    public async Task DequeueAsync_ReturnsSignalsInEnqueueOrderUpToTheBatchSize()
    {
        var store = CreateStore();
        foreach (var id in new[] { "a", "b", "c" })
        {
            await store.EnqueueAsync(CreateSignal(id));
            Time.Advance(TimeSpan.FromSeconds(1));
        }

        var signals = await DequeueAsync(store, batchSize: 2);

        Assert.Equal(["a", "b"], signals.Select(s => s.Id));
        Assert.Equal("payload-a", signals[0].Payload);
        Assert.Equal(3, await store.GetPendingCountAsync());
    }

    [Fact]
    public async Task AcknowledgeAsync_RemovesTheSignal()
    {
        var store = CreateStore();
        await store.EnqueueAsync(CreateSignal("a"));
        await DequeueAsync(store);

        await store.AcknowledgeAsync("a");

        Assert.Equal(0, await store.GetPendingCountAsync());
    }

    [Fact]
    public async Task RejectAsync_WithRequeue_DeliversTheSignalAgainAtOnce()
    {
        var store = CreateStore();
        await store.EnqueueAsync(CreateSignal("a"));
        await DequeueAsync(store);

        await store.RejectAsync("a", requeue: true);

        Assert.Equal("a", Assert.Single(await DequeueAsync(store)).Id);
    }

    [Fact]
    public async Task RejectAsync_WithoutRequeue_RemovesTheSignal()
    {
        var store = CreateStore();
        await store.EnqueueAsync(CreateSignal("a"));
        await DequeueAsync(store);

        await store.RejectAsync("a", requeue: false);

        Assert.Equal(0, await store.GetPendingCountAsync());
    }

    [Fact]
    public async Task DequeueAsync_UnsettledSignal_IsDeliveredAgainAfterTheClaimTimeout()
    {
        await CreateStore().EnqueueAsync(CreateSignal("a"));
        Assert.Single(await DequeueAsync(CreateStore()));

        Assert.Empty(await DequeueAsync(CreateStore()));
        Time.Advance(StoreOptions.ClaimTimeout);
        Assert.Equal("a", Assert.Single(await DequeueAsync(CreateStore())).Id);
    }

    [Fact]
    public async Task AcknowledgeAsync_AfterTheClaimExpired_RemovesTheSignal()
    {
        var store = CreateStore();
        await store.EnqueueAsync(CreateSignal("a"));
        await DequeueAsync(store);

        // Another processor takes the signal over when the claim expires
        Time.Advance(StoreOptions.ClaimTimeout);
        await DequeueAsync(CreateStore());
        await store.AcknowledgeAsync("a");

        Assert.Equal(0, await store.GetPendingCountAsync());
    }

    [Fact]
    public async Task RejectedSignal_IsArchivedBeforeRemovalAndExpiresAfterRetention()
    {
        var store = CreateStore();
        await store.EnqueueAsync(CreateSignal("poison"));
        await DequeueAsync(store);
        await store.RejectAsync("poison", requeue: false);
        Assert.Equal(0, await store.GetPendingCountAsync());
        var failed = await Provider.Documents.GetAsync(
            $"{StoreOptions.Prefix}.signals.failed",
            "poison"
        );
        Assert.NotNull(failed);
        Time.Advance(StoreOptions.SignalFailedRetention);
        Assert.Null(
            await Provider.Documents.GetAsync($"{StoreOptions.Prefix}.signals.failed", "poison")
        );
    }

    [Fact]
    public async Task RepeatedFailures_AreBoundedAcrossProcessorRestarts()
    {
        StoreOptions.SignalMaxAttempts = 3;
        await CreateStore().EnqueueAsync(CreateSignal("poison"));
        for (var attempt = 0; attempt < StoreOptions.SignalMaxAttempts; attempt++)
        {
            var store = CreateStore();
            Assert.Single(await DequeueAsync(store));
            await store.RejectAsync("poison", requeue: true);
        }
        Assert.Empty(await DequeueAsync(CreateStore()));
        Assert.NotNull(
            await Provider.Documents.GetAsync($"{StoreOptions.Prefix}.signals.failed", "poison")
        );
    }

    [Fact]
    public async Task InvalidSignalEnvelope_IsArchivedWithoutBlockingTheNextSignal()
    {
        await Provider.Queues.EnqueueAsync(
            $"{StoreOptions.Prefix}.signals",
            "a",
            "invalid json"u8.ToArray(),
            Time.GetUtcNow()
        );
        var store = CreateStore();
        await store.EnqueueAsync(CreateSignal("b"));
        Assert.Equal("b", Assert.Single(await DequeueAsync(store)).Id);
        Assert.NotNull(
            await Provider.Documents.GetAsync($"{StoreOptions.Prefix}.signals.failed", "a")
        );
    }

    private SignalMessage CreateSignal(string id) =>
        new()
        {
            Id = id,
            SignalType = "TaskSuccess",
            TaskId = $"task-{id}",
            TaskName = "tests.task",
            Payload = $"payload-{id}",
            CreatedAt = Time.GetUtcNow(),
        };

    private static async Task<List<SignalMessage>> DequeueAsync(
        SignalStore store,
        int batchSize = 100
    )
    {
        var signals = new List<SignalMessage>();
        await foreach (var signal in store.DequeueAsync(batchSize))
        {
            signals.Add(signal);
        }

        return signals;
    }
}
