using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Storage;
using DotCelery.Core.Storage.Stores;
using Microsoft.Extensions.Time.Testing;

namespace DotCelery.Tests.Conformance.Stores;

/// <summary>
/// Conformance tests for <see cref="RevocationStore"/>.
/// </summary>
public abstract class RevocationStoreConformanceTests : StoreConformanceTests
{
    private RevocationStore CreateStore(
        IStorageProvider? storage = null,
        TimeProvider? time = null
    ) => new(storage ?? Provider, CreateOptions(), time ?? Time);

    [Fact]
    public async Task RevokeAsync_MarksTheTaskRevoked()
    {
        var store = CreateStore();

        await store.RevokeAsync("a");
        await store.RevokeAsync(["b", "c"], RevokeOptions.WithTermination);

        Assert.True(await CreateStore().IsRevokedAsync("a"));
        Assert.True(await store.IsRevokedAsync("c"));
        Assert.False(await store.IsRevokedAsync("d"));
        Assert.Equal(["a", "b", "c"], (await ListAsync(store)).Order());
    }

    [Fact]
    public async Task RevokeAsync_WithExpiry_ExpiresTheRevocation()
    {
        var store = CreateStore();

        await store.RevokeAsync("a", new RevokeOptions { Expiry = TimeSpan.FromMinutes(1) });
        await store.RevokeAsync("b");

        Time.Advance(TimeSpan.FromMinutes(1));
        Assert.False(await store.IsRevokedAsync("a"));
        Assert.True(await store.IsRevokedAsync("b"));
        Time.Advance(StoreOptions.RevocationRetention);
        Assert.False(await store.IsRevokedAsync("b"));
    }

    [Fact]
    public async Task CleanupAsync_RemovesRevocationsOlderThanTheAge()
    {
        var store = CreateStore();
        await store.RevokeAsync("old");
        Time.Advance(TimeSpan.FromMinutes(2));
        await store.RevokeAsync("new");

        Assert.Equal(1, await store.CleanupAsync(TimeSpan.FromMinutes(1)));
        Assert.Equal(["new"], await ListAsync(store));
    }

    [Fact]
    public async Task SubscribeAsync_ReceivesRevocationsWithTheirOptions()
    {
        Assert.SkipWhen(Provider.Notifications is null, "The provider has no notifications");
        var store = CreateStore();
        using var cts = new CancellationTokenSource(Timeout);
        await using var subscription = store
            .SubscribeAsync(cts.Token)
            .GetAsyncEnumerator(cts.Token);
        var next = subscription.MoveNextAsync().AsTask();

        // Providers may start listening asynchronously, so keep revoking until it is received
        while (!next.IsCompleted)
        {
            await CreateStore().RevokeAsync("a", RevokeOptions.WithTermination, cts.Token);
            await Task.WhenAny(next, Task.Delay(100, cts.Token));
        }

        Assert.True(await next);
        Assert.Equal("a", subscription.Current.TaskId);
        Assert.True(subscription.Current.Options.Terminate);
        Assert.Equal(Start, subscription.Current.Timestamp);
    }

    [Fact]
    public async Task SubscribeAsync_WithoutNotifications_PollsForRevocations()
    {
        var storage = new WithoutNotifications(Provider);
        var store = CreateStore(storage);
        using var cts = new CancellationTokenSource(Timeout);
        await using var subscription = store
            .SubscribeAsync(cts.Token)
            .GetAsyncEnumerator(cts.Token);

        await store.RevokeAsync("a", RevokeOptions.WithTermination, cts.Token);
        Assert.Equal("a", await NextAsync(subscription));
        Assert.True(subscription.Current.Options.Terminate);

        // A poll that looks back over "a" again must not repeat it
        await store.RevokeAsync("b", cancellationToken: cts.Token);
        Assert.Equal("b", await NextAsync(subscription));
    }

    [Fact]
    public async Task SubscribeAsync_WithoutNotifications_ReceivesRevocationsCommittedOutOfOrder()
    {
        var storage = new WithoutNotifications(Provider);
        var store = CreateStore(storage);
        var lateClock = new FakeTimeProvider(Start);
        using var cts = new CancellationTokenSource(Timeout);
        await using var subscription = store
            .SubscribeAsync(cts.Token)
            .GetAsyncEnumerator(cts.Token);
        var first = subscription.MoveNextAsync().AsTask();

        Time.Advance(TimeSpan.FromSeconds(2));
        await store.RevokeAsync("b", cancellationToken: cts.Token);
        Assert.True(await AdvanceUntilAsync(first, StoreOptions.RevocationPollInterval));
        Assert.Equal("b", subscription.Current.TaskId);

        // Revoked after "b" was received, but stamped earlier, as by a slower revoker
        lateClock.Advance(TimeSpan.FromSeconds(1));
        await CreateStore(storage, lateClock).RevokeAsync("a", cancellationToken: cts.Token);
        Assert.Equal("a", await NextAsync(subscription));
    }

    private async Task<string> NextAsync(IAsyncEnumerator<RevocationEvent> subscription)
    {
        Assert.True(
            await AdvanceUntilAsync(
                subscription.MoveNextAsync().AsTask(),
                StoreOptions.RevocationPollInterval
            )
        );
        return subscription.Current.TaskId;
    }

    private static async Task<List<string>> ListAsync(RevocationStore store)
    {
        var taskIds = new List<string>();
        await foreach (var taskId in store.GetRevokedTaskIdsAsync())
        {
            taskIds.Add(taskId);
        }

        return taskIds;
    }
}
