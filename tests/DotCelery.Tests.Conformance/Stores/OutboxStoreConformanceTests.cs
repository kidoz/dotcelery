using DotCelery.Core.Outbox;
using DotCelery.Core.Storage.Stores;

namespace DotCelery.Tests.Conformance.Stores;

/// <summary>
/// Conformance tests for <see cref="OutboxStore"/> and <see cref="InboxStore"/>.
/// </summary>
public abstract class OutboxStoreConformanceTests : StoreConformanceTests
{
    // Each store instance stands for a dispatcher in its own process
    private OutboxStore CreateStore() => new(Provider, CreateOptions(), Time);

    private InboxStore CreateInbox() => new(Provider, CreateOptions(), Time);

    [Fact]
    public async Task GetPendingAsync_ReturnsStoredMessagesUpToTheLimit()
    {
        var store = CreateStore();
        await store.StoreAsync(CreateOutboxMessage("a"));
        Time.Advance(TimeSpan.FromSeconds(1));
        await store.StoreAsync(CreateOutboxMessage("b"));
        Time.Advance(TimeSpan.FromSeconds(1));
        await store.StoreAsync(CreateOutboxMessage("c"));

        var pending = await ClaimAsync(store, limit: 2);

        Assert.Equal(["a", "b"], pending.Select(m => m.Id));
        Assert.All(pending, m => Assert.Equal(OutboxMessageStatus.Pending, m.Status));
        Assert.Equal("a", pending[0].TaskMessage.Id);
        Assert.Equal(3, await store.GetPendingCountAsync());
    }

    [Fact]
    public async Task GetPendingAsync_ClaimedMessages_AreNotReturnedToOtherDispatchers()
    {
        await CreateStore().StoreAsync(CreateOutboxMessage("a"));

        Assert.Single(await ClaimAsync(CreateStore()));
        Assert.Empty(await ClaimAsync(CreateStore()));
    }

    [Fact]
    public async Task GetPendingAsync_DispatcherCrashed_RedeliversAfterTheClaimTimeout()
    {
        await CreateStore().StoreAsync(CreateOutboxMessage("a"));
        Assert.Single(await ClaimAsync(CreateStore()));

        Time.Advance(StoreOptions.ClaimTimeout);
        var redelivered = Assert.Single(await ClaimAsync(CreateStore()));

        Assert.Equal("a", redelivered.Id);
        Assert.Equal(0, redelivered.Attempts);
    }

    [Fact]
    public async Task GetPendingAsync_ConcurrentDispatchers_ClaimEachMessageOnce()
    {
        var store = CreateStore();
        for (var i = 0; i < 20; i++)
        {
            await store.StoreAsync(CreateOutboxMessage($"m-{i}"));
        }

        var batches = await RunConcurrentlyAsync(4, _ => ClaimAsync(CreateStore(), limit: 20));
        var claimed = batches.SelectMany(b => b).Select(m => m.Id).ToList();

        Assert.Equal(20, claimed.Count);
        Assert.Equal(20, claimed.Distinct().Count());
    }

    [Fact]
    public async Task MarkDispatchedAsync_RemovesTheMessage()
    {
        var store = CreateStore();
        await store.StoreAsync(CreateOutboxMessage("a"));
        await ClaimAsync(store);

        await store.MarkDispatchedAsync("a");

        Assert.Equal(0, await store.GetPendingCountAsync());
        Time.Advance(StoreOptions.ClaimTimeout);
        Assert.Empty(await ClaimAsync(store));
    }

    [Fact]
    public async Task MarkFailedAsync_RetriesAfterTheRetryDelay()
    {
        var store = CreateStore();
        await store.StoreAsync(CreateOutboxMessage("a"));
        await ClaimAsync(store);

        await store.MarkFailedAsync("a", "broker down");

        Assert.Equal(1, await store.GetPendingCountAsync());
        Assert.Empty(await ClaimAsync(store));
        Time.Advance(StoreOptions.OutboxRetryDelay);
        var retried = Assert.Single(await ClaimAsync(store));
        Assert.Equal(1, retried.Attempts);
        Assert.Equal("broker down", retried.LastError);
    }

    [Fact]
    public async Task MarkFailedAsync_AfterMaxAttempts_StopsRetrying()
    {
        StoreOptions.OutboxMaxAttempts = 2;
        var store = CreateStore();
        await store.StoreAsync(CreateOutboxMessage("a"));

        for (var attempt = 0; attempt < 2; attempt++)
        {
            Assert.Single(await ClaimAsync(store));
            await store.MarkFailedAsync("a", $"error {attempt}");
            Time.Advance(StoreOptions.OutboxRetryDelay);
        }

        Assert.Equal(0, await store.GetPendingCountAsync());
        Assert.Empty(await ClaimAsync(store));
    }

    [Fact]
    public async Task MarkFailedAsync_AfterTheClaimExpired_LeavesTheMessageToTheNextDispatcher()
    {
        var store = CreateStore();
        await store.StoreAsync(CreateOutboxMessage("a"));
        await ClaimAsync(store);

        Time.Advance(StoreOptions.ClaimTimeout);
        await store.MarkFailedAsync("a", "too late");

        var next = Assert.Single(await ClaimAsync(CreateStore()));
        Assert.Equal(0, next.Attempts);
    }

    [Fact]
    public async Task CleanupAsync_RemovesFailedMessagesOlderThanTheAge()
    {
        StoreOptions.OutboxMaxAttempts = 1;
        var store = CreateStore();
        await store.StoreAsync(CreateOutboxMessage("a"));
        await ClaimAsync(store);
        await store.MarkFailedAsync("a", "error");

        Assert.Equal(0, await store.CleanupAsync(TimeSpan.FromMinutes(1)));
        Time.Advance(TimeSpan.FromMinutes(2));
        Assert.Equal(1, await store.CleanupAsync(TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public async Task Inbox_MarkProcessedAsync_RemembersTheMessageUntilTheRetentionEnds()
    {
        var inbox = CreateInbox();

        Assert.False(await inbox.IsProcessedAsync("a"));
        await inbox.MarkProcessedAsync("a");
        await inbox.MarkProcessedAsync("a");

        Assert.True(await CreateInbox().IsProcessedAsync("a"));
        Assert.Equal(1, await inbox.GetCountAsync());
        Time.Advance(StoreOptions.InboxRetention);
        Assert.False(await inbox.IsProcessedAsync("a"));
    }

    [Fact]
    public async Task Inbox_CleanupAsync_RemovesMessagesProcessedBeforeTheAge()
    {
        var inbox = CreateInbox();
        await inbox.MarkProcessedAsync("old");
        Time.Advance(TimeSpan.FromMinutes(2));
        await inbox.MarkProcessedAsync("new");

        Assert.Equal(1, await inbox.CleanupAsync(TimeSpan.FromMinutes(1)));
        Assert.False(await inbox.IsProcessedAsync("old"));
        Assert.True(await inbox.IsProcessedAsync("new"));
    }

    private OutboxMessage CreateOutboxMessage(string id) =>
        new()
        {
            Id = id,
            TaskMessage = CreateTaskMessage(id),
            CreatedAt = Time.GetUtcNow(),
        };

    private static async Task<List<OutboxMessage>> ClaimAsync(OutboxStore store, int limit = 100)
    {
        var messages = new List<OutboxMessage>();
        await foreach (var message in store.GetPendingAsync(limit))
        {
            messages.Add(message);
        }

        return messages;
    }
}
