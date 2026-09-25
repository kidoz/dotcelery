using DotCelery.Core.Models;
using DotCelery.Core.Storage.Stores;

namespace DotCelery.Tests.Conformance.Stores;

/// <summary>
/// Conformance tests for <see cref="DelayedMessageStore"/>.
/// </summary>
public abstract class DelayedMessageStoreConformanceTests : StoreConformanceTests
{
    private DelayedMessageStore CreateStore() => new(Provider, CreateOptions());

    [Fact]
    public async Task GetDueMessagesAsync_ReturnsOnlyDueMessagesInDeliveryOrder()
    {
        var store = CreateStore();
        await store.AddAsync(CreateTaskMessage("later"), Start.AddMinutes(10));
        await store.AddAsync(CreateTaskMessage("second"), Start.AddSeconds(20));
        await store.AddAsync(CreateTaskMessage("first"), Start.AddSeconds(10));

        Time.Advance(TimeSpan.FromSeconds(30));
        var due = await DrainAsync(store);

        Assert.Equal(["first", "second"], due.Select(m => m.Id));
        Assert.Equal(1, await store.GetPendingCountAsync());
    }

    [Fact]
    public async Task GetDueMessagesAsync_RoundTripsTheMessage()
    {
        var store = CreateStore();
        var message = CreateTaskMessage("task") with { Eta = Start, Priority = 7 };
        await store.AddAsync(message, Start);

        var due = Assert.Single(await DrainAsync(store));

        Assert.Equal(message.Id, due.Id);
        Assert.Equal(message.Args, due.Args);
        Assert.Equal(message.Eta, due.Eta);
        Assert.Equal(message.Priority, due.Priority);
    }

    [Fact]
    public async Task GetDueMessagesAsync_HandlerFailure_RedeliversTheMessageAfterTheClaimTimeout()
    {
        var store = CreateStore();
        await store.AddAsync(CreateTaskMessage("task"), Start);

        // The handler fails before it has published the message
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in store.GetDueMessagesAsync(Start))
            {
                throw new InvalidOperationException("Publish failed");
            }
        });

        Assert.Empty(await DrainAsync(store));
        Time.Advance(StoreOptions.ClaimTimeout);
        Assert.Equal("task", Assert.Single(await DrainAsync(store)).Id);
    }

    [Fact]
    public async Task GetDueMessagesAsync_MessageReAddedByTheHandler_IsKept()
    {
        var store = CreateStore();
        await store.AddAsync(CreateTaskMessage("task"), Start);

        await foreach (var message in store.GetDueMessagesAsync(Start))
        {
            await store.AddAsync(message, Start.AddMinutes(1));
        }

        Assert.Equal(1, await store.GetPendingCountAsync());
        Assert.Equal(Start.AddMinutes(1), await store.GetNextDeliveryTimeAsync());
    }

    [Fact]
    public async Task GetDueMessagesAsync_ConcurrentDispatchers_DeliverEachMessageOnce()
    {
        var store = CreateStore();
        for (var i = 0; i < 20; i++)
        {
            await store.AddAsync(CreateTaskMessage($"task-{i}"), Start);
        }

        var batches = await RunConcurrentlyAsync(4, _ => DrainAsync(CreateStore()));
        var delivered = batches.SelectMany(b => b).Select(m => m.Id).ToList();

        Assert.Equal(20, delivered.Count);
        Assert.Equal(20, delivered.Distinct().Count());
    }

    [Fact]
    public async Task AddAsync_SameId_ReplacesTheMessage()
    {
        var store = CreateStore();
        await store.AddAsync(CreateTaskMessage("task"), Start.AddMinutes(1));
        await store.AddAsync(CreateTaskMessage("task"), Start.AddMinutes(2));

        Assert.Equal(1, await store.GetPendingCountAsync());
        Assert.Equal(Start.AddMinutes(2), await store.GetNextDeliveryTimeAsync());
    }

    [Fact]
    public async Task RemoveAsync_RemovesTheMessage()
    {
        var store = CreateStore();
        await store.AddAsync(CreateTaskMessage("task"), Start);

        Assert.True(await store.RemoveAsync("task"));
        Assert.False(await store.RemoveAsync("task"));
        Assert.Equal(0, await store.GetPendingCountAsync());
        Assert.Null(await store.GetNextDeliveryTimeAsync());
    }

    private static async Task<List<TaskMessage>> DrainAsync(DelayedMessageStore store)
    {
        var messages = new List<TaskMessage>();
        await foreach (var message in store.GetDueMessagesAsync(DateTimeOffset.MinValue))
        {
            messages.Add(message);
        }

        return messages;
    }
}
