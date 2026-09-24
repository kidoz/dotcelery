using DotCelery.Core.Storage;

namespace DotCelery.Tests.Conformance.Storage;

/// <summary>
/// Conformance tests for <see cref="IQueueStore"/>.
/// </summary>
public abstract class QueueStoreConformanceTests : StorageConformanceTests
{
    private static readonly TimeSpan Visibility = TimeSpan.FromSeconds(30);

    private readonly string _queue = Unique("queue");

    private IQueueStore Queues => Provider.Queues;

    [Fact]
    public async Task ClaimAsync_ItemNotDue_IsClaimedOnceDue()
    {
        await Queues.EnqueueAsync(_queue, "a", Bytes("payload"), Start.AddMinutes(1));

        Assert.Empty(await Queues.ClaimAsync(_queue, 10, Visibility));

        Time.Advance(TimeSpan.FromMinutes(1));
        var item = Assert.Single(await Queues.ClaimAsync(_queue, 10, Visibility));

        Assert.Equal(_queue, item.Queue);
        Assert.Equal("a", item.Id);
        Assert.Equal("payload", Text(item.Payload));
        Assert.Equal(Start.AddMinutes(1), item.DueAt);
        Assert.Equal(1, item.DeliveryCount);
        Assert.Equal(Start.AddMinutes(1) + Visibility, item.ClaimedUntil);
    }

    [Fact]
    public async Task ClaimAsync_OrdersByDueTimeThenIdAndHonorsMaxItems()
    {
        await Queues.EnqueueAsync(_queue, "b", Bytes("b"), Start);
        await Queues.EnqueueAsync(_queue, "a", Bytes("a"), Start);
        await Queues.EnqueueAsync(_queue, "c", Bytes("c"), Start.AddMinutes(-1));
        await Queues.EnqueueAsync(_queue, "future", Bytes("f"), Start.AddMinutes(1));

        Assert.Equal(["c", "a"], Ids(await Queues.ClaimAsync(_queue, 2, Visibility)));
        Assert.Equal(["b"], Ids(await Queues.ClaimAsync(_queue, 10, Visibility)));
    }

    [Fact]
    public async Task ClaimAsync_ClaimedItem_IsClaimableAgainOnlyAfterTheVisibilityTimeout()
    {
        await Queues.EnqueueAsync(_queue, "a", Bytes("a"), Start);
        var first = Assert.Single(await Queues.ClaimAsync(_queue, 10, Visibility));

        Assert.Empty(await Queues.ClaimAsync(_queue, 10, Visibility));

        Time.Advance(Visibility);
        var second = Assert.Single(await Queues.ClaimAsync(_queue, 10, Visibility));

        Assert.Equal(2, second.DeliveryCount);
        Assert.NotEqual(first.ClaimToken, second.ClaimToken);
    }

    [Fact]
    public async Task CompleteAsync_ClaimedItem_RemovesIt()
    {
        await Queues.EnqueueAsync(_queue, "a", Bytes("a"), Start);
        var item = Assert.Single(await Queues.ClaimAsync(_queue, 10, Visibility));

        Assert.True(await Queues.CompleteAsync(item));

        Assert.Equal(0, await Queues.CountAsync(_queue));
        Time.Advance(Visibility);
        Assert.Empty(await Queues.ClaimAsync(_queue, 10, Visibility));
    }

    [Fact]
    public async Task CompleteAsync_AfterTheItemWasClaimedAgain_ReturnsFalse()
    {
        await Queues.EnqueueAsync(_queue, "a", Bytes("a"), Start);
        var first = Assert.Single(await Queues.ClaimAsync(_queue, 10, Visibility));
        Time.Advance(Visibility);
        var second = Assert.Single(await Queues.ClaimAsync(_queue, 10, Visibility));

        Assert.False(await Queues.CompleteAsync(first));
        Assert.Equal(1, await Queues.CountAsync(_queue));
        Assert.True(await Queues.CompleteAsync(second));
    }

    [Fact]
    public async Task AbandonAsync_ClaimedItem_IsClaimableAgainAtOnce()
    {
        await Queues.EnqueueAsync(_queue, "a", Bytes("a"), Start);
        var item = Assert.Single(await Queues.ClaimAsync(_queue, 10, Visibility));

        Assert.True(await Queues.AbandonAsync(item));

        var again = Assert.Single(await Queues.ClaimAsync(_queue, 10, Visibility));
        Assert.Equal(2, again.DeliveryCount);
    }

    [Fact]
    public async Task AbandonAsync_WithDueTime_DelaysTheItem()
    {
        await Queues.EnqueueAsync(_queue, "a", Bytes("a"), Start);
        var item = Assert.Single(await Queues.ClaimAsync(_queue, 10, Visibility));

        await Queues.AbandonAsync(item, Start.AddMinutes(1));

        Assert.Empty(await Queues.ClaimAsync(_queue, 10, Visibility));
        Assert.Equal(Start.AddMinutes(1), await Queues.GetNextDueAsync(_queue));
        Time.Advance(TimeSpan.FromMinutes(1));
        Assert.Single(await Queues.ClaimAsync(_queue, 10, Visibility));
    }

    [Fact]
    public async Task AbandonAsync_AfterTheItemWasClaimedAgain_ReturnsFalse()
    {
        await Queues.EnqueueAsync(_queue, "a", Bytes("a"), Start);
        var first = Assert.Single(await Queues.ClaimAsync(_queue, 10, Visibility));
        Time.Advance(Visibility);
        await Queues.ClaimAsync(_queue, 10, Visibility);

        Assert.False(await Queues.AbandonAsync(first));
        Assert.Empty(await Queues.ClaimAsync(_queue, 10, Visibility));
    }

    [Fact]
    public async Task EnqueueAsync_SameId_ReplacesTheItemAndReleasesItsClaim()
    {
        await Queues.EnqueueAsync(_queue, "a", Bytes("one"), Start);
        var old = Assert.Single(await Queues.ClaimAsync(_queue, 10, Visibility));

        await Queues.EnqueueAsync(_queue, "a", Bytes("two"), Start);

        var item = Assert.Single(await Queues.ClaimAsync(_queue, 10, Visibility));
        Assert.Equal("two", Text(item.Payload));
        Assert.Equal(1, item.DeliveryCount);
        Assert.False(await Queues.CompleteAsync(old));
        Assert.Equal(1, await Queues.CountAsync(_queue));
    }

    [Fact]
    public async Task RemoveAsync_RemovesClaimedAndWaitingItems()
    {
        await Queues.EnqueueAsync(_queue, "a", Bytes("a"), Start);
        await Queues.EnqueueAsync(_queue, "b", Bytes("b"), Start);
        await Queues.ClaimAsync(_queue, 1, Visibility);

        Assert.True(await Queues.RemoveAsync(_queue, "a"));
        Assert.True(await Queues.RemoveAsync(_queue, "b"));
        Assert.False(await Queues.RemoveAsync(_queue, "b"));
        Assert.Equal(0, await Queues.CountAsync(_queue));
    }

    [Fact]
    public async Task CountAsync_IncludesClaimedItems()
    {
        await Queues.EnqueueAsync(_queue, "a", Bytes("a"), Start);
        await Queues.EnqueueAsync(_queue, "b", Bytes("b"), Start);
        await Queues.EnqueueAsync(_queue, "c", Bytes("c"), Start.AddMinutes(1));
        await Queues.ClaimAsync(_queue, 1, Visibility);

        Assert.Equal(3, await Queues.CountAsync(_queue));
    }

    [Fact]
    public async Task GetNextDueAsync_IgnoresClaimedItems()
    {
        Assert.Null(await Queues.GetNextDueAsync(_queue));

        await Queues.EnqueueAsync(_queue, "a", Bytes("a"), Start);
        await Queues.EnqueueAsync(_queue, "b", Bytes("b"), Start.AddMinutes(1));
        Assert.Equal(Start, await Queues.GetNextDueAsync(_queue));

        await Queues.ClaimAsync(_queue, 10, Visibility);
        Assert.Equal(Start.AddMinutes(1), await Queues.GetNextDueAsync(_queue));
    }

    [Fact]
    public async Task Queues_AreIsolated()
    {
        var other = Unique("queue");
        await Queues.EnqueueAsync(_queue, "a", Bytes("a"), Start);

        Assert.Empty(await Queues.ClaimAsync(other, 10, Visibility));
        Assert.Equal(0, await Queues.CountAsync(other));
        Assert.Null(await Queues.GetNextDueAsync(other));
    }

    [Fact]
    public async Task ClaimAsync_ConcurrentConsumers_ClaimEachItemOnce()
    {
        for (var i = 0; i < 50; i++)
        {
            await Queues.EnqueueAsync(_queue, $"item-{i:D2}", Bytes("x"), Start);
        }

        var claims = await RunConcurrentlyAsync(
            10,
            async _ =>
            {
                var ids = new List<string>();
                while (await Queues.ClaimAsync(_queue, 5, Visibility) is { Count: > 0 } items)
                {
                    ids.AddRange(Ids(items));
                }

                return ids;
            }
        );

        var all = claims.SelectMany(ids => ids).ToList();
        Assert.Equal(50, all.Count);
        Assert.Equal(50, all.Distinct().Count());
    }

    private static List<string> Ids(IReadOnlyList<QueueItem> items) => [.. items.Select(i => i.Id)];
}
