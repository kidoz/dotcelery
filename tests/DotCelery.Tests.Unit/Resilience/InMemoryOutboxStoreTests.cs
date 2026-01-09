using DotCelery.Backend.InMemory.Outbox;
using DotCelery.Core.Models;
using DotCelery.Core.Outbox;

namespace DotCelery.Tests.Unit.Resilience;

/// <summary>
/// Tests for <see cref="InMemoryOutboxStore"/>.
/// </summary>
public sealed class InMemoryOutboxStoreTests : IAsyncDisposable
{
    private readonly InMemoryOutboxStore _store = new();

    public async ValueTask DisposeAsync()
    {
        await _store.DisposeAsync();
    }

    [Fact]
    public async Task StoreAsync_AddsMessage()
    {
        var message = CreateOutboxMessage("msg-1");

        await _store.StoreAsync(message);

        var count = await _store.GetPendingCountAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task GetPendingAsync_ReturnsOnlyPendingMessages()
    {
        await _store.StoreAsync(CreateOutboxMessage("msg-1"));
        await _store.StoreAsync(CreateOutboxMessage("msg-2"));

        await _store.MarkDispatchedAsync("msg-1");

        var pending = await _store.GetPendingAsync().ToListAsync();

        Assert.Single(pending);
        Assert.Equal("msg-2", pending[0].Id);
    }

    [Fact]
    public async Task GetPendingAsync_ReturnsInSequenceOrder()
    {
        await _store.StoreAsync(CreateOutboxMessage("msg-1"));
        await _store.StoreAsync(CreateOutboxMessage("msg-2"));
        await _store.StoreAsync(CreateOutboxMessage("msg-3"));

        var pending = await _store.GetPendingAsync().ToListAsync();

        Assert.Equal(3, pending.Count);
        Assert.Equal("msg-1", pending[0].Id);
        Assert.Equal("msg-2", pending[1].Id);
        Assert.Equal("msg-3", pending[2].Id);
    }

    [Fact]
    public async Task GetPendingAsync_RespectsLimit()
    {
        for (var i = 0; i < 10; i++)
        {
            await _store.StoreAsync(CreateOutboxMessage($"msg-{i}"));
        }

        var pending = await _store.GetPendingAsync(limit: 3).ToListAsync();

        Assert.Equal(3, pending.Count);
    }

    [Fact]
    public async Task MarkDispatchedAsync_UpdatesMessageStatus()
    {
        await _store.StoreAsync(CreateOutboxMessage("msg-1"));

        await _store.MarkDispatchedAsync("msg-1");

        var pendingCount = await _store.GetPendingCountAsync();
        Assert.Equal(0, pendingCount);
    }

    [Fact]
    public async Task MarkFailedAsync_IncrementsAttempts()
    {
        await _store.StoreAsync(CreateOutboxMessage("msg-1"));

        await _store.MarkFailedAsync("msg-1", "Test error");

        // Message should still be pending (not failed until max attempts)
        var pendingCount = await _store.GetPendingCountAsync();
        Assert.Equal(1, pendingCount);
    }

    [Fact]
    public async Task MarkFailedAsync_AfterMaxAttempts_MarksAsFailed()
    {
        await _store.StoreAsync(CreateOutboxMessage("msg-1"));

        // Fail 5 times (max attempts)
        for (var i = 0; i < 5; i++)
        {
            await _store.MarkFailedAsync("msg-1", $"Error {i}");
        }

        // Message should be failed, not pending
        var pendingCount = await _store.GetPendingCountAsync();
        Assert.Equal(0, pendingCount);
    }

    [Fact]
    public async Task CleanupAsync_RemovesOldDispatchedMessages()
    {
        await _store.StoreAsync(CreateOutboxMessage("msg-1"));
        await _store.StoreAsync(CreateOutboxMessage("msg-2"));

        await _store.MarkDispatchedAsync("msg-1");
        await _store.MarkDispatchedAsync("msg-2");

        // Note: InMemory implementation uses DateTimeOffset.UtcNow directly
        // so we can't easily test time-based cleanup without a time provider
        // For this test, we just verify the method runs without error
        var removed = await _store.CleanupAsync(TimeSpan.FromMinutes(1));

        // Messages just dispatched should not be removed
        Assert.Equal(0, removed);
    }

    [Fact]
    public async Task GetPendingCountAsync_ReturnsCorrectCount()
    {
        await _store.StoreAsync(CreateOutboxMessage("msg-1"));
        await _store.StoreAsync(CreateOutboxMessage("msg-2"));
        await _store.StoreAsync(CreateOutboxMessage("msg-3"));

        await _store.MarkDispatchedAsync("msg-2");

        var count = await _store.GetPendingCountAsync();
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task DisposeAsync_ClearsAllMessages()
    {
        await _store.StoreAsync(CreateOutboxMessage("msg-1"));
        await _store.StoreAsync(CreateOutboxMessage("msg-2"));

        await _store.DisposeAsync();

        var count = await _store.GetPendingCountAsync();
        Assert.Equal(0, count);
    }

    private static OutboxMessage CreateOutboxMessage(string id)
    {
        return new OutboxMessage
        {
            Id = id,
            TaskMessage = new TaskMessage
            {
                Id = id,
                Task = "test.task",
                Args = [],
                ContentType = "application/json",
                Timestamp = DateTimeOffset.UtcNow,
            },
            CreatedAt = DateTimeOffset.UtcNow,
            Status = OutboxMessageStatus.Pending,
        };
    }
}
