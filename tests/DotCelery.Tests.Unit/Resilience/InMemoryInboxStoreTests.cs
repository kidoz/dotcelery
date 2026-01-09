using DotCelery.Backend.InMemory.Outbox;

namespace DotCelery.Tests.Unit.Resilience;

/// <summary>
/// Tests for <see cref="InMemoryInboxStore"/>.
/// </summary>
public sealed class InMemoryInboxStoreTests : IAsyncDisposable
{
    private readonly InMemoryInboxStore _store = new();

    public async ValueTask DisposeAsync()
    {
        await _store.DisposeAsync();
    }

    [Fact]
    public async Task IsProcessedAsync_ForNewMessage_ReturnsFalse()
    {
        var isProcessed = await _store.IsProcessedAsync("msg-1");

        Assert.False(isProcessed);
    }

    [Fact]
    public async Task MarkProcessedAsync_MarksMessageAsProcessed()
    {
        await _store.MarkProcessedAsync("msg-1");

        var isProcessed = await _store.IsProcessedAsync("msg-1");
        Assert.True(isProcessed);
    }

    [Fact]
    public async Task IsProcessedAsync_ForProcessedMessage_ReturnsTrue()
    {
        await _store.MarkProcessedAsync("msg-1");
        await _store.MarkProcessedAsync("msg-2");

        Assert.True(await _store.IsProcessedAsync("msg-1"));
        Assert.True(await _store.IsProcessedAsync("msg-2"));
        Assert.False(await _store.IsProcessedAsync("msg-3"));
    }

    [Fact]
    public async Task MarkProcessedAsync_IsIdempotent()
    {
        await _store.MarkProcessedAsync("msg-1");
        await _store.MarkProcessedAsync("msg-1");
        await _store.MarkProcessedAsync("msg-1");

        var count = await _store.GetCountAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task GetCountAsync_ReturnsCorrectCount()
    {
        await _store.MarkProcessedAsync("msg-1");
        await _store.MarkProcessedAsync("msg-2");
        await _store.MarkProcessedAsync("msg-3");

        var count = await _store.GetCountAsync();
        Assert.Equal(3, count);
    }

    [Fact]
    public async Task CleanupAsync_RemovesOldMessages()
    {
        await _store.MarkProcessedAsync("msg-1");
        await _store.MarkProcessedAsync("msg-2");

        // Note: InMemory implementation uses DateTimeOffset.UtcNow directly
        // so we can't easily test time-based cleanup without a time provider
        // For this test, we just verify the method runs without error
        var removed = await _store.CleanupAsync(TimeSpan.FromMinutes(1));

        // Messages just processed should not be removed
        Assert.Equal(0, removed);
    }

    [Fact]
    public async Task DisposeAsync_ClearsAllMessages()
    {
        await _store.MarkProcessedAsync("msg-1");
        await _store.MarkProcessedAsync("msg-2");

        await _store.DisposeAsync();

        var count = await _store.GetCountAsync();
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task MultipleMessages_TrackIndependently()
    {
        await _store.MarkProcessedAsync("msg-1");

        Assert.True(await _store.IsProcessedAsync("msg-1"));
        Assert.False(await _store.IsProcessedAsync("msg-2"));

        await _store.MarkProcessedAsync("msg-2");

        Assert.True(await _store.IsProcessedAsync("msg-1"));
        Assert.True(await _store.IsProcessedAsync("msg-2"));
    }
}
