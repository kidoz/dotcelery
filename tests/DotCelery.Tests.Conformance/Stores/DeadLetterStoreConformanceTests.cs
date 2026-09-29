using System.Text.Json;
using DotCelery.Core.Abstractions;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage.Stores;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Conformance.Stores;

/// <summary>
/// Conformance tests for <see cref="DeadLetterStore"/>.
/// </summary>
public abstract class DeadLetterStoreConformanceTests : StoreConformanceTests
{
    private readonly RecordingBroker _broker = new();

    [Fact]
    public async Task StoreAsync_RoundTripsTheMessage()
    {
        var store = CreateStore();
        var message = DeadLetter("dl-1", Start) with
        {
            ExceptionMessage = "boom",
            ExceptionType = "System.Exception",
            RetryCount = 3,
            Worker = "worker-1",
        };

        await store.StoreAsync(message);
        var stored = await CreateStore().GetAsync("dl-1");

        Assert.NotNull(stored);
        Assert.Equal(message.OriginalMessage, stored.OriginalMessage);
        Assert.Equal(DeadLetterReason.MaxRetriesExceeded, stored.Reason);
        Assert.Equal("boom", stored.ExceptionMessage);
        Assert.Equal(3, stored.RetryCount);
        Assert.Equal(Start, stored.Timestamp);
        Assert.Null(await store.GetAsync("unknown"));
    }

    [Fact]
    public async Task GetAllAsync_ReturnsNewestFirstWithPaging()
    {
        var store = CreateStore();
        for (var i = 0; i < 5; i++)
        {
            await store.StoreAsync(DeadLetter($"dl-{i}", Start.AddMinutes(i)));
        }

        var page = await ToListAsync(store.GetAllAsync(limit: 2, offset: 1));

        Assert.Equal(["dl-3", "dl-2"], page.Select(m => m.Id));
        Assert.Equal(5, await store.GetCountAsync());
    }

    [Fact]
    public async Task StoreAsync_MessagesExpire()
    {
        var store = CreateStore(new DeadLetterOptions { RetentionPeriod = TimeSpan.FromHours(1) });
        await store.StoreAsync(DeadLetter("retained", Start));
        await store.StoreAsync(
            DeadLetter("expiring", Start) with
            {
                ExpiresAt = Start.AddMinutes(10),
            }
        );
        await store.StoreAsync(
            DeadLetter("expired", Start) with
            {
                ExpiresAt = Start.AddMinutes(-1),
            }
        );

        Assert.Equal(2, await store.GetCountAsync());
        Time.Advance(TimeSpan.FromMinutes(10));
        Assert.Null(await store.GetAsync("expiring"));
        Time.Advance(TimeSpan.FromMinutes(50));
        Assert.Equal(0, await store.GetCountAsync());
    }

    [Fact]
    public async Task StoreAsync_OverMaxMessages_RemovesTheOldest()
    {
        var store = CreateStore(new DeadLetterOptions { MaxMessages = 3 });
        for (var i = 0; i < 5; i++)
        {
            await store.StoreAsync(DeadLetter($"dl-{i}", Start.AddMinutes(i)));
        }

        var remaining = await ToListAsync(store.GetAllAsync());

        Assert.Equal(["dl-4", "dl-3", "dl-2"], remaining.Select(m => m.Id));
    }

    [Fact]
    public async Task RequeueAsync_PublishesTheOriginalMessageOnce()
    {
        var store = CreateStore();
        await store.StoreAsync(DeadLetter("dl-1", Start));

        var results = await RunConcurrentlyAsync(
            4,
            async _ => await CreateStore().RequeueAsync("dl-1")
        );

        Assert.Single(results, requeued => requeued);
        Assert.Equal("task-dl-1", Assert.Single(_broker.Published).Id);
        Assert.Null(await store.GetAsync("dl-1"));
        Assert.False(await store.RequeueAsync("unknown"));
    }

    [Fact]
    public async Task RequeueAsync_PublishFails_KeepsTheMessage()
    {
        var store = CreateStore();
        await store.StoreAsync(DeadLetter("dl-1", Start));
        _broker.Fail = true;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.RequeueAsync("dl-1").AsTask()
        );

        Assert.NotNull(await store.GetAsync("dl-1"));
    }

    [Fact]
    public async Task DeleteAndPurge_RemoveMessages()
    {
        var store = CreateStore();
        for (var i = 0; i < 3; i++)
        {
            await store.StoreAsync(DeadLetter($"dl-{i}", Start));
        }

        Assert.True(await store.DeleteAsync("dl-0"));
        Assert.False(await store.DeleteAsync("dl-0"));
        Assert.Equal(2, await store.PurgeAsync());
        Assert.Equal(0, await store.GetCountAsync());
    }

    [Fact]
    public async Task CleanupExpiredAsync_RemovesMessagesOlderThanTheRetention()
    {
        var store = CreateStore(new DeadLetterOptions { RetentionPeriod = TimeSpan.FromHours(1) });
        await store.StoreAsync(
            DeadLetter("old", Start.AddMinutes(-90)) with
            {
                ExpiresAt = Start.AddHours(1),
            }
        );
        await store.StoreAsync(DeadLetter("new", Start));

        Assert.Equal(1, await store.CleanupExpiredAsync());
        Assert.Equal(["new"], (await ToListAsync(store.GetAllAsync())).Select(m => m.Id));
    }

    private DeadLetterStore CreateStore(DeadLetterOptions? options = null) =>
        new(
            Provider,
            _broker,
            new JsonMessageSerializer(),
            Options.Create(options ?? new DeadLetterOptions()),
            CreateOptions(),
            Time
        );

    private static DeadLetterMessage DeadLetter(string id, DateTimeOffset timestamp) =>
        new()
        {
            Id = id,
            TaskId = $"task-{id}",
            TaskName = "tests.task",
            Queue = "celery",
            Reason = DeadLetterReason.MaxRetriesExceeded,
            OriginalMessage = JsonSerializer.SerializeToUtf8Bytes(
                CreateTaskMessage($"task-{id}"),
                DotCeleryJsonContext.Default.TaskMessage
            ),
            Timestamp = timestamp,
        };

    private static async Task<List<T>> ToListAsync<T>(IAsyncEnumerable<T> items)
    {
        var list = new List<T>();
        await foreach (var item in items)
        {
            list.Add(item);
        }

        return list;
    }
}
