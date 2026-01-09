using DotCelery.Backend.Redis.DelayedMessageStore;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.Redis;

namespace DotCelery.Tests.Integration.Redis;

/// <summary>
/// Integration tests for Redis delayed message store using Testcontainers.
/// These tests require Docker to be running.
/// </summary>
[Collection("Redis")]
public class RedisDelayedMessageStoreIntegrationTests : IAsyncLifetime
{
    private readonly RedisContainer _container;
    private RedisDelayedMessageStore? _store;

    public RedisDelayedMessageStoreIntegrationTests()
    {
        _container = new RedisBuilder("redis:7-alpine").Build();
    }

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();

        var options = Options.Create(
            new RedisDelayedMessageStoreOptions
            {
                ConnectionString = _container.GetConnectionString(),
            }
        );

        var logger = LoggerFactory
            .Create(builder => builder.AddConsole())
            .CreateLogger<RedisDelayedMessageStore>();

        _store = new RedisDelayedMessageStore(options, logger);
    }

    public async ValueTask DisposeAsync()
    {
        if (_store is not null)
        {
            await _store.DisposeAsync();
        }

        await _container.DisposeAsync();
    }

    [Fact]
    public async Task AddAsync_StoresMessage()
    {
        var message = CreateTestMessage();
        var deliveryTime = DateTimeOffset.UtcNow.AddMinutes(5);

        await _store!.AddAsync(message, deliveryTime);

        var count = await _store.GetPendingCountAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task GetPendingCountAsync_ReturnsCorrectCount()
    {
        var messages = Enumerable.Range(0, 5).Select(_ => CreateTestMessage()).ToList();
        var deliveryTime = DateTimeOffset.UtcNow.AddMinutes(5);

        foreach (var message in messages)
        {
            await _store!.AddAsync(message, deliveryTime);
        }

        var count = await _store!.GetPendingCountAsync();
        Assert.Equal(5, count);
    }

    [Fact]
    public async Task GetDueMessagesAsync_ReturnsOnlyDueMessages()
    {
        var dueMessage = CreateTestMessage();
        var futureMessage = CreateTestMessage();

        // Add a message due in the past (should be returned)
        await _store!.AddAsync(dueMessage, DateTimeOffset.UtcNow.AddSeconds(-1));

        // Add a message due in the future (should not be returned)
        await _store.AddAsync(futureMessage, DateTimeOffset.UtcNow.AddMinutes(5));

        var dueMessages = new List<TaskMessage>();
        await foreach (var msg in _store.GetDueMessagesAsync(DateTimeOffset.UtcNow))
        {
            dueMessages.Add(msg);
        }

        Assert.Single(dueMessages);
        Assert.Equal(dueMessage.Id, dueMessages[0].Id);
    }

    [Fact]
    public async Task GetDueMessagesAsync_RemovesReturnedMessages()
    {
        var message = CreateTestMessage();
        await _store!.AddAsync(message, DateTimeOffset.UtcNow.AddSeconds(-1));

        // First call should return the message
        var firstCall = new List<TaskMessage>();
        await foreach (var msg in _store.GetDueMessagesAsync(DateTimeOffset.UtcNow))
        {
            firstCall.Add(msg);
        }

        Assert.Single(firstCall);

        // Second call should return nothing (message was removed)
        var secondCall = new List<TaskMessage>();
        await foreach (var msg in _store.GetDueMessagesAsync(DateTimeOffset.UtcNow))
        {
            secondCall.Add(msg);
        }

        Assert.Empty(secondCall);
    }

    [Fact]
    public async Task RemoveAsync_RemovesExistingMessage()
    {
        var message = CreateTestMessage();
        await _store!.AddAsync(message, DateTimeOffset.UtcNow.AddMinutes(5));

        var removed = await _store.RemoveAsync(message.Id);

        Assert.True(removed);
        var count = await _store.GetPendingCountAsync();
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task RemoveAsync_ReturnsFalseForNonExistent()
    {
        var removed = await _store!.RemoveAsync("non-existent-task-id");

        Assert.False(removed);
    }

    [Fact]
    public async Task GetNextDeliveryTimeAsync_ReturnsEarliestTime()
    {
        var earlyTime = DateTimeOffset.UtcNow.AddMinutes(1);
        var lateTime = DateTimeOffset.UtcNow.AddMinutes(10);

        await _store!.AddAsync(CreateTestMessage(), lateTime);
        await _store.AddAsync(CreateTestMessage(), earlyTime);

        var nextDelivery = await _store.GetNextDeliveryTimeAsync();

        Assert.NotNull(nextDelivery);
        // Allow 1 second tolerance for timing
        Assert.True(
            Math.Abs((nextDelivery.Value - earlyTime).TotalSeconds) < 1,
            $"Expected {earlyTime}, got {nextDelivery.Value}"
        );
    }

    [Fact]
    public async Task GetNextDeliveryTimeAsync_ReturnsNullWhenEmpty()
    {
        var nextDelivery = await _store!.GetNextDeliveryTimeAsync();

        Assert.Null(nextDelivery);
    }

    [Fact]
    public async Task AddAsync_UpdatesExistingMessage()
    {
        var message = CreateTestMessage();
        var originalTime = DateTimeOffset.UtcNow.AddMinutes(5);
        var newTime = DateTimeOffset.UtcNow.AddMinutes(10);

        await _store!.AddAsync(message, originalTime);
        await _store.AddAsync(message, newTime);

        // Should still only have one message
        var count = await _store.GetPendingCountAsync();
        Assert.Equal(1, count);

        // Next delivery time should be the new time
        var nextDelivery = await _store.GetNextDeliveryTimeAsync();
        Assert.NotNull(nextDelivery);
        Assert.True(
            Math.Abs((nextDelivery.Value - newTime).TotalSeconds) < 1,
            $"Expected {newTime}, got {nextDelivery.Value}"
        );
    }

    [Fact]
    public async Task ConcurrentAddAndRetrieve_Works()
    {
        var tasks = Enumerable
            .Range(0, 10)
            .Select(async i =>
            {
                var message = CreateTestMessage();
                await _store!.AddAsync(message, DateTimeOffset.UtcNow.AddSeconds(-1));
                return message.Id;
            });

        var taskIds = await Task.WhenAll(tasks);

        // Retrieve all due messages
        var retrieved = new List<TaskMessage>();
        await foreach (var msg in _store!.GetDueMessagesAsync(DateTimeOffset.UtcNow))
        {
            retrieved.Add(msg);
        }

        Assert.Equal(10, retrieved.Count);
        Assert.All(taskIds, id => Assert.Contains(retrieved, m => m.Id == id));
    }

    [Fact]
    public async Task GetDueMessagesAsync_PreservesMessageContent()
    {
        var message = CreateTestMessage() with
        {
            Task = "custom.task",
            Queue = "custom-queue",
            Priority = 5,
        };
        await _store!.AddAsync(message, DateTimeOffset.UtcNow.AddSeconds(-1));

        TaskMessage? retrieved = null;
        await foreach (var msg in _store.GetDueMessagesAsync(DateTimeOffset.UtcNow))
        {
            retrieved = msg;
            break;
        }

        Assert.NotNull(retrieved);
        Assert.Equal(message.Id, retrieved.Id);
        Assert.Equal(message.Task, retrieved.Task);
        Assert.Equal(message.Queue, retrieved.Queue);
        Assert.Equal(message.Priority, retrieved.Priority);
    }

    private static TaskMessage CreateTestMessage() =>
        new()
        {
            Id = Guid.NewGuid().ToString(),
            Task = "test.task",
            Args = "{\"value\": 42}"u8.ToArray(),
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Queue = "celery",
        };
}
