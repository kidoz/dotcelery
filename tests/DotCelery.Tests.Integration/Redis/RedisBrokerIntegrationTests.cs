using DotCelery.Broker.Redis;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.Redis;

namespace DotCelery.Tests.Integration.Redis;

/// <summary>
/// Integration tests for Redis Streams broker using Testcontainers.
/// These tests require Docker to be running.
/// </summary>
[Collection("Redis")]
public class RedisBrokerIntegrationTests : IAsyncLifetime
{
    private readonly RedisContainer _container;
    private RedisBroker? _broker;

    public RedisBrokerIntegrationTests()
    {
        _container = new RedisBuilder("redis:7-alpine").Build();
    }

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();

        var options = Options.Create(
            new RedisBrokerOptions
            {
                ConnectionString = _container.GetConnectionString(),
                PrefetchCount = 10,
                BlockTimeout = TimeSpan.FromMilliseconds(100),
                ClaimTimeout = TimeSpan.FromSeconds(5),
            }
        );

        var logger = LoggerFactory
            .Create(builder => builder.AddConsole())
            .CreateLogger<RedisBroker>();

        _broker = new RedisBroker(options, logger);
    }

    public async ValueTask DisposeAsync()
    {
        if (_broker is not null)
        {
            await _broker.DisposeAsync();
        }

        await _container.DisposeAsync();
    }

    [Fact]
    public async Task IsHealthyAsync_ConnectedBroker_ReturnsTrue()
    {
        var healthy = await _broker!.IsHealthyAsync();

        Assert.True(healthy);
    }

    [Fact]
    public async Task PublishAsync_ValidMessage_Succeeds()
    {
        var message = CreateTestMessage();

        // Should not throw
        await _broker!.PublishAsync(message);
    }

    [Fact]
    public async Task PublishAndConsume_SingleMessage_Works()
    {
        var message = CreateTestMessage();
        await _broker!.PublishAsync(message);

        BrokerMessage? received = null;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await foreach (var msg in _broker!.ConsumeAsync(["celery"], cts.Token))
        {
            received = msg;
            await _broker!.AckAsync(msg);
            break;
        }

        Assert.NotNull(received);
        Assert.Equal(message.Id, received.Message.Id);
        Assert.Equal(message.Task, received.Message.Task);
    }

    [Fact]
    public async Task PublishAndConsume_MultipleMessages_Works()
    {
        var messages = Enumerable.Range(0, 5).Select(_ => CreateTestMessage()).ToList();

        foreach (var message in messages)
        {
            await _broker!.PublishAsync(message);
        }

        var received = new List<BrokerMessage>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await foreach (var msg in _broker!.ConsumeAsync(["celery"], cts.Token))
        {
            received.Add(msg);
            await _broker!.AckAsync(msg);

            if (received.Count >= 5)
            {
                break;
            }
        }

        Assert.Equal(5, received.Count);
    }

    [Fact]
    public async Task PublishAndConsume_MultipleQueues_Works()
    {
        var message1 = CreateTestMessage() with { Queue = "queue1" };
        var message2 = CreateTestMessage() with { Queue = "queue2" };

        await _broker!.PublishAsync(message1);
        await _broker!.PublishAsync(message2);

        var received = new List<BrokerMessage>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await foreach (var msg in _broker!.ConsumeAsync(["queue1", "queue2"], cts.Token))
        {
            received.Add(msg);
            await _broker!.AckAsync(msg);

            if (received.Count >= 2)
            {
                break;
            }
        }

        Assert.Equal(2, received.Count);
        Assert.Contains(received, m => m.Queue == "queue1");
        Assert.Contains(received, m => m.Queue == "queue2");
    }

    [Fact]
    public async Task RejectAsync_WithRequeue_MessageIsRedelivered()
    {
        var message = CreateTestMessage();
        await _broker!.PublishAsync(message);

        // First consume and reject with requeue
        BrokerMessage? first = null;
        using var cts1 = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await foreach (var msg in _broker!.ConsumeAsync(["celery"], cts1.Token))
        {
            first = msg;
            await _broker!.RejectAsync(msg, requeue: true);
            break;
        }

        Assert.NotNull(first);
        var firstId = first.Message.Id;

        // Dispose the first broker to release the message
        await _broker!.DisposeAsync();
        _broker = null;

        // Wait for claim timeout to allow message to be reclaimed
        await Task.Delay(TimeSpan.FromSeconds(6));

        // Create a second broker instance to simulate a different worker
        var options2 = Options.Create(
            new RedisBrokerOptions
            {
                ConnectionString = _container.GetConnectionString(),
                PrefetchCount = 10,
                BlockTimeout = TimeSpan.FromMilliseconds(100),
                ClaimTimeout = TimeSpan.FromSeconds(1), // Short claim timeout for test
            }
        );
        var logger2 = LoggerFactory
            .Create(builder => builder.AddConsole())
            .CreateLogger<RedisBroker>();
        await using var broker2 = new RedisBroker(options2, logger2);

        // Second consume should get the same message (reclaimed from pending)
        BrokerMessage? second = null;
        using var cts2 = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await foreach (var msg in broker2.ConsumeAsync(["celery"], cts2.Token))
        {
            second = msg;
            await broker2.AckAsync(msg);
            break;
        }

        Assert.NotNull(second);
        Assert.Equal(firstId, second.Message.Id);
    }

    [Fact]
    public async Task RejectAsync_WithoutRequeue_MessageIsLost()
    {
        var message = CreateTestMessage();
        await _broker!.PublishAsync(message);

        // Consume and reject without requeue
        BrokerMessage? first = null;
        using var cts1 = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await foreach (var msg in _broker!.ConsumeAsync(["celery"], cts1.Token))
        {
            first = msg;
            await _broker!.RejectAsync(msg, requeue: false);
            break;
        }

        Assert.NotNull(first);

        // Try to consume again - should timeout with no message
        using var cts2 = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var received = new List<BrokerMessage>();

        try
        {
            await foreach (var msg in _broker!.ConsumeAsync(["celery"], cts2.Token))
            {
                received.Add(msg);
                break;
            }
        }
        catch (OperationCanceledException)
        {
            // Expected - no more messages
        }

        Assert.Empty(received);
    }

    [Fact]
    public async Task PublishAsync_ToCustomQueue_Succeeds()
    {
        var message = CreateTestMessage() with { Queue = "custom-queue" };

        await _broker!.PublishAsync(message);

        BrokerMessage? received = null;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await foreach (var msg in _broker!.ConsumeAsync(["custom-queue"], cts.Token))
        {
            received = msg;
            await _broker!.AckAsync(msg);
            break;
        }

        Assert.NotNull(received);
        Assert.Equal("custom-queue", received.Queue);
    }

    [Fact]
    public async Task PublishAsync_MessageProperties_Preserved()
    {
        var message = CreateTestMessage() with
        {
            Priority = 5,
            CorrelationId = "test-correlation-123",
            ParentId = "parent-task-456",
            RootId = "root-task-789",
            Headers = new Dictionary<string, string> { ["custom"] = "header" },
        };

        await _broker!.PublishAsync(message);

        BrokerMessage? received = null;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await foreach (var msg in _broker!.ConsumeAsync(["celery"], cts.Token))
        {
            received = msg;
            await _broker!.AckAsync(msg);
            break;
        }

        Assert.NotNull(received);
        Assert.Equal(message.Id, received.Message.Id);
        Assert.Equal(message.Priority, received.Message.Priority);
        Assert.Equal(message.CorrelationId, received.Message.CorrelationId);
        Assert.Equal(message.ParentId, received.Message.ParentId);
        Assert.Equal(message.RootId, received.Message.RootId);
        Assert.NotNull(received.Message.Headers);
        Assert.Equal("header", received.Message.Headers["custom"]);
    }

    [Fact]
    public async Task ConcurrentPublish_MultipleMessages_AllDelivered()
    {
        const int messageCount = 20;
        var messages = Enumerable.Range(0, messageCount).Select(_ => CreateTestMessage()).ToList();

        // Publish concurrently
        await Task.WhenAll(messages.Select(m => _broker!.PublishAsync(m).AsTask()));

        // Consume all messages
        var received = new List<BrokerMessage>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await foreach (var msg in _broker!.ConsumeAsync(["celery"], cts.Token))
        {
            received.Add(msg);
            await _broker!.AckAsync(msg);

            if (received.Count >= messageCount)
            {
                break;
            }
        }

        Assert.Equal(messageCount, received.Count);

        // Verify all unique messages received
        var receivedIds = received.Select(m => m.Message.Id).ToHashSet();
        var sentIds = messages.Select(m => m.Id).ToHashSet();
        Assert.Equal(sentIds, receivedIds);
    }

    [Fact]
    public async Task DeliveryTag_IsValidString()
    {
        var message = CreateTestMessage();
        await _broker!.PublishAsync(message);

        BrokerMessage? received = null;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await foreach (var msg in _broker!.ConsumeAsync(["celery"], cts.Token))
        {
            received = msg;
            await _broker!.AckAsync(msg);
            break;
        }

        Assert.NotNull(received);
        Assert.IsType<string>(received.DeliveryTag);

        var deliveryTag = (string)received.DeliveryTag;
        Assert.Contains("dotcelery:stream:celery:", deliveryTag);
        Assert.Contains("-", deliveryTag); // Stream ID format: timestamp-sequence
    }

    [Fact]
    public async Task ConsumerGroup_MultipleConsumers_MessagesDistributed()
    {
        // Publish multiple messages
        const int messageCount = 10;
        for (var i = 0; i < messageCount; i++)
        {
            await _broker!.PublishAsync(CreateTestMessage());
        }

        // Create second consumer with same consumer group
        var options2 = Options.Create(
            new RedisBrokerOptions
            {
                ConnectionString = _container.GetConnectionString(),
                PrefetchCount = 10,
                BlockTimeout = TimeSpan.FromMilliseconds(100),
                ConsumerName = "consumer-2", // Different consumer name
            }
        );
        var logger2 = LoggerFactory
            .Create(builder => builder.AddConsole())
            .CreateLogger<RedisBroker>();
        await using var broker2 = new RedisBroker(options2, logger2);

        // Both consumers consume from the same group
        var received1 = new List<BrokerMessage>();
        var received2 = new List<BrokerMessage>();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        // Run both consumers concurrently
        var task1 = Task.Run(async () =>
        {
            await foreach (var msg in _broker!.ConsumeAsync(["celery"], cts.Token))
            {
                received1.Add(msg);
                await _broker!.AckAsync(msg);
                if (received1.Count + received2.Count >= messageCount)
                {
                    await cts.CancelAsync();
                }
            }
        });

        var task2 = Task.Run(async () =>
        {
            await foreach (var msg in broker2.ConsumeAsync(["celery"], cts.Token))
            {
                received2.Add(msg);
                await broker2.AckAsync(msg);
                if (received1.Count + received2.Count >= messageCount)
                {
                    await cts.CancelAsync();
                }
            }
        });

        try
        {
            await Task.WhenAll(task1, task2);
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        // All messages should be consumed between the two consumers
        var totalReceived = received1.Count + received2.Count;
        Assert.Equal(messageCount, totalReceived);

        // No duplicate messages
        var allIds = received1.Concat(received2).Select(m => m.Message.Id).ToList();
        Assert.Equal(allIds.Count, allIds.Distinct().Count());
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
