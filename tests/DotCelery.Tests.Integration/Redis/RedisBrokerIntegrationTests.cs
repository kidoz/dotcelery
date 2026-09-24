using System.Diagnostics;
using DotCelery.Broker.Redis;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
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

    [Fact]
    public async Task ConsumeAsync_BacklogLargerThanOneRead_IsDrainedWithoutIdleWaits()
    {
        // The idle poll interval applies only when a read returns nothing
        await using var broker = CreateBroker(o =>
        {
            o.BlockTimeout = TimeSpan.FromSeconds(5);
            o.PrefetchCount = 5;
        });

        const int messageCount = 20;
        for (var i = 0; i < messageCount; i++)
        {
            await broker.PublishAsync(CreateTestMessage());
        }

        var received = 0;
        var stopwatch = Stopwatch.StartNew();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await foreach (var msg in broker.ConsumeAsync(["celery"], cts.Token))
        {
            await broker.AckAsync(msg);
            if (++received == messageCount)
            {
                break;
            }
        }

        Assert.Equal(messageCount, received);
        Assert.True(
            stopwatch.Elapsed < TimeSpan.FromSeconds(4),
            $"Draining took {stopwatch.Elapsed}"
        );
    }

    [Fact]
    public async Task ConsumeAsync_StreamDeleted_RecreatesConsumerGroupAndContinues()
    {
        await using var broker = CreateBroker();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var consumer = broker
            .ConsumeAsync(["celery"], cts.Token)
            .GetAsyncEnumerator(cts.Token);

        await broker.PublishAsync(CreateTestMessage());
        Assert.True(await consumer.MoveNextAsync());
        await broker.AckAsync(consumer.Current);

        // Deleting the stream also deletes its consumer group, as FLUSHDB or eviction would
        await using (
            var redis = await ConnectionMultiplexer.ConnectAsync(_container.GetConnectionString())
        )
        {
            await redis.GetDatabase().KeyDeleteAsync("dotcelery:stream:celery");
        }

        var next = CreateTestMessage();
        await broker.PublishAsync(next);

        Assert.True(await consumer.MoveNextAsync());
        Assert.Equal(next.Id, consumer.Current.Message.Id);
    }

    [Fact]
    public async Task ConsumeAsync_StoppedWithBufferedMessages_ReturnsThemToOtherConsumers()
    {
        // A long claim timeout: other consumers must not have to wait for a reclaim
        await using var first = CreateBroker(o =>
        {
            o.ConsumerName = "consumer-1";
            o.ClaimTimeout = TimeSpan.FromMinutes(5);
        });
        await using var second = CreateBroker(o =>
        {
            o.ConsumerName = "consumer-2";
            o.ClaimTimeout = TimeSpan.FromMinutes(5);
        });

        const int messageCount = 5;
        for (var i = 0; i < messageCount; i++)
        {
            await first.PublishAsync(CreateTestMessage());
        }

        // Take one message, then stop consuming while the rest are buffered
        BrokerMessage? taken = null;
        using var cts1 = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await foreach (var msg in first.ConsumeAsync(["celery"], cts1.Token))
        {
            taken = msg;
            break;
        }

        Assert.NotNull(taken);

        var received = new List<BrokerMessage>();
        using var cts2 = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await foreach (var msg in second.ConsumeAsync(["celery"], cts2.Token))
        {
            received.Add(msg);
            await second.AckAsync(msg);
            if (received.Count == messageCount - 1)
            {
                break;
            }
        }

        Assert.Equal(messageCount - 1, received.Count);
        Assert.DoesNotContain(received, m => m.Message.Id == taken.Message.Id);
    }

    private RedisBroker CreateBroker(Action<RedisBrokerOptions>? configure = null)
    {
        var options = new RedisBrokerOptions
        {
            ConnectionString = _container.GetConnectionString(),
            PrefetchCount = 10,
            BlockTimeout = TimeSpan.FromMilliseconds(100),
            ClaimTimeout = TimeSpan.FromSeconds(5),
        };
        configure?.Invoke(options);

        var logger = LoggerFactory
            .Create(builder => builder.AddConsole())
            .CreateLogger<RedisBroker>();

        return new RedisBroker(Options.Create(options), logger);
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
