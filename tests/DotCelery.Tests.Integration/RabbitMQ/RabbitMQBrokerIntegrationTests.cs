using System.Threading.Channels;
using DotCelery.Broker.RabbitMQ;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Tests.Conformance.Stores;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using Testcontainers.RabbitMq;

namespace DotCelery.Tests.Integration.RabbitMQ;

/// <summary>
/// Integration tests for RabbitMQ broker using Testcontainers.
/// These tests require Docker to be running.
/// </summary>
[Collection("RabbitMQ")]
public class RabbitMQBrokerIntegrationTests : IAsyncLifetime
{
    private readonly RabbitMqContainer _container;
    private RabbitMQBroker? _broker;

    public RabbitMQBrokerIntegrationTests()
    {
        _container = new RabbitMqBuilder("rabbitmq:3.13-management-alpine").Build();
    }

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();

        var options = Options.Create(
            new RabbitMQBrokerOptions
            {
                ConnectionString = _container.GetConnectionString(),
                PrefetchCount = 1,
                ConnectionRetryCount = 1,
                ConnectionRetryDelay = TimeSpan.FromSeconds(1),
                ReconnectDelay = TimeSpan.FromSeconds(1),
            }
        );

        var logger = LoggerFactory
            .Create(builder => builder.AddConsole())
            .CreateLogger<RabbitMQBroker>();

        _broker = new RabbitMQBroker(options, logger);
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
    public async Task Reconnect_RedeliveryCannotBeSettledWithTheOldReceipt()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        const string queue = "stale-reconnected";
        await _broker!.PublishAsync(CreateTestMessage() with { Queue = queue });
        await using var consumer = _broker
            .ConsumeAsync([queue], timeout.Token)
            .GetAsyncEnumerator();
        Assert.True(await consumer.MoveNextAsync());
        var stale = consumer.Current;
        var closed = await _container.ExecAsync([
            "rabbitmqctl",
            "close_all_connections",
            "stale delivery test",
        ]);
        Assert.Equal(0, closed.ExitCode);
        Assert.True(await consumer.MoveNextAsync());
        var current = consumer.Current;
        Assert.Equal(stale.Message.Id, current.Message.Id);
        Assert.NotEqual(stale.DeliveryTag, current.DeliveryTag);
        await Assert.ThrowsAsync<InvalidOperationException>(() => _broker.AckAsync(stale).AsTask());
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _broker.RejectAsync(stale, true).AsTask()
        );
        await _broker.AckAsync(current);
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
    public async Task RejectAsync_WithRequeue_MessageIsRedelivered()
    {
        var message = CreateTestMessage();
        await _broker!.PublishAsync(message);

        // First consume and reject
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

        // Dispose the first broker to ensure all unacked messages are returned to queue
        // This simulates a worker gracefully shutting down
        await _broker!.DisposeAsync();
        _broker = null;

        // Allow RabbitMQ time to process the requeue and make the message available
        await Task.Delay(500);

        // Create a second broker instance to simulate a different worker
        var options2 = Options.Create(
            new RabbitMQBrokerOptions
            {
                ConnectionString = _container.GetConnectionString(),
                PrefetchCount = 1,
            }
        );
        var logger2 = LoggerFactory
            .Create(builder => builder.AddConsole())
            .CreateLogger<RabbitMQBroker>();
        await using var broker2 = new RabbitMQBroker(options2, logger2);

        // Second consume should get the same message
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
    public async Task ConsumeAsync_AfterTheConnectionIsLost_PausesAndKeepsConsuming()
    {
        var queue = "recovery-queue";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var received = Channel.CreateUnbounded<BrokerMessage>();

        // One consumer for the whole test, as a worker has: it must survive the connection loss
        var consuming = Task.Run(
            async () =>
            {
                try
                {
                    await foreach (var message in _broker!.ConsumeAsync([queue], cts.Token))
                    {
                        await received.Writer.WriteAsync(message, cts.Token).ConfigureAwait(false);
                        await _broker.AckAsync(message, cts.Token).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException) when (cts.IsCancellationRequested)
                {
                    // Expected when the test ends
                }
            },
            cts.Token
        );

        var before = await PublishUntilReceivedAsync(queue, received.Reader, cts.Token);

        // Lose the connection under the running consumer, as a broker restart or a network
        // reset does. The container keeps its port, so the broker can reconnect to it.
        var closed = await _container.ExecAsync([
            "rabbitmqctl",
            "close_all_connections",
            "lost by test",
        ]);
        Assert.Equal(0, closed.ExitCode);

        var after = await PublishUntilReceivedAsync(queue, received.Reader, cts.Token);

        Assert.NotEqual(before.Message.Id, after.Message.Id);

        await cts.CancelAsync();
        await consuming;
    }

    private async Task<BrokerMessage> PublishUntilReceivedAsync(
        string queue,
        ChannelReader<BrokerMessage> reader,
        CancellationToken cancellationToken
    )
    {
        while (true)
        {
            try
            {
                await _broker!
                    .PublishAsync(CreateTestMessage() with { Queue = queue }, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception) when (!cancellationToken.IsCancellationRequested)
            {
                // The broker is not reachable yet; publish again on the next round
            }

            var read = reader.ReadAsync(cancellationToken).AsTask();
            if (
                await Task.WhenAny(read, Task.Delay(TimeSpan.FromSeconds(2), cancellationToken))
                == read
            )
            {
                return await read;
            }
        }
    }

    [Fact]
    public async Task PublishAsync_WithPriority_Succeeds()
    {
        var lowPriority = CreateTestMessage() with { Priority = 1 };
        var highPriority = CreateTestMessage() with { Priority = 9 };

        await _broker!.PublishAsync(lowPriority);
        await _broker.PublishAsync(highPriority);

        // Both messages should be consumable
        var received = new List<BrokerMessage>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await foreach (var msg in _broker!.ConsumeAsync(["celery"], cts.Token))
        {
            received.Add(msg);
            await _broker!.AckAsync(msg);
            if (received.Count >= 2)
                break;
        }

        Assert.Equal(2, received.Count);
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
    public async Task Queues_AreDeclaredWithTheConfiguredArguments()
    {
        var options = Options.Create(
            new RabbitMQBrokerOptions
            {
                ConnectionString = _container.GetConnectionString(),
                QueueArguments = new Dictionary<string, object?>
                {
                    ["x-max-priority"] = 5,
                    ["x-message-ttl"] = 60_000,
                },
            }
        );
        await using var broker = new RabbitMQBroker(options, CreateLogger());

        // Publishing declares the queue with the configured arguments
        await broker.PublishAsync(CreateTestMessage() with { Queue = "queue-arguments" });

        await AssertQueueArgumentsAsync(
            "queue-arguments",
            new Dictionary<string, object?> { ["x-max-priority"] = 5, ["x-message-ttl"] = 60_000 },
            new Dictionary<string, object?> { ["x-message-ttl"] = 60_000 }
        );
    }

    [Fact]
    public async Task Queues_UseTheDefaultPriorityArgumentWhenNotConfigured()
    {
        await _broker!.PublishAsync(CreateTestMessage());

        await AssertQueueArgumentsAsync(
            "celery",
            new Dictionary<string, object?> { ["x-max-priority"] = 10 },
            new Dictionary<string, object?> { ["x-max-priority"] = 9 }
        );
    }

    /// <summary>
    /// Proves which arguments a queue was declared with: declaring it again with the same
    /// arguments succeeds, and with different ones the broker refuses the declaration
    /// (PRECONDITION_FAILED).
    /// </summary>
    private async Task AssertQueueArgumentsAsync(
        string queue,
        Dictionary<string, object?> matching,
        Dictionary<string, object?> mismatching
    )
    {
        var factory = new ConnectionFactory { Uri = new Uri(_container.GetConnectionString()) };
        await using var connection = await factory.CreateConnectionAsync();

        await using (var channel = await connection.CreateChannelAsync())
        {
            await channel.QueueDeclareAsync(
                queue: queue,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: matching
            );
        }

        await using (var channel = await connection.CreateChannelAsync())
        {
            var mismatch = await Assert.ThrowsAnyAsync<Exception>(() =>
                channel.QueueDeclareAsync(
                    queue: queue,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: mismatching
                )
            );

            Assert.Contains(
                "PRECONDITION_FAILED",
                mismatch.ToString(),
                StringComparison.OrdinalIgnoreCase
            );
        }
    }

    [Fact]
    public async Task PublishAsync_WithQueueMetrics_RecordsTheEnqueue()
    {
        var metrics = new RecordingQueueMetrics();
        await using var broker = new RabbitMQBroker(
            Options.Create(
                new RabbitMQBrokerOptions { ConnectionString = _container.GetConnectionString() }
            ),
            CreateLogger(),
            queueMetrics: metrics
        );

        await broker.PublishAsync(CreateTestMessage() with { Queue = "metrics-queue" });
        await broker.PublishAsync(CreateTestMessage() with { Queue = "metrics-queue" });

        Assert.Equal(2, metrics.CountOf("metrics-queue"));
        Assert.DoesNotContain(metrics.Enqueued, queue => queue != "metrics-queue");
    }

    [Fact]
    public async Task PublishAsync_OfAnOversizedMessage_RecordsNothing()
    {
        var metrics = new RecordingQueueMetrics();
        await using var broker = new RabbitMQBroker(
            Options.Create(
                new RabbitMQBrokerOptions
                {
                    ConnectionString = _container.GetConnectionString(),
                    MaxMessageSizeBytes = 1,
                }
            ),
            CreateLogger(),
            queueMetrics: metrics
        );

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await broker.PublishAsync(CreateTestMessage())
        );

        Assert.Empty(metrics.Enqueued);
    }

    [Fact]
    public async Task ConcurrentPublishingAndSettlement_PreserveEveryMessage()
    {
        var queue = $"concurrent-{Guid.NewGuid():N}";
        await using var broker = new RabbitMQBroker(
            Options.Create(
                new RabbitMQBrokerOptions
                {
                    ConnectionString = _container.GetConnectionString(),
                    PrefetchCount = 64,
                }
            ),
            CreateLogger()
        );
        var messages = Enumerable
            .Range(0, 100)
            .Select(_ => CreateTestMessage() with { Queue = queue, Args = new byte[32_768] })
            .ToArray();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await Task.WhenAll(
            messages.Select(message => broker.PublishAsync(message, timeout.Token).AsTask())
        );
        var received = new List<BrokerMessage>();
        await using var consumer = broker.ConsumeAsync([queue], timeout.Token).GetAsyncEnumerator();
        while (received.Count < messages.Length)
        {
            Assert.True(await consumer.MoveNextAsync());
            received.Add(consumer.Current);
            if (received.Count % 25 == 0)
            {
                await Task.WhenAll(
                    received
                        .TakeLast(25)
                        .Select(
                            (message, i) =>
                                i % 2 == 0
                                    ? broker.AckAsync(message, timeout.Token).AsTask()
                                    : broker.RejectAsync(message, false, timeout.Token).AsTask()
                        )
                );
            }
        }
        Assert.Equal(
            messages.Select(m => m.Id).Order(),
            received.Select(m => m.Message.Id).Order()
        );
        Assert.True(await broker.IsHealthyAsync(timeout.Token));
    }

    [Fact]
    public async Task OversizedDirectBrokerWrite_IsRejectedBeforeDeserializing()
    {
        var queue = $"size-{Guid.NewGuid():N}";
        await using var receiver = new RabbitMQBroker(
            Options.Create(
                new RabbitMQBrokerOptions
                {
                    ConnectionString = _container.GetConnectionString(),
                    MaxMessageSizeBytes = 2048,
                }
            ),
            CreateLogger()
        );
        await _broker!.PublishAsync(
            CreateTestMessage() with
            {
                Queue = queue,
                Args = new byte[4096],
            }
        );
        var valid = CreateTestMessage() with { Queue = queue };
        await _broker.PublishAsync(valid);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await foreach (var delivery in receiver.ConsumeAsync([queue], timeout.Token))
        {
            Assert.Equal(valid.Id, delivery.Message.Id);
            await receiver.AckAsync(delivery, timeout.Token);
            break;
        }
    }

    [Fact]
    public async Task CancelledAcknowledgement_RetainsDeliveryOwnershipForRetry()
    {
        var queue = $"ack-retry-{Guid.NewGuid():N}";
        await _broker!.PublishAsync(CreateTestMessage() with { Queue = queue });
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var consumer = _broker
            .ConsumeAsync([queue], timeout.Token)
            .GetAsyncEnumerator();
        Assert.True(await consumer.MoveNextAsync());
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            _broker.AckAsync(consumer.Current, cancelled.Token).AsTask()
        );
        await _broker.AckAsync(consumer.Current, timeout.Token);
        Assert.True(await _broker.IsHealthyAsync(timeout.Token));
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

    private static ILogger<RabbitMQBroker> CreateLogger() =>
        LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger<RabbitMQBroker>();
}
