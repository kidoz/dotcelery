using System.Threading.Channels;
using DotCelery.Broker.RabbitMQ;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
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
