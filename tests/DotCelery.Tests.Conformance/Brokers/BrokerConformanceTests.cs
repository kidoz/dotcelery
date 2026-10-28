using DotCelery.Core.Abstractions;
using DotCelery.Core.Canvas;
using DotCelery.Core.Models;

namespace DotCelery.Tests.Conformance.Brokers;

/// <summary>
/// Base class for message broker conformance tests: the delivery contract the client and the
/// worker rely on, held to the same behavior on every broker. A case a broker does not claim
/// through <see cref="IBrokerCapabilities"/> is skipped instead of silently differing.
/// </summary>
/// <remarks>
/// Each test uses its own queues, so a broker may be shared across tests.
/// </remarks>
public abstract class BrokerConformanceTests : IAsyncLifetime
{
    protected static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);

    protected IMessageBroker Broker { get; private set; } = null!;

    /// <summary>
    /// What the broker under test claims; a broker that declares nothing is held only to the
    /// delivery contract every broker honors.
    /// </summary>
    protected MessageBrokerCapabilities Capabilities =>
        (Broker as IBrokerCapabilities)?.Capabilities ?? new MessageBrokerCapabilities();

    /// <summary>
    /// Creates the broker under test. The test disposes it, and its transport with it.
    /// </summary>
    protected abstract ValueTask<IMessageBroker> CreateBrokerAsync();

    public async ValueTask InitializeAsync() => Broker = await CreateBrokerAsync();

    public virtual async ValueTask DisposeAsync() => await Broker.DisposeAsync();

    [Fact]
    public async Task PublishAndConsume_PreservesEveryMessageField()
    {
        var queue = Unique("preserve");
        var sent = CreateMessage(queue) with
        {
            // A past ETA travels like any other field; a future one is broker-specific, since a
            // broker with native delay (SQS) holds the message until it is due
            Eta = DateTimeOffset.UtcNow.AddMinutes(-1),
            Expires = DateTimeOffset.UtcNow.AddHours(1),
            Retries = 2,
            MaxRetries = 5,
            ParentId = "parent-1",
            RootId = "root-1",
            BatchId = "batch-1",
            CorrelationId = "correlation-1",
            Priority = 7,
            PartitionKey = "partition-1",
            TenantId = "tenant-1",
            Headers = new Dictionary<string, string> { ["trace"] = "abc" },
            Chain =
            [
                new ChainStep
                {
                    TaskId = "step-1",
                    Signature = new Signature { TaskName = "next.task" },
                },
            ],
        };

        await Broker.PublishAsync(sent);

        var received = await ConsumeAndAckAsync(queue);

        AssertMessageEquals(sent, received.Message);
    }

    [Fact]
    public async Task PublishAndConsume_DeliversOnlyToTheAddressedQueue()
    {
        var queue = Unique("addressed");
        var other = Unique("other");
        await Broker.PublishAsync(CreateMessage(queue));

        var fromOther = await TryConsumeOneAsync(other, TimeSpan.FromSeconds(1));
        Assert.Null(fromOther);

        var received = await ConsumeAndAckAsync(queue);
        Assert.Equal(queue, received.Queue);
    }

    [Fact]
    public async Task ConsumeAsync_FromSeveralQueues_DeliversFromAllQueues()
    {
        var first = Unique("multi-a");
        var second = Unique("multi-b");
        await Broker.PublishAsync(CreateMessage(first) with { Id = "a-1" });
        await Broker.PublishAsync(CreateMessage(second) with { Id = "b-1" });

        var ids = new HashSet<string>(StringComparer.Ordinal);
        using var timeout = new CancellationTokenSource(Timeout);

        await foreach (var message in Broker.ConsumeAsync([first, second], timeout.Token))
        {
            ids.Add(message.Message.Id);
            await Broker.AckAsync(message);

            if (ids.Count == 2)
            {
                break;
            }
        }

        Assert.Equal(["a-1", "b-1"], [.. ids.OrderBy(id => id, StringComparer.Ordinal)]);
    }

    [Fact]
    public async Task Ack_SettlesTheMessage()
    {
        var queue = Unique("ack");
        await Broker.PublishAsync(CreateMessage(queue));

        await ConsumeAndAckAsync(queue);

        // The consumer stopped, but the message was settled, so it is not delivered again
        var redelivered = await TryConsumeOneAsync(queue, TimeSpan.FromSeconds(2));
        Assert.Null(redelivered);
    }

    [Fact]
    public async Task RejectWithoutRequeue_DiscardsTheMessage()
    {
        var queue = Unique("discard");
        await Broker.PublishAsync(CreateMessage(queue));

        await ConsumeAndRejectAsync(queue, requeue: false);

        var redelivered = await TryConsumeOneAsync(queue, TimeSpan.FromSeconds(2));
        Assert.Null(redelivered);
    }

    [Fact]
    public async Task RejectWithRequeue_DeliversTheSameMessageAgain()
    {
        var queue = Unique("requeue");
        var sent = CreateMessage(queue);
        await Broker.PublishAsync(sent);

        var received = await ConsumeAndRejectAsync(queue, requeue: true);
        Assert.Equal(sent.Id, received.Message.Id);

        var redelivered = await ConsumeAndAckAsync(queue);
        Assert.Equal(sent.Id, redelivered.Message.Id);
    }

    [Fact]
    public async Task AnUnsettledMessage_IsDeliveredAgainWhenItsConsumerStops()
    {
        Assert.SkipWhen(
            !Capabilities.RedeliveryOnConsumerStop,
            "The broker does not redeliver a message whose consumer stopped"
        );

        var queue = Unique("unsettled");
        var sent = CreateMessage(queue);
        await Broker.PublishAsync(sent);

        // Consume and stop without settling
        var received = await ConsumeWithoutSettlingAsync(queue);
        Assert.Equal(sent.Id, received.Message.Id);

        var redelivered = await ConsumeAndAckAsync(queue);
        Assert.Equal(sent.Id, redelivered.Message.Id);
    }

    [Fact]
    public async Task HigherPriority_IsDeliveredFirst()
    {
        Assert.SkipWhen(!Capabilities.Priority, "The broker does not deliver by priority");

        var queue = Unique("priority");
        await Broker.PublishAsync(CreateMessage(queue) with { Id = "low", Priority = 1 });
        await Broker.PublishAsync(CreateMessage(queue) with { Id = "high", Priority = 9 });

        var received = await ConsumeAndAckAsync(queue);

        Assert.Equal("high", received.Message.Id);
    }

    [Fact]
    public async Task ConcurrentPublishes_AreAllDelivered()
    {
        const int count = 20;
        var queue = Unique("concurrent");

        await Task.WhenAll(
            Enumerable
                .Range(0, count)
                .Select(i =>
                    Broker.PublishAsync(CreateMessage(queue) with { Id = $"c-{i}" }).AsTask()
                )
        );

        // Delivery is at-least-once, so a repeated delivery is settled and counted rather than
        // failing the case; every published message must arrive
        var ids = new HashSet<string>(StringComparer.Ordinal);
        using var timeout = new CancellationTokenSource(Timeout);

        await foreach (var message in Broker.ConsumeAsync([queue], timeout.Token))
        {
            ids.Add(message.Message.Id);
            await Broker.AckAsync(message);

            if (ids.Count == count)
            {
                break;
            }
        }

        Assert.Equal(count, ids.Count);
    }

    [Fact]
    public async Task RejectWithRequeue_HonorsTheDeclaredEtaBehavior()
    {
        var queue = Unique("requeue-eta");
        var sent = CreateMessage(queue);
        await Broker.PublishAsync(sent);

        // The ETA is set on the returned copy so the first delivery is not delayed by a broker
        // with native delay
        var eta = DateTimeOffset.UtcNow.AddSeconds(2);
        using (var timeout = new CancellationTokenSource(Timeout))
        {
            await foreach (var message in Broker.ConsumeAsync([queue], timeout.Token))
            {
                var returned = new BrokerMessage
                {
                    Message = message.Message with { Eta = eta },
                    DeliveryTag = message.DeliveryTag,
                    Queue = message.Queue,
                    ReceivedAt = message.ReceivedAt,
                    RawBody = message.RawBody,
                    Signature = message.Signature,
                };
                await Broker.RejectAsync(returned, requeue: true);
                break;
            }
        }

        if (Capabilities.HoldsRequeuedMessagesUntilEta)
        {
            var early = await TryConsumeOneAsync(queue, TimeSpan.FromSeconds(1));
            Assert.Null(early);
        }

        var redelivered = await ConsumeAndAckAsync(queue);
        Assert.Equal(sent.Id, redelivered.Message.Id);

        if (Capabilities.HoldsRequeuedMessagesUntilEta)
        {
            Assert.True(DateTimeOffset.UtcNow >= eta);
        }
    }

    [Fact]
    public async Task CancelledSettlement_DoesNotSettleTheMessage()
    {
        var queue = Unique("settle-cancelled");
        var sent = CreateMessage(queue);
        await Broker.PublishAsync(sent);

        // Keep the consumer alive so its ownership is not the reason the settlement fails
        using var timeout = new CancellationTokenSource(Timeout);
        await using var consumer = Broker
            .ConsumeAsync([queue], timeout.Token)
            .GetAsyncEnumerator(CancellationToken.None);
        Assert.True(await consumer.MoveNextAsync());
        var received = consumer.Current;

        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            Broker.AckAsync(received, cancelled.Token).AsTask()
        );

        // The message was not settled, so stopping the consumer returns it and it is delivered
        // again, this time settled normally
        await consumer.DisposeAsync();

        var redelivered = await ConsumeAndAckAsync(queue);
        Assert.Equal(sent.Id, redelivered.Message.Id);
    }

    [Fact]
    public async Task SettlementAfterConsumerStops_IsRefused()
    {
        Assert.SkipWhen(
            !Capabilities.RejectsStaleSettlements,
            "The broker does not refuse a settlement from a consumer that no longer owns the delivery"
        );

        var queue = Unique("settle-stale");
        var sent = CreateMessage(queue);
        await Broker.PublishAsync(sent);

        var received = await ConsumeWithoutSettlingAsync(queue);

        await Assert.ThrowsAnyAsync<Exception>(() => Broker.AckAsync(received).AsTask());

        var redelivered = await ConsumeAndAckAsync(queue);
        Assert.Equal(sent.Id, redelivered.Message.Id);
    }

    [Fact]
    public async Task StaleSettlementAfterRedelivery_DoesNotSettleTheNewDelivery()
    {
        Assert.SkipWhen(
            !Capabilities.RejectsStaleSettlements,
            "Stale settlement rejection is unsupported"
        );
        var queue = Unique("settle-aba");
        await Broker.PublishAsync(CreateMessage(queue));
        var stale = await ConsumeWithoutSettlingAsync(queue);
        using var timeout = new CancellationTokenSource(Timeout);
        await using var consumer = Broker.ConsumeAsync([queue], timeout.Token).GetAsyncEnumerator();
        Assert.True(await consumer.MoveNextAsync());
        var current = consumer.Current;
        Assert.Equal(stale.Message.Id, current.Message.Id);
        await Assert.ThrowsAnyAsync<Exception>(() => Broker.AckAsync(stale).AsTask());
        await Assert.ThrowsAnyAsync<Exception>(() => Broker.RejectAsync(stale, false).AsTask());
        await Assert.ThrowsAnyAsync<Exception>(() => Broker.RejectAsync(stale, true).AsTask());
        // Settling the live receipt must still work, including with an equivalent message copy.
        await Broker.AckAsync(
            new BrokerMessage
            {
                Message = current.Message,
                DeliveryTag = current.DeliveryTag,
                Queue = current.Queue,
                ReceivedAt = current.ReceivedAt,
            }
        );
    }

    protected static TaskMessage CreateMessage(string queue) =>
        new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Task = "tests.contract",
            Args = "{\"value\":42}"u8.ToArray(),
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Queue = queue,
        };

    protected static string Unique(string prefix) => $"{prefix}-{Guid.NewGuid():N}";

    /// <summary>
    /// Consumes one message and acknowledges it before the consumer stops, as the worker does.
    /// </summary>
    protected Task<BrokerMessage> ConsumeAndAckAsync(string queue) =>
        ConsumeAndSettleAsync(queue, message => Broker.AckAsync(message).AsTask());

    /// <summary>
    /// Consumes one message and rejects it before the consumer stops.
    /// </summary>
    protected Task<BrokerMessage> ConsumeAndRejectAsync(string queue, bool requeue) =>
        ConsumeAndSettleAsync(queue, message => Broker.RejectAsync(message, requeue).AsTask());

    /// <summary>
    /// Consumes one message and stops the consumer without settling it, so the broker is left
    /// with an unsettled delivery.
    /// </summary>
    protected async Task<BrokerMessage> ConsumeWithoutSettlingAsync(string queue)
    {
        using var timeout = new CancellationTokenSource(Timeout);
        await using var consumer = Broker
            .ConsumeAsync([queue], timeout.Token)
            .GetAsyncEnumerator(CancellationToken.None);

        try
        {
            if (await consumer.MoveNextAsync())
            {
                return consumer.Current;
            }
        }
        catch (OperationCanceledException)
        {
            // Reported below, so the failure names the queue
        }

        throw new TimeoutException($"No message was delivered from queue '{queue}'.");
    }

    /// <summary>
    /// Consumes one message within a time budget, or returns <c>null</c> when none arrives.
    /// </summary>
    protected async Task<BrokerMessage?> TryConsumeOneAsync(string queue, TimeSpan wait)
    {
        using var timeout = new CancellationTokenSource(wait);
        await using var consumer = Broker
            .ConsumeAsync([queue], timeout.Token)
            .GetAsyncEnumerator(CancellationToken.None);

        try
        {
            return await consumer.MoveNextAsync() ? consumer.Current : null;
        }
        catch (OperationCanceledException)
        {
            return null;
        }
    }

    private async Task<BrokerMessage> ConsumeAndSettleAsync(
        string queue,
        Func<BrokerMessage, Task> settle
    )
    {
        using var timeout = new CancellationTokenSource(Timeout);

        await foreach (var message in Broker.ConsumeAsync([queue], timeout.Token))
        {
            await settle(message).ConfigureAwait(false);
            return message;
        }

        throw new TimeoutException($"No message was delivered from queue '{queue}'.");
    }

    private static void AssertMessageEquals(TaskMessage expected, TaskMessage actual)
    {
        Assert.Equal(expected.Id, actual.Id);
        Assert.Equal(expected.Task, actual.Task);
        Assert.Equal(expected.Args, actual.Args);
        Assert.Equal(expected.ContentType, actual.ContentType);
        Assert.Equal(expected.Queue, actual.Queue);

        // Brokers may round the timestamp to the second, as RabbitMQ's AMQP timestamp does
        Assert.Equal(expected.Timestamp.ToUnixTimeSeconds(), actual.Timestamp.ToUnixTimeSeconds());

        Assert.Equal(expected.Eta, actual.Eta);
        Assert.Equal(expected.Expires, actual.Expires);
        Assert.Equal(expected.Retries, actual.Retries);
        Assert.Equal(expected.MaxRetries, actual.MaxRetries);
        Assert.Equal(expected.ParentId, actual.ParentId);
        Assert.Equal(expected.RootId, actual.RootId);
        Assert.Equal(expected.BatchId, actual.BatchId);
        Assert.Equal(expected.CorrelationId, actual.CorrelationId);
        Assert.Equal(expected.Priority, actual.Priority);
        Assert.Equal(expected.PartitionKey, actual.PartitionKey);
        Assert.Equal(expected.TenantId, actual.TenantId);
        Assert.Equal(expected.SchemaVersion, actual.SchemaVersion);
        Assert.Equal(expected.Headers, actual.Headers);
        Assert.NotNull(actual.Chain);
        var step = Assert.Single(actual.Chain);
        Assert.Equal("step-1", step.TaskId);
        Assert.Equal("next.task", step.Signature.TaskName);
    }
}
