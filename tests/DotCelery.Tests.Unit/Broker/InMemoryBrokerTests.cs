using DotCelery.Broker.InMemory;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;

namespace DotCelery.Tests.Unit.Broker;

public class InMemoryBrokerTests : IAsyncDisposable
{
    private readonly InMemoryBroker _broker = new();

    [Fact]
    public async Task PublishAsync_ValidMessage_Succeeds()
    {
        var message = CreateTestMessage();

        await _broker.PublishAsync(message);

        Assert.Equal(1, _broker.GetQueueLength("celery"));
    }

    [Fact]
    public async Task ConsumeAsync_AfterPublish_ReceivesMessage()
    {
        var message = CreateTestMessage();
        await _broker.PublishAsync(message);

        BrokerMessage? received = null;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));

        await foreach (var msg in _broker.ConsumeAsync(["celery"], cts.Token))
        {
            received = msg;
            break;
        }

        Assert.NotNull(received);
        Assert.Equal(message.Id, received.Message.Id);
        Assert.Equal(message.Task, received.Message.Task);
        Assert.Equal("celery", received.Queue);
    }

    [Fact]
    public async Task ConsumeAsync_MultipleQueues_ReceivesFromBoth()
    {
        var message1 = CreateTestMessage() with { Queue = "queue1" };
        var message2 = CreateTestMessage() with { Queue = "queue2" };

        await _broker.PublishAsync(message1);
        await _broker.PublishAsync(message2);

        var messages = new List<BrokerMessage>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));

        await foreach (var msg in _broker.ConsumeAsync(["queue1", "queue2"], cts.Token))
        {
            messages.Add(msg);
            if (messages.Count >= 2)
                break;
        }

        Assert.Equal(2, messages.Count);
    }

    [Fact]
    public async Task ConsumeAsync_EmptyQueue_TimesOut()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        var messages = new List<BrokerMessage>();
        await foreach (var msg in _broker.ConsumeAsync(["empty-queue"], cts.Token))
        {
            messages.Add(msg);
        }

        Assert.Empty(messages);
    }

    [Fact]
    public async Task AckAsync_ValidMessage_Succeeds()
    {
        var message = CreateTestMessage();
        await _broker.PublishAsync(message);

        BrokerMessage? received = null;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));

        await foreach (var msg in _broker.ConsumeAsync(["celery"], cts.Token))
        {
            received = msg;
            break;
        }

        Assert.NotNull(received);

        // Ack should not throw
        await _broker.AckAsync(received);
    }

    [Fact]
    public async Task RejectAsync_WithRequeue_RepublishesMessage()
    {
        var message = CreateTestMessage();
        await _broker.PublishAsync(message);

        // Consume the message
        BrokerMessage? received = null;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        await foreach (var msg in _broker.ConsumeAsync(["celery"], cts.Token))
        {
            received = msg;
            break;
        }

        Assert.NotNull(received);

        // Reject with requeue
        await _broker.RejectAsync(received, requeue: true);

        // Should be able to consume again
        Assert.Equal(1, _broker.GetQueueLength("celery"));
    }

    [Fact]
    public async Task RejectAsync_WithoutRequeue_DoesNotRepublish()
    {
        var message = CreateTestMessage();
        await _broker.PublishAsync(message);

        // Consume the message
        BrokerMessage? received = null;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        await foreach (var msg in _broker.ConsumeAsync(["celery"], cts.Token))
        {
            received = msg;
            break;
        }

        Assert.NotNull(received);

        // Reject without requeue
        await _broker.RejectAsync(received, requeue: false);

        Assert.Equal(0, _broker.GetQueueLength("celery"));
    }

    [Fact]
    public async Task IsHealthyAsync_NotDisposed_ReturnsTrue()
    {
        var healthy = await _broker.IsHealthyAsync();

        Assert.True(healthy);
    }

    [Fact]
    public async Task IsHealthyAsync_AfterDispose_ReturnsFalse()
    {
        await _broker.DisposeAsync();

        var healthy = await _broker.IsHealthyAsync();

        Assert.False(healthy);
    }

    [Fact]
    public async Task DisposeAsync_CanBeCalledMultipleTimes()
    {
        await _broker.DisposeAsync();
        await _broker.DisposeAsync(); // Should not throw
    }

    [Fact]
    public void GetQueueLength_NonExistentQueue_ReturnsZero()
    {
        var length = _broker.GetQueueLength("non-existent");

        Assert.Equal(0, length);
    }

    [Fact]
    public async Task PurgeQueue_ClearsAllMessages()
    {
        await _broker.PublishAsync(CreateTestMessage());
        await _broker.PublishAsync(CreateTestMessage());
        await _broker.PublishAsync(CreateTestMessage());

        Assert.Equal(3, _broker.GetQueueLength("celery"));

        _broker.PurgeQueue("celery");

        Assert.Equal(0, _broker.GetQueueLength("celery"));
    }

    public async ValueTask DisposeAsync()
    {
        await _broker.DisposeAsync();
    }

    private static TaskMessage CreateTestMessage() =>
        new()
        {
            Id = Guid.NewGuid().ToString(),
            Task = "test.task",
            Args = "{}"u8.ToArray(),
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Queue = "celery",
        };
}
