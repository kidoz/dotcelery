using DotCelery.Broker.RabbitMQ;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using NSubstitute;
using RabbitMQ.Client;

namespace DotCelery.Tests.Unit.Broker;

/// <summary>
/// A message is settled only on the channel that delivered it: delivery tags are channel-local,
/// so acknowledging on another channel would settle a different message.
/// </summary>
public sealed class ConsumerChannelTests
{
    private readonly IChannel _rabbitChannel = Substitute.For<IChannel>();
    private readonly ConsumerChannel _channel;

    public ConsumerChannelTests()
    {
        _rabbitChannel.IsOpen.Returns(true);
        _channel = new ConsumerChannel(_rabbitChannel);
    }

    [Fact]
    public async Task AckAsync_SettlesOnTheDeliveringChannel()
    {
        var message = CreateMessage();
        _channel.Track(12, message);

        await _channel.AckAsync(12);

        await _rabbitChannel.Received(1).BasicAckAsync(12, false, Arg.Any<CancellationToken>());
        Assert.Empty(_channel.InFlight);
    }

    [Fact]
    public async Task RejectAsync_SettlesOnTheDeliveringChannel()
    {
        var message = CreateMessage();
        _channel.Track(12, message);

        await _channel.RejectAsync(12, requeue: true);

        await _rabbitChannel.Received(1).BasicRejectAsync(12, true, Arg.Any<CancellationToken>());
        Assert.Empty(_channel.InFlight);
    }

    [Fact]
    public async Task AckAsync_WhenTheChannelIsClosed_IsRefusedWithoutTouchingAnotherChannel()
    {
        var message = CreateMessage();
        _channel.Track(12, message);
        _rabbitChannel.IsOpen.Returns(false);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await _channel.AckAsync(12)
        );

        Assert.Contains("closed", exception.Message, StringComparison.Ordinal);
        await _rabbitChannel
            .DidNotReceive()
            .BasicAckAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task AckAsync_UnknownDelivery_IsRefused()
    {
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await _channel.AckAsync(99)
        );

        Assert.Contains("not in flight", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AckAsync_WhenAlreadySettled_IsRefused()
    {
        var message = CreateMessage();
        _channel.Track(12, message);
        await _channel.AckAsync(12);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await _channel.AckAsync(12)
        );
    }

    [Fact]
    public void ForgetInFlight_ReturnsTheMessagesSoTheyAreNotSettledLater()
    {
        var first = CreateMessage();
        var second = CreateMessage();
        _channel.Track(12, first);
        _channel.Track(13, second);

        var forgotten = _channel.ForgetInFlight();

        Assert.Equal([first, second], forgotten);
        Assert.Empty(_channel.InFlight);
    }

    [Fact]
    public async Task ConcurrentAckAndReject_AreSerializedOnTheChannel()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
#pragma warning disable CA2012 // NSubstitute consumes the configured ValueTask.
        _rabbitChannel
            .BasicAckAsync(12, false, Arg.Any<CancellationToken>())
            .Returns(_ => new ValueTask(BlockAckAsync()));
#pragma warning restore CA2012
        async Task BlockAckAsync()
        {
            entered.TrySetResult();
            await release.Task;
        }
        _channel.Track(12, CreateMessage());
        _channel.Track(13, CreateMessage());
        var ack = _channel.AckAsync(12).AsTask();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var reject = _channel.RejectAsync(13, false).AsTask();
        try
        {
            Assert.False(reject.IsCompleted);
            await _rabbitChannel
                .DidNotReceive()
                .BasicRejectAsync(13, false, Arg.Any<CancellationToken>());
        }
        finally
        {
            release.TrySetResult();
            await Task.WhenAll(ack, reject).WaitAsync(TimeSpan.FromSeconds(5));
        }
        Assert.Empty(_channel.InFlight);
    }

    [Fact]
    public async Task FailedAck_CanBeRetriedWithoutLosingTheDelivery()
    {
        var attempts = 0;
#pragma warning disable CA2012 // NSubstitute consumes the configured ValueTask.
        _rabbitChannel
            .BasicAckAsync(12, false, Arg.Any<CancellationToken>())
            .Returns(_ =>
                ++attempts == 1
                    ? ValueTask.FromException(new IOException("transient"))
                    : ValueTask.CompletedTask
            );
#pragma warning restore CA2012
        _channel.Track(12, CreateMessage());
        await Assert.ThrowsAsync<IOException>(() => _channel.AckAsync(12).AsTask());
        Assert.Single(_channel.InFlight);
        await _channel.AckAsync(12);
        Assert.Empty(_channel.InFlight);
    }

    private static BrokerMessage CreateMessage() =>
        new()
        {
            Message = new TaskMessage
            {
                Id = Guid.NewGuid().ToString("N"),
                Task = "tests.task",
                Args = [],
                ContentType = "application/json",
                Timestamp = DateTimeOffset.UtcNow,
            },
            DeliveryTag = 12UL,
            Queue = "celery",
            ReceivedAt = DateTimeOffset.UtcNow,
        };
}
