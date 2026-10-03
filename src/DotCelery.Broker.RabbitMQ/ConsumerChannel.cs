using System.Collections.Concurrent;
using DotCelery.Core.Abstractions;
using RabbitMQ.Client;

namespace DotCelery.Broker.RabbitMQ;

/// <summary>
/// The channel one consume loop receives messages on, with the messages it delivered and that
/// are not settled yet.
/// </summary>
/// <remarks>
/// Delivery tags belong to the channel that issued them, so a message is acknowledged or
/// rejected only on its own channel; the same tag on another channel means another message. The
/// channel lives as long as the connection of the consume loop. When it is lost, the broker
/// requeues what it had delivered and the consumer rebuilds the channel, whose tags start
/// again, so a tracked delivery is refused instead of settled.
/// </remarks>
internal sealed class ConsumerChannel(IChannel channel)
{
    private readonly ConcurrentDictionary<ulong, BrokerMessage> _inFlight = new();

    /// <summary>
    /// Gets the channel.
    /// </summary>
    public IChannel Channel { get; } = channel;

    /// <summary>
    /// Gets the messages delivered on this channel and not settled yet.
    /// </summary>
    public IReadOnlyCollection<BrokerMessage> InFlight => _inFlight.Values.ToArray();

    /// <summary>
    /// Remembers a delivery so that it can be settled on this channel.
    /// </summary>
    /// <param name="deliveryTag">The delivery tag the channel issued.</param>
    /// <param name="message">The message.</param>
    public void Track(ulong deliveryTag, BrokerMessage message) => _inFlight[deliveryTag] = message;

    /// <summary>
    /// Acknowledges a message on the channel that delivered it.
    /// </summary>
    /// <param name="deliveryTag">The delivery tag the channel issued.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="InvalidOperationException">
    /// The delivery is not in flight on this channel, or the channel that delivered it is gone,
    /// so the message cannot be settled here.
    /// </exception>
    public async ValueTask AckAsync(
        ulong deliveryTag,
        CancellationToken cancellationToken = default
    )
    {
        RequireLiveDelivery(deliveryTag);
        await Channel
            .BasicAckAsync(deliveryTag, multiple: false, cancellationToken)
            .ConfigureAwait(false);
        _inFlight.TryRemove(deliveryTag, out _);
    }

    /// <summary>
    /// Rejects a message on the channel that delivered it.
    /// </summary>
    /// <param name="deliveryTag">The delivery tag the channel issued.</param>
    /// <param name="requeue">Whether the broker should deliver the message again.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="InvalidOperationException">
    /// The delivery is not in flight on this channel, or the channel that delivered it is gone,
    /// so the message cannot be settled here.
    /// </exception>
    public async ValueTask RejectAsync(
        ulong deliveryTag,
        bool requeue,
        CancellationToken cancellationToken = default
    )
    {
        RequireLiveDelivery(deliveryTag);
        await Channel
            .BasicRejectAsync(deliveryTag, requeue, cancellationToken)
            .ConfigureAwait(false);
        _inFlight.TryRemove(deliveryTag, out _);
    }

    /// <summary>
    /// Forgets every tracked delivery, because the channel is gone and the broker requeued what
    /// it had delivered.
    /// </summary>
    /// <returns>The forgotten messages.</returns>
    public IReadOnlyList<BrokerMessage> ForgetInFlight()
    {
        var messages = _inFlight.Values.ToArray();
        _inFlight.Clear();
        return messages;
    }

    private void RequireLiveDelivery(ulong deliveryTag)
    {
        if (!_inFlight.TryGetValue(deliveryTag, out var message))
        {
            throw new InvalidOperationException(
                $"Delivery {deliveryTag} is not in flight on this channel: it was settled already, "
                    + "or the channel was lost and the broker requeued the message."
            );
        }

        if (!Channel.IsOpen)
        {
            throw new InvalidOperationException(
                $"The channel that delivered message {message.Message.Id} is closed; the message "
                    + "is requeued and will be redelivered."
            );
        }
    }
}
