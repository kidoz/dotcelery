namespace DotCelery.Core.Storage;

/// <summary>
/// Publishes short messages to subscribers, for example to wake up waiters when a result is stored.
/// </summary>
/// <remarks>
/// Delivery is best effort: messages published while no subscriber is connected, or during a
/// reconnect, are lost. Callers must not rely on notifications alone and should also poll.
/// </remarks>
public interface INotificationChannel
{
    /// <summary>
    /// Publishes a message to the current subscribers of a channel.
    /// </summary>
    /// <param name="channel">The channel name.</param>
    /// <param name="message">The message; keep it short, such as an id.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the message is published.</returns>
    ValueTask PublishAsync(
        string channel,
        string message,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Receives messages published to a channel after the subscription starts, until cancelled.
    /// </summary>
    /// <param name="channel">The channel name.</param>
    /// <param name="cancellationToken">Cancels the subscription.</param>
    /// <returns>The published messages.</returns>
    IAsyncEnumerable<string> SubscribeAsync(
        string channel,
        CancellationToken cancellationToken = default
    );
}
