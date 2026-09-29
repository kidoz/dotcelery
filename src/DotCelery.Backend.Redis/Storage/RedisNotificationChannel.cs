using System.Runtime.CompilerServices;
using DotCelery.Core.Storage;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.Storage;

/// <summary>
/// Redis <see cref="INotificationChannel"/> on pub/sub.
/// </summary>
internal sealed class RedisNotificationChannel(RedisContext context) : INotificationChannel
{
    public async ValueTask PublishAsync(
        string channel,
        string message,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(channel);
        ArgumentNullException.ThrowIfNull(message);

        var subscriber = await context.GetSubscriberAsync(cancellationToken).ConfigureAwait(false);
        await subscriber
            .PublishAsync(RedisChannel.Literal(context.Keys.Channel(channel)), message)
            .ConfigureAwait(false);
    }

    public async IAsyncEnumerable<string> SubscribeAsync(
        string channel,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(channel);

        var subscriber = await context.GetSubscriberAsync(cancellationToken).ConfigureAwait(false);
        var messages = await subscriber
            .SubscribeAsync(RedisChannel.Literal(context.Keys.Channel(channel)))
            .ConfigureAwait(false);

        try
        {
            while (true)
            {
                var message = await messages.ReadAsync(cancellationToken).ConfigureAwait(false);
                yield return message.Message.ToString();
            }
        }
        finally
        {
            await messages.UnsubscribeAsync().ConfigureAwait(false);
        }
    }
}
