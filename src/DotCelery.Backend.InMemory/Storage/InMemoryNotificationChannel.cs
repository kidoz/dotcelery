using System.Runtime.CompilerServices;
using System.Threading.Channels;
using DotCelery.Core.Storage;

namespace DotCelery.Backend.InMemory.Storage;

/// <summary>
/// In-memory <see cref="INotificationChannel"/>.
/// </summary>
internal sealed class InMemoryNotificationChannel : INotificationChannel
{
    private readonly Dictionary<string, List<Channel<string>>> _subscribers = new(
        StringComparer.Ordinal
    );
    private readonly Lock _lock = new();

    public ValueTask PublishAsync(
        string channel,
        string message,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(channel);
        ArgumentNullException.ThrowIfNull(message);

        lock (_lock)
        {
            if (_subscribers.TryGetValue(channel, out var subscribers))
            {
                foreach (var subscriber in subscribers)
                {
                    subscriber.Writer.TryWrite(message);
                }
            }
        }

        return ValueTask.CompletedTask;
    }

    public async IAsyncEnumerable<string> SubscribeAsync(
        string channel,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(channel);

        var subscriber = Channel.CreateUnbounded<string>(
            new UnboundedChannelOptions { SingleReader = true }
        );

        lock (_lock)
        {
            if (!_subscribers.TryGetValue(channel, out var subscribers))
            {
                subscribers = [];
                _subscribers[channel] = subscribers;
            }

            subscribers.Add(subscriber);
        }

        try
        {
            await foreach (
                var message in subscriber
                    .Reader.ReadAllAsync(cancellationToken)
                    .ConfigureAwait(false)
            )
            {
                yield return message;
            }
        }
        finally
        {
            lock (_lock)
            {
                if (_subscribers.TryGetValue(channel, out var subscribers))
                {
                    subscribers.Remove(subscriber);
                    if (subscribers.Count == 0)
                    {
                        _subscribers.Remove(channel);
                    }
                }
            }
        }
    }
}
