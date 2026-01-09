using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using Microsoft.Extensions.Options;

namespace DotCelery.Broker.InMemory;

/// <summary>
/// In-memory message broker for testing and development.
/// </summary>
public sealed class InMemoryBroker : IMessageBroker
{
    private readonly ConcurrentDictionary<string, Channel<BrokerMessage>> _queues = new();
    private readonly InMemoryBrokerOptions _options;
    private readonly Lock _lock = new();
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryBroker"/> class with default options.
    /// </summary>
    public InMemoryBroker()
        : this(Options.Create(new InMemoryBrokerOptions())) { }

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryBroker"/> class.
    /// </summary>
    /// <param name="options">The broker options.</param>
    public InMemoryBroker(IOptions<InMemoryBrokerOptions> options)
    {
        _options = options.Value;
    }

    /// <inheritdoc />
    public ValueTask PublishAsync(
        TaskMessage message,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var channel = GetOrCreateQueue(message.Queue);
        var brokerMessage = new BrokerMessage
        {
            Message = message,
            DeliveryTag = Guid.NewGuid(),
            Queue = message.Queue,
            ReceivedAt = DateTimeOffset.UtcNow,
        };

        return channel.Writer.WriteAsync(brokerMessage, cancellationToken);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<BrokerMessage> ConsumeAsync(
        IReadOnlyList<string> queues,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var channels = queues.Select(GetOrCreateQueue).ToList();

        while (!cancellationToken.IsCancellationRequested && !_disposed)
        {
            foreach (var channel in channels)
            {
                if (channel.Reader.TryRead(out var message))
                {
                    yield return message;
                }
            }

            // Small delay to prevent busy-waiting
            try
            {
                await Task.Delay(10, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                yield break;
            }
        }
    }

    /// <inheritdoc />
    public ValueTask AckAsync(BrokerMessage message, CancellationToken cancellationToken = default)
    {
        // In-memory broker doesn't need acknowledgments
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask RejectAsync(
        BrokerMessage message,
        bool requeue = false,
        CancellationToken cancellationToken = default
    )
    {
        if (requeue)
        {
            return PublishAsync(message.Message, cancellationToken);
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<bool> IsHealthyAsync(CancellationToken cancellationToken = default)
    {
        return ValueTask.FromResult(!_disposed);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return ValueTask.CompletedTask;
        }

        _disposed = true;

        foreach (var queue in _queues.Values)
        {
            queue.Writer.TryComplete();
        }

        _queues.Clear();

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Gets the number of messages in a queue.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <returns>The number of messages.</returns>
    public int GetQueueLength(string queueName)
    {
        if (_queues.TryGetValue(queueName, out var channel))
        {
            return channel.Reader.Count;
        }

        return 0;
    }

    /// <summary>
    /// Clears all messages from a queue.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    public void PurgeQueue(string queueName)
    {
        if (_queues.TryGetValue(queueName, out var channel))
        {
            while (channel.Reader.TryRead(out _))
            {
                // Discard messages
            }
        }
    }

    private Channel<BrokerMessage> GetOrCreateQueue(string name)
    {
        return _queues.GetOrAdd(name, CreateChannel);
    }

    private Channel<BrokerMessage> CreateChannel(string _)
    {
        if (_options.MaxQueueCapacity.HasValue)
        {
            return Channel.CreateBounded<BrokerMessage>(
                new BoundedChannelOptions(_options.MaxQueueCapacity.Value)
                {
                    SingleReader = false,
                    SingleWriter = false,
                    FullMode = _options.FullMode,
                }
            );
        }

        return Channel.CreateUnbounded<BrokerMessage>(
            new UnboundedChannelOptions { SingleReader = false, SingleWriter = false }
        );
    }
}
