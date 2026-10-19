using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Security;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Options;

namespace DotCelery.Broker.InMemory;

/// <summary>
/// In-memory message broker for testing and development.
/// </summary>
/// <remarks>
/// The broker models what the other brokers do, so tests that run against it exercise realistic
/// behavior: a delivered message is what its serialized payload says, an unsettled message whose
/// consumer stops is delivered again, and an acknowledged or rejected message is settled. It
/// delivers in publish order and holds no message until its ETA, so it declares no priority or
/// native-delay capability.
/// </remarks>
public sealed class InMemoryBroker : IMessageBroker, IBrokerCapabilities
{
    private readonly ConcurrentDictionary<string, Channel<BrokerMessage>> _queues = new();
    private readonly InMemoryBrokerOptions _options;
    private readonly IMessageSecurityValidator? _messageSecurityValidator;
    private readonly IQueueMetrics? _queueMetrics;
    private readonly Lock _lock = new();

    // A delivered message that has not been settled, and the consumer it was delivered to
    private readonly ConcurrentDictionary<BrokerMessage, ConsumerDeliveries> _deliveries = new();

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
    /// <param name="messageSecurityValidator">Optional message security validator.</param>
    /// <param name="queueMetrics">Optional queue metrics store the publishes are recorded in.</param>
    public InMemoryBroker(
        IOptions<InMemoryBrokerOptions> options,
        IMessageSecurityValidator? messageSecurityValidator = null,
        IQueueMetrics? queueMetrics = null
    )
    {
        _options = options.Value;
        _messageSecurityValidator = messageSecurityValidator;
        _queueMetrics = queueMetrics;
    }

    /// <inheritdoc />
    public async ValueTask PublishAsync(
        TaskMessage message,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnqueueAsync(message, cancellationToken).ConfigureAwait(false);

        // The message is in the queue, so the queue's waiting count follows what was published
        await _queueMetrics
            .RecordEnqueueAsync(message.Queue, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    // Enqueues without recording an enqueue: requeues go through here so a returned message is
    // not counted as a fresh enqueue
    private async ValueTask EnqueueAsync(TaskMessage message, CancellationToken cancellationToken)
    {
        var channel = GetOrCreateQueue(message.Queue);
        var rawBody = JsonSerializer.SerializeToUtf8Bytes(
            message,
            DotCeleryJsonContext.Default.TaskMessage
        );
        var brokerMessage = new BrokerMessage
        {
            Message = message,
            DeliveryTag = Guid.NewGuid(),
            Queue = message.Queue,
            ReceivedAt = DateTimeOffset.UtcNow,
            RawBody = rawBody,
            Signature = _messageSecurityValidator?.Sign(rawBody),
        };

        await channel.Writer.WriteAsync(brokerMessage, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<BrokerMessage> ConsumeAsync(
        IReadOnlyList<string> queues,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var channels = queues.Select(GetOrCreateQueue).ToList();
        var pending = new ConsumerDeliveries();

        try
        {
            while (!cancellationToken.IsCancellationRequested && !_disposed)
            {
                foreach (var channel in channels)
                {
                    if (channel.Reader.TryRead(out var message))
                    {
                        var delivered = RoundTrip(message);
                        _deliveries[delivered] = pending;
                        pending.Add(delivered);
                        yield return delivered;
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
        finally
        {
            // A consumer that stops without settling its messages gives them back, as a broker
            // redelivers them
            foreach (var message in pending.Drain())
            {
                _deliveries.TryRemove(message, out _);
                Requeue(message);
            }
        }
    }

    /// <inheritdoc />
    public ValueTask AckAsync(BrokerMessage message, CancellationToken cancellationToken = default)
    {
        Settle(message);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask RejectAsync(
        BrokerMessage message,
        bool requeue = false,
        CancellationToken cancellationToken = default
    )
    {
        Settle(message);

        if (requeue)
        {
            return EnqueueAsync(message.Message, cancellationToken);
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<bool> IsHealthyAsync(CancellationToken cancellationToken = default)
    {
        return ValueTask.FromResult(!_disposed);
    }

    /// <inheritdoc />
    public MessageBrokerCapabilities Capabilities { get; } =
        new() { RedeliveryOnConsumerStop = true };

    // The delivered message is what its serialized payload says, as with a broker that sends
    // bytes over the wire, so a message that does not survive serialization fails here instead
    // of only in production
    private static BrokerMessage RoundTrip(BrokerMessage message)
    {
        var body =
            message.RawBody
            ?? JsonSerializer.SerializeToUtf8Bytes(
                message.Message,
                DotCeleryJsonContext.Default.TaskMessage
            );

        return new BrokerMessage
        {
            Message = JsonSerializer.Deserialize(body, DotCeleryJsonContext.Default.TaskMessage)!,
            DeliveryTag = message.DeliveryTag,
            Queue = message.Queue,
            ReceivedAt = DateTimeOffset.UtcNow,
            RawBody = body,
            Signature = message.Signature,
        };
    }

    // Marks a delivered message settled, so stopping its consumer does not deliver it again.
    // Settling a message that was not delivered (or was already settled) does nothing.
    private void Settle(BrokerMessage message)
    {
        if (_deliveries.TryRemove(message, out var pending))
        {
            pending.Remove(message);
        }
    }

    // Puts an unsettled message back on its queue for redelivery
    private void Requeue(BrokerMessage message)
    {
        if (_disposed || _queues.TryGetValue(message.Message.Queue, out var channel) is false)
        {
            return;
        }

        var rawBody = message.RawBody;
        if (rawBody is null)
        {
            return;
        }

        channel.Writer.TryWrite(
            new BrokerMessage
            {
                Message = message.Message,
                DeliveryTag = Guid.NewGuid(),
                Queue = message.Queue,
                ReceivedAt = DateTimeOffset.UtcNow,
                RawBody = rawBody,
                Signature = _messageSecurityValidator?.Sign(rawBody),
            }
        );
    }

    // The messages one consumer was given and has not settled
    private sealed class ConsumerDeliveries
    {
        private readonly Lock _lock = new();
        private readonly List<BrokerMessage> _messages = [];

        public void Add(BrokerMessage message)
        {
            lock (_lock)
            {
                _messages.Add(message);
            }
        }

        public void Remove(BrokerMessage message)
        {
            lock (_lock)
            {
                _messages.Remove(message);
            }
        }

        public List<BrokerMessage> Drain()
        {
            lock (_lock)
            {
                var drained = new List<BrokerMessage>(_messages);
                _messages.Clear();
                return drained;
            }
        }
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
