using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Channels;
using DotCelery.Core.Abstractions;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace DotCelery.Broker.RabbitMQ;

/// <summary>
/// RabbitMQ message broker implementation.
/// </summary>
public sealed class RabbitMQBroker : IMessageBroker
{
    private readonly RabbitMQBrokerOptions _options;
    private readonly ILogger<RabbitMQBroker> _logger;
    private readonly IDeadLetterStore? _deadLetterStore;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly SemaphoreSlim _publishChannelLock = new(1, 1);
    private readonly SemaphoreSlim _consumeChannelLock = new(1, 1);
    private readonly ConcurrentDictionary<ulong, BrokerMessage> _unackedMessages = new();

    // AOT-friendly type info for TaskMessage serialization
    private static JsonTypeInfo<TaskMessage> TaskMessageTypeInfo =>
        DotCeleryJsonContext.Default.TaskMessage;

    private IConnection? _connection;
    private IChannel? _publishChannel;
    private IChannel? _consumeChannel;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMQBroker"/> class.
    /// </summary>
    /// <param name="options">The broker options.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="deadLetterStore">Optional dead letter store for deserialization failures.</param>
    public RabbitMQBroker(
        IOptions<RabbitMQBrokerOptions> options,
        ILogger<RabbitMQBroker> logger,
        IDeadLetterStore? deadLetterStore = null
    )
    {
        _options = options.Value;
        _logger = logger;
        _deadLetterStore = deadLetterStore;
    }

    /// <inheritdoc />
    public async ValueTask PublishAsync(
        TaskMessage message,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(message);

        var channel = await GetPublishChannelAsync(cancellationToken).ConfigureAwait(false);

        if (_options.AutoDeclareQueues)
        {
            await EnsureQueueDeclaredAsync(channel, message.Queue, cancellationToken)
                .ConfigureAwait(false);
        }

        // Serialize using AOT-friendly type info
        var body = JsonSerializer.SerializeToUtf8Bytes(message, TaskMessageTypeInfo);

        // Validate message size
        if (_options.MaxMessageSizeBytes > 0 && body.Length > _options.MaxMessageSizeBytes)
        {
            throw new InvalidOperationException(
                $"Message size ({body.Length} bytes) exceeds maximum allowed size ({_options.MaxMessageSizeBytes} bytes)."
            );
        }

        var properties = new BasicProperties
        {
            Persistent = true,
            MessageId = message.Id,
            Type = message.Task,
            ContentType = "application/json",
            Timestamp = new AmqpTimestamp(message.Timestamp.ToUnixTimeSeconds()),
            Priority = (byte)Math.Clamp(message.Priority, 0, 9),
            CorrelationId = message.CorrelationId,
        };

        if (message.Expires.HasValue)
        {
            var ttl = message.Expires.Value - DateTimeOffset.UtcNow;
            if (ttl > TimeSpan.Zero)
            {
                properties.Expiration = ((long)ttl.TotalMilliseconds).ToString(
                    System.Globalization.CultureInfo.InvariantCulture
                );
            }
        }

        await channel
            .BasicPublishAsync(
                exchange: _options.Exchange,
                routingKey: message.Queue,
                mandatory: false,
                basicProperties: properties,
                body: body,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);

        _logger.LogDebug(
            "Published message {MessageId} to queue {Queue}",
            message.Id,
            message.Queue
        );
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<BrokerMessage> ConsumeAsync(
        IReadOnlyList<string> queues,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(queues);

        var channel = await GetConsumeChannelAsync(cancellationToken).ConfigureAwait(false);

        // Set prefetch
        await channel
            .BasicQosAsync(
                prefetchSize: 0,
                prefetchCount: _options.PrefetchCount,
                global: false,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);

        // Ensure queues exist
        if (_options.AutoDeclareQueues)
        {
            foreach (var queue in queues)
            {
                await EnsureQueueDeclaredAsync(channel, queue, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        // Create a channel to buffer messages
        var messageChannel = Channel.CreateBounded<BrokerMessage>(
            new BoundedChannelOptions(100)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false,
            }
        );

        // Set up consumers for each queue
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            try
            {
                // Deserialize using AOT-friendly type info
                TaskMessage? taskMessage;
                try
                {
                    taskMessage = JsonSerializer.Deserialize(ea.Body.Span, TaskMessageTypeInfo);
                }
                catch (JsonException jsonEx)
                {
                    await HandleDeserializationFailureAsync(ea, channel, jsonEx, cancellationToken)
                        .ConfigureAwait(false);
                    return;
                }

                if (taskMessage is null)
                {
                    await HandleDeserializationFailureAsync(ea, channel, null, cancellationToken)
                        .ConfigureAwait(false);
                    return;
                }

                var brokerMessage = new BrokerMessage
                {
                    Message = taskMessage,
                    DeliveryTag = ea.DeliveryTag,
                    Queue = ea.RoutingKey,
                    ReceivedAt = DateTimeOffset.UtcNow,
                };

                _unackedMessages[ea.DeliveryTag] = brokerMessage;

                await messageChannel
                    .Writer.WriteAsync(brokerMessage, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Error processing received message {DeliveryTag}",
                    ea.DeliveryTag
                );

                // Reject without requeue to prevent infinite loops
                try
                {
                    await channel
                        .BasicRejectAsync(ea.DeliveryTag, requeue: false, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (Exception rejectEx)
                {
                    _logger.LogError(
                        rejectEx,
                        "Failed to reject message {DeliveryTag}",
                        ea.DeliveryTag
                    );
                }
            }
        };

        // Start consuming from all queues
        var consumerTags = new List<string>();
        foreach (var queue in queues)
        {
            var tag = await channel
                .BasicConsumeAsync(
                    queue: queue,
                    autoAck: false,
                    consumer: consumer,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
            consumerTags.Add(tag);
            _logger.LogInformation(
                "Started consuming from queue {Queue} with tag {ConsumerTag}",
                queue,
                tag
            );
        }

        try
        {
            await foreach (
                var message in messageChannel
                    .Reader.ReadAllAsync(cancellationToken)
                    .ConfigureAwait(false)
            )
            {
                yield return message;
            }
        }
        finally
        {
            // Cancel consumers on cleanup
            foreach (var tag in consumerTags)
            {
                try
                {
                    await channel
                        .BasicCancelAsync(
                            tag,
                            noWait: false,
                            cancellationToken: CancellationToken.None
                        )
                        .ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error cancelling consumer {ConsumerTag}", tag);
                }
            }

            // Reject any unacknowledged messages with requeue so they can be
            // consumed by other consumers
            foreach (var kvp in _unackedMessages)
            {
                try
                {
                    await channel
                        .BasicRejectAsync(kvp.Key, requeue: true, CancellationToken.None)
                        .ConfigureAwait(false);
                    _unackedMessages.TryRemove(kvp.Key, out _);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(
                        ex,
                        "Error rejecting unacknowledged message {DeliveryTag}",
                        kvp.Key
                    );
                }
            }

            messageChannel.Writer.Complete();
        }
    }

    /// <inheritdoc />
    public async ValueTask AckAsync(
        BrokerMessage message,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(message);

        if (message.DeliveryTag is not ulong deliveryTag)
        {
            throw new ArgumentException("Invalid delivery tag", nameof(message));
        }

        var channel = await GetConsumeChannelAsync(cancellationToken).ConfigureAwait(false);
        await channel
            .BasicAckAsync(deliveryTag, multiple: false, cancellationToken)
            .ConfigureAwait(false);
        _unackedMessages.TryRemove(deliveryTag, out _);

        _logger.LogDebug("Acknowledged message {MessageId}", message.Message.Id);
    }

    /// <inheritdoc />
    public async ValueTask RejectAsync(
        BrokerMessage message,
        bool requeue = false,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(message);

        if (message.DeliveryTag is not ulong deliveryTag)
        {
            throw new ArgumentException("Invalid delivery tag", nameof(message));
        }

        var channel = await GetConsumeChannelAsync(cancellationToken).ConfigureAwait(false);
        await channel
            .BasicRejectAsync(deliveryTag, requeue: requeue, cancellationToken)
            .ConfigureAwait(false);
        _unackedMessages.TryRemove(deliveryTag, out _);

        _logger.LogDebug(
            "Rejected message {MessageId} (requeue: {Requeue})",
            message.Message.Id,
            requeue
        );
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsHealthyAsync(CancellationToken cancellationToken = default)
    {
        if (_disposed)
        {
            return false;
        }

        try
        {
            var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
            return connection.IsOpen;
        }
        catch
        {
            return false;
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        try
        {
            if (_publishChannel is not null)
            {
                await _publishChannel.CloseAsync().ConfigureAwait(false);
                _publishChannel.Dispose();
            }

            if (_consumeChannel is not null)
            {
                await _consumeChannel.CloseAsync().ConfigureAwait(false);
                _consumeChannel.Dispose();
            }

            if (_connection is not null)
            {
                await _connection.CloseAsync().ConfigureAwait(false);
                _connection.Dispose();
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error during RabbitMQ broker disposal");
        }

        _connectionLock.Dispose();
        _publishChannelLock.Dispose();
        _consumeChannelLock.Dispose();
        _unackedMessages.Clear();

        _logger.LogInformation("RabbitMQ broker disposed");
    }

    private async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken)
    {
        if (_connection?.IsOpen == true)
        {
            return _connection;
        }

        await _connectionLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_connection?.IsOpen == true)
            {
                return _connection;
            }

            var factory = new ConnectionFactory
            {
                Uri = new Uri(_options.ConnectionString),
                ClientProvidedName = _options.ConnectionName,
                RequestedHeartbeat = _options.Heartbeat,
            };

            for (var attempt = 1; attempt <= _options.ConnectionRetryCount; attempt++)
            {
                try
                {
                    _connection = await factory
                        .CreateConnectionAsync(cancellationToken)
                        .ConfigureAwait(false);
                    _logger.LogInformation("Connected to RabbitMQ");
                    return _connection;
                }
                catch (Exception ex) when (attempt < _options.ConnectionRetryCount)
                {
                    _logger.LogWarning(
                        ex,
                        "Failed to connect to RabbitMQ (attempt {Attempt}/{MaxAttempts})",
                        attempt,
                        _options.ConnectionRetryCount
                    );
                    await Task.Delay(_options.ConnectionRetryDelay, cancellationToken)
                        .ConfigureAwait(false);
                }
            }

            // Final attempt - let exception propagate
            _connection = await factory
                .CreateConnectionAsync(cancellationToken)
                .ConfigureAwait(false);
            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private async Task<IChannel> GetPublishChannelAsync(CancellationToken cancellationToken)
    {
        if (_publishChannel?.IsOpen == true)
        {
            return _publishChannel;
        }

        await _publishChannelLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_publishChannel?.IsOpen == true)
            {
                return _publishChannel;
            }

            var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
            _publishChannel = await connection
                .CreateChannelAsync(cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            return _publishChannel;
        }
        finally
        {
            _publishChannelLock.Release();
        }
    }

    private async Task<IChannel> GetConsumeChannelAsync(CancellationToken cancellationToken)
    {
        if (_consumeChannel?.IsOpen == true)
        {
            return _consumeChannel;
        }

        await _consumeChannelLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_consumeChannel?.IsOpen == true)
            {
                return _consumeChannel;
            }

            var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
            _consumeChannel = await connection
                .CreateChannelAsync(cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            return _consumeChannel;
        }
        finally
        {
            _consumeChannelLock.Release();
        }
    }

    private async Task EnsureQueueDeclaredAsync(
        IChannel channel,
        string queue,
        CancellationToken cancellationToken
    )
    {
        await channel
            .QueueDeclareAsync(
                queue: queue,
                durable: _options.DurableQueues,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object?>
                {
                    ["x-max-priority"] = 10, // Enable priority queues
                },
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);
    }

    private async Task HandleDeserializationFailureAsync(
        BasicDeliverEventArgs ea,
        IChannel channel,
        JsonException? exception,
        CancellationToken cancellationToken
    )
    {
        _logger.LogError(
            exception,
            "Failed to deserialize message {DeliveryTag} from queue {Queue}",
            ea.DeliveryTag,
            ea.RoutingKey
        );

        // Store in dead letter queue if available
        if (_deadLetterStore is not null)
        {
            try
            {
                var deadLetterMessage = new DeadLetterMessage
                {
                    Id = Guid.NewGuid().ToString(),
                    TaskId = ea.BasicProperties?.MessageId ?? "unknown",
                    TaskName = ea.BasicProperties?.Type ?? "unknown",
                    Queue = ea.RoutingKey,
                    Reason = DeadLetterReason.DeserializationFailed,
                    OriginalMessage = ea.Body.ToArray(),
                    ExceptionMessage = exception?.Message,
                    ExceptionType = exception?.GetType().FullName,
                    StackTrace = exception?.StackTrace,
                    Timestamp = DateTimeOffset.UtcNow,
                };

                await _deadLetterStore
                    .StoreAsync(deadLetterMessage, cancellationToken)
                    .ConfigureAwait(false);

                _logger.LogInformation(
                    "Stored deserialization failure in DLQ: {MessageId}",
                    deadLetterMessage.Id
                );
            }
            catch (Exception dlqEx)
            {
                _logger.LogError(
                    dlqEx,
                    "Failed to store message {DeliveryTag} in dead letter queue",
                    ea.DeliveryTag
                );
            }
        }

        // Reject without requeue - message is unprocessable
        try
        {
            await channel
                .BasicRejectAsync(ea.DeliveryTag, requeue: false, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception rejectEx)
        {
            _logger.LogError(rejectEx, "Failed to reject message {DeliveryTag}", ea.DeliveryTag);
        }
    }
}
