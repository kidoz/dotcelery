using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Channels;
using DotCelery.Core.Abstractions;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Models;
using DotCelery.Core.Security;
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
    private readonly IMessageSecurityValidator? _messageSecurityValidator;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly SemaphoreSlim _publishChannelLock = new(1, 1);

    // The channel a message arrived on, so it is settled on that channel and no other
    private readonly ConcurrentDictionary<BrokerMessage, ConsumerChannel> _deliveries = new();

    // AOT-friendly type info for TaskMessage serialization
    private static JsonTypeInfo<TaskMessage> TaskMessageTypeInfo =>
        DotCeleryJsonContext.Default.TaskMessage;

    private IConnection? _connection;
    private IChannel? _publishChannel;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMQBroker"/> class.
    /// </summary>
    /// <param name="options">The broker options.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="deadLetterStore">Optional dead letter store for deserialization failures.</param>
    /// <param name="messageSecurityValidator">Optional message security validator.</param>
    public RabbitMQBroker(
        IOptions<RabbitMQBrokerOptions> options,
        ILogger<RabbitMQBroker> logger,
        IDeadLetterStore? deadLetterStore = null,
        IMessageSecurityValidator? messageSecurityValidator = null
    )
    {
        _options = options.Value;
        _logger = logger;
        _deadLetterStore = deadLetterStore;
        _messageSecurityValidator = messageSecurityValidator;
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

        var signature = _messageSecurityValidator?.Sign(body);
        if (!string.IsNullOrEmpty(signature))
        {
            properties.Headers = new Dictionary<string, object?>
            {
                ["x-dotcelery-signature"] = Encoding.UTF8.GetBytes(signature),
            };
        }

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
                mandatory: _options.MandatoryPublish,
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
    /// <remarks>
    /// The loop owns its channel and rebuilds it, and its consumers, after the channel or its
    /// connection is lost, so the stream pauses across a broker restart instead of ending.
    /// Anything the lost channel had delivered is requeued by the broker and delivered again on
    /// the new channel.
    /// </remarks>
    public async IAsyncEnumerable<BrokerMessage> ConsumeAsync(
        IReadOnlyList<string> queues,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(queues);

        // Create a channel to buffer messages
        var messageChannel = Channel.CreateBounded<BrokerMessage>(
            new BoundedChannelOptions(100)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false,
            }
        );

        using var consumingCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var consuming = ConsumeUntilLostAsync(queues, messageChannel.Writer, consumingCts.Token);

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
            await consumingCts.CancelAsync().ConfigureAwait(false);

            try
            {
                await consuming.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error while stopping the consumer");
            }

            messageChannel.Writer.TryComplete();
        }
    }

    // Consumes until cancelled, rebuilding the channel and the consumers whenever the channel or
    // its connection is lost
    private async Task ConsumeUntilLostAsync(
        IReadOnlyList<string> queues,
        ChannelWriter<BrokerMessage> writer,
        CancellationToken cancellationToken
    )
    {
        while (!cancellationToken.IsCancellationRequested && !_disposed)
        {
            ConsumerChannel? consumerChannel = null;
            IConnection? connection = null;
            AsyncEventHandler<ShutdownEventArgs>? onShutdown = null;

            try
            {
                connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
                consumerChannel = new ConsumerChannel(
                    await connection
                        .CreateChannelAsync(cancellationToken: cancellationToken)
                        .ConfigureAwait(false)
                );

                var lost = new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously
                );
                onShutdown = (_, _) =>
                {
                    lost.TrySetResult();
                    return Task.CompletedTask;
                };
                connection.ConnectionShutdownAsync += onShutdown;
                consumerChannel.Channel.ChannelShutdownAsync += onShutdown;

                await StartConsumersAsync(consumerChannel, queues, writer, cancellationToken)
                    .ConfigureAwait(false);

                await lost.Task.WaitAsync(cancellationToken).ConfigureAwait(false);

                if (!cancellationToken.IsCancellationRequested)
                {
                    _logger.LogWarning(
                        "The consume channel or its connection closed; reconnecting"
                    );
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "The consumer stopped; reconnecting");
            }
            finally
            {
                if (consumerChannel is not null)
                {
                    ForgetDeliveries(consumerChannel);

                    try
                    {
                        consumerChannel.Channel.Dispose();
                    }
                    catch (Exception ex)
                    {
                        _logger.LogDebug(ex, "Error disposing the consume channel");
                    }
                }

                if (connection is not null && onShutdown is not null)
                {
                    connection.ConnectionShutdownAsync -= onShutdown;
                }
            }

            try
            {
                await Task.Delay(_options.ReconnectDelay, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task StartConsumersAsync(
        ConsumerChannel consumerChannel,
        IReadOnlyList<string> queues,
        ChannelWriter<BrokerMessage> writer,
        CancellationToken cancellationToken
    )
    {
        var channel = consumerChannel.Channel;

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

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            try
            {
                // Deserialize using AOT-friendly type info
                var body = ea.Body.ToArray();
                var signature = GetSignature(ea.BasicProperties);
                if (!IsSignatureValid(body, signature))
                {
                    await HandleSecurityFailureAsync(ea, channel, cancellationToken)
                        .ConfigureAwait(false);
                    return;
                }

                TaskMessage? taskMessage;
                try
                {
                    taskMessage = JsonSerializer.Deserialize(body, TaskMessageTypeInfo);
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
                    RawBody = body,
                    Signature = signature,
                };

                consumerChannel.Track(ea.DeliveryTag, brokerMessage);
                _deliveries[brokerMessage] = consumerChannel;

                await writer.WriteAsync(brokerMessage, cancellationToken).ConfigureAwait(false);
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
            _logger.LogInformation(
                "Started consuming from queue {Queue} with tag {ConsumerTag}",
                queue,
                tag
            );
        }
    }

    // The deliveries of a channel that is gone can no longer be settled: the broker requeued
    // them, and they are delivered again on a new channel with new tags
    private void ForgetDeliveries(ConsumerChannel consumerChannel)
    {
        foreach (var message in consumerChannel.ForgetInFlight())
        {
            _deliveries.TryRemove(message, out _);
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

        var consumerChannel = RequireDeliveringChannel(message, "acknowledged");
        await consumerChannel.AckAsync(deliveryTag, cancellationToken).ConfigureAwait(false);

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

        var consumerChannel = RequireDeliveringChannel(message, "rejected");
        await consumerChannel
            .RejectAsync(deliveryTag, requeue, cancellationToken)
            .ConfigureAwait(false);

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
        _deliveries.Clear();

        _logger.LogInformation("RabbitMQ broker disposed");
    }

    /// <remarks>
    /// A closed connection is never reused: the broker connects again, and the consumer rebuilds
    /// its channel and consumers on the new connection. Automatic recovery stays off so that
    /// reconnecting has a single owner.
    /// </remarks>
    private async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken)
    {
        if (_connection is { IsOpen: true })
        {
            return _connection;
        }

        await _connectionLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_connection is { IsOpen: true })
            {
                return _connection;
            }

            if (_connection is not null)
            {
                try
                {
                    _connection.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Error disposing a closed RabbitMQ connection");
                }

                _connection = null;
            }

            var factory = new ConnectionFactory
            {
                Uri = new Uri(_options.ConnectionString),
                ClientProvidedName = _options.ConnectionName,
                RequestedHeartbeat = _options.Heartbeat,
                AutomaticRecoveryEnabled = false,
            };

            for (var attempt = 1; attempt <= _options.ConnectionRetryCount; attempt++)
            {
                try
                {
                    _connection = await factory
                        .CreateConnectionAsync(cancellationToken)
                        .ConfigureAwait(false);
                    break;
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
            _connection ??= await factory
                .CreateConnectionAsync(cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation("Connected to RabbitMQ");
            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    // A message is settled only on the channel that delivered it: delivery tags are
    // channel-local, so another channel's tag of the same value means another message
    private ConsumerChannel RequireDeliveringChannel(BrokerMessage message, string action)
    {
        if (!_deliveries.TryRemove(message, out var consumerChannel))
        {
            throw new InvalidOperationException(
                $"Message {message.Message.Id} cannot be {action}: it was settled already, or its "
                    + "channel was lost and the broker requeued it for redelivery."
            );
        }

        return consumerChannel;
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
            var channelOptions = new CreateChannelOptions(
                publisherConfirmationsEnabled: _options.EnablePublisherConfirms,
                publisherConfirmationTrackingEnabled: _options.EnablePublisherConfirms
            );
            var replaced = _publishChannel;
            _publishChannel = await connection
                .CreateChannelAsync(channelOptions, cancellationToken)
                .ConfigureAwait(false);
            replaced?.Dispose();

            return _publishChannel;
        }
        finally
        {
            _publishChannelLock.Release();
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

    private bool IsSignatureValid(byte[] body, string? signature)
    {
        if (_messageSecurityValidator is null)
        {
            return true;
        }

        var validation = _messageSecurityValidator.Validate(
            new TaskMessage
            {
                Id = "unknown",
                Task = "unknown",
                Args = [],
                ContentType = "application/json",
                Timestamp = DateTimeOffset.UtcNow,
            },
            signature
        );

        if (validation.ErrorCode == MessageValidationError.MissingSignature)
        {
            return false;
        }

        return string.IsNullOrEmpty(signature)
            || _messageSecurityValidator.VerifySignature(body, signature);
    }

    private async Task HandleSecurityFailureAsync(
        BasicDeliverEventArgs ea,
        IChannel channel,
        CancellationToken cancellationToken
    )
    {
        _logger.LogWarning(
            "Rejected message {DeliveryTag} from queue {Queue} due to invalid or missing signature",
            ea.DeliveryTag,
            ea.RoutingKey
        );

        if (_deadLetterStore is not null)
        {
            var deadLetterMessage = new DeadLetterMessage
            {
                Id = Guid.NewGuid().ToString(),
                TaskId = ea.BasicProperties?.MessageId ?? "unknown",
                TaskName = ea.BasicProperties?.Type ?? "unknown",
                Queue = ea.RoutingKey,
                Reason = DeadLetterReason.Rejected,
                OriginalMessage = ea.Body.ToArray(),
                ExceptionMessage = "Message signature is invalid or missing",
                ExceptionType = nameof(MessageSecurityException),
                Timestamp = DateTimeOffset.UtcNow,
            };

            await _deadLetterStore
                .StoreAsync(deadLetterMessage, cancellationToken)
                .ConfigureAwait(false);
        }

        await channel
            .BasicRejectAsync(ea.DeliveryTag, requeue: false, cancellationToken)
            .ConfigureAwait(false);
    }

    private static string? GetSignature(IReadOnlyBasicProperties? properties)
    {
        if (
            properties?.Headers is null
            || !properties.Headers.TryGetValue("x-dotcelery-signature", out var value)
        )
        {
            return null;
        }

        return value switch
        {
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            ReadOnlyMemory<byte> bytes => Encoding.UTF8.GetString(bytes.Span),
            string text => text,
            _ => value?.ToString(),
        };
    }
}
