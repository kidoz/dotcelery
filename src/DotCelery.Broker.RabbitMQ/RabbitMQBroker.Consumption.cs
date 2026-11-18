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

public sealed partial class RabbitMQBroker
{
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
                        await consumerChannel.DisposeAsync().ConfigureAwait(false);
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
        await consumerChannel
            .UseAsync(
                async () =>
                {
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
                },
                cancellationToken
            )
            .ConfigureAwait(false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (sender, ea) =>
        {
            BrokerMessage? brokerMessage = null;
            try
            {
                if (
                    _options.MaxMessageSizeBytes > 0
                    && ea.Body.Length > _options.MaxMessageSizeBytes
                )
                {
                    _logger.LogWarning(
                        "Rejecting oversized delivery {DeliveryTag}",
                        ea.DeliveryTag
                    );
                    await consumerChannel
                        .RejectReceivedAsync(ea.DeliveryTag, false, cancellationToken)
                        .ConfigureAwait(false);
                    return;
                }
                // Deserialize using AOT-friendly type info
                var body = ea.Body.ToArray();
                var signature = GetSignature(ea.BasicProperties);
                if (!_messageSecurityValidator.IsSignatureValid(body, signature))
                {
                    await HandleSecurityFailureAsync(ea, consumerChannel, cancellationToken)
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
                    await HandleDeserializationFailureAsync(
                            ea,
                            consumerChannel,
                            jsonEx,
                            cancellationToken
                        )
                        .ConfigureAwait(false);
                    return;
                }

                if (taskMessage is null)
                {
                    await HandleDeserializationFailureAsync(
                            ea,
                            consumerChannel,
                            null,
                            cancellationToken
                        )
                        .ConfigureAwait(false);
                    return;
                }

                brokerMessage = new BrokerMessage
                {
                    Message = taskMessage,
                    DeliveryTag = new DeliveryReceipt(ea.DeliveryTag, Guid.NewGuid()),
                    Queue = ea.RoutingKey,
                    ReceivedAt = DateTimeOffset.UtcNow,
                    RawBody = body,
                    Signature = signature,
                };

                consumerChannel.Track(ea.DeliveryTag, brokerMessage);
                _deliveries[KeyOf(brokerMessage)] = consumerChannel;

                await writer.WriteAsync(brokerMessage, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Error processing received message {DeliveryTag}",
                    ea.DeliveryTag
                );

                // Infrastructure failures must not discard an accepted delivery.
                try
                {
                    await consumerChannel
                        .RejectReceivedAsync(ea.DeliveryTag, requeue: true, cancellationToken)
                        .ConfigureAwait(false);
                    if (brokerMessage is not null)
                        _deliveries.TryRemove(KeyOf(brokerMessage), out _);
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
            string? tag = null;
            await consumerChannel
                .UseAsync(
                    async () =>
                    {
                        tag = await channel
                            .BasicConsumeAsync(
                                queue: queue,
                                autoAck: false,
                                consumer: consumer,
                                cancellationToken: cancellationToken
                            )
                            .ConfigureAwait(false);
                    },
                    cancellationToken
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
            _deliveries.TryRemove(KeyOf(message), out _);
        }
    }

    private async Task HandleDeserializationFailureAsync(
        BasicDeliverEventArgs ea,
        ConsumerChannel channel,
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
                await channel
                    .RejectReceivedAsync(ea.DeliveryTag, requeue: true, cancellationToken)
                    .ConfigureAwait(false);
                return;
            }
        }

        // Reject without requeue - message is unprocessable
        try
        {
            await channel
                .RejectReceivedAsync(ea.DeliveryTag, requeue: false, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception rejectEx)
        {
            _logger.LogError(rejectEx, "Failed to reject message {DeliveryTag}", ea.DeliveryTag);
        }
    }

    private async Task HandleSecurityFailureAsync(
        BasicDeliverEventArgs ea,
        ConsumerChannel channel,
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
            .RejectReceivedAsync(ea.DeliveryTag, requeue: false, cancellationToken)
            .ConfigureAwait(false);
    }

    private static DeliveryKey KeyOf(BrokerMessage message) =>
        new((DeliveryReceipt)message.DeliveryTag, message.Message.Id, message.Queue);

    // A delivery is identified by its channel-local tag and the message it carries, so an
    // equivalent copy of the delivered message settles the same delivery
    private readonly record struct DeliveryReceipt(ulong Tag, Guid Generation);

    private readonly record struct DeliveryKey(
        DeliveryReceipt DeliveryTag,
        string MessageId,
        string Queue
    );

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
