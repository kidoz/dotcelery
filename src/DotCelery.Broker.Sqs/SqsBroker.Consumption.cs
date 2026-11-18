using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using DotCelery.Core.Abstractions;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Models;
using DotCelery.Core.Security;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Broker.Sqs;

public sealed partial class SqsBroker
{
    private async Task ReceiveAsync(
        IReadOnlyList<string> queues,
        ChannelWriter<BrokerMessage> writer,
        CancellationToken cancellationToken
    )
    {
        var failureDelay = TimeSpan.Zero;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    foreach (var queue in queues)
                    {
                        var queueUrl = await GetQueueUrlAsync(queue, cancellationToken)
                            .ConfigureAwait(false);

                        var response = await _client
                            .ReceiveMessageAsync(
                                new ReceiveMessageRequest
                                {
                                    QueueUrl = queueUrl,
                                    MaxNumberOfMessages = Math.Clamp(_options.BatchSize, 1, 10),
                                    WaitTimeSeconds = (int)
                                        Math.Clamp(_options.WaitTime.TotalSeconds, 0, 20),
                                    VisibilityTimeout = (int)
                                        Math.Max(0, _options.VisibilityTimeout.TotalSeconds),
                                    MessageAttributeNames = ["All"],
                                },
                                cancellationToken
                            )
                            .ConfigureAwait(false);

                        foreach (var sqsMessage in response.Messages)
                        {
                            var brokerMessage = ParseMessage(sqsMessage, queue);
                            if (brokerMessage is null)
                            {
                                await HandleUnreadableMessageAsync(sqsMessage, queueUrl)
                                    .ConfigureAwait(false);
                                continue;
                            }

                            await writer
                                .WriteAsync(brokerMessage, cancellationToken)
                                .ConfigureAwait(false);
                        }
                    }

                    failureDelay = TimeSpan.Zero;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    // Keep receiving through transient errors: a consumer that stops here would
                    // leave the worker running without receiving messages
                    failureDelay = NextFailureDelay(failureDelay);
                    _logger.LogError(
                        ex,
                        "Error receiving from SQS, retrying in {Delay}",
                        failureDelay
                    );

                    try
                    {
                        await Task.Delay(failureDelay, cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }
        }
        finally
        {
            writer.TryComplete();
        }
    }

    // Keeps the visibility of the messages this consumption is processing renewed, so a task
    // that runs longer than the visibility timeout is not delivered to another consumer
    private async Task RenewVisibilityAsync(
        ConsumeSession session,
        CancellationToken cancellationToken
    )
    {
        var interval = _options.VisibilityTimeout / 3;
        if (!_options.RenewVisibilityWhileProcessing || interval <= TimeSpan.Zero)
        {
            return;
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            foreach (var (queue, receiptHandle) in session.Snapshot())
            {
                await _deliveryLock.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    if (
                        !_deliveries.TryGetValue(receiptHandle, out var owner)
                        || !ReferenceEquals(owner, session)
                    )
                        continue;
                    var queueUrl = await GetQueueUrlAsync(queue, cancellationToken)
                        .ConfigureAwait(false);

                    await _client
                        .ChangeMessageVisibilityAsync(
                            new ChangeMessageVisibilityRequest
                            {
                                QueueUrl = queueUrl,
                                ReceiptHandle = receiptHandle,
                                VisibilityTimeout = (int)
                                    Math.Max(0, _options.VisibilityTimeout.TotalSeconds),
                            },
                            cancellationToken
                        )
                        .ConfigureAwait(false);
                }
                catch (AmazonSQSException ex)
                    when (ex.ErrorCode is "MessageNotInflight" or "ReceiptHandleIsInvalid")
                {
                    // The message was settled or its delivery expired; there is nothing to renew
                    ForgetDelivery(receiptHandle);
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                {
                    _logger.LogDebug(ex, "Failed to renew the visibility of a message");
                }
                finally
                {
                    _deliveryLock.Release();
                }
            }
        }
    }

    private async ValueTask ReturnUndeliveredAsync(List<BrokerMessage> undelivered)
    {
        foreach (var message in undelivered)
        {
            try
            {
                var (queueUrl, receiptHandle) = await DeliveryAsync(
                        message,
                        CancellationToken.None,
                        requireOwnership: false
                    )
                    .ConfigureAwait(false);

                await _client
                    .ChangeMessageVisibilityAsync(
                        new ChangeMessageVisibilityRequest
                        {
                            QueueUrl = queueUrl,
                            ReceiptHandle = receiptHandle,
                            VisibilityTimeout = 0,
                        },
                        CancellationToken.None
                    )
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // The message becomes visible again when its visibility timeout expires
                _logger.LogDebug(ex, "Failed to return an undelivered message to its queue");
            }
        }
    }

    private async ValueTask HandleUnreadableMessageAsync(Message sqsMessage, string queueUrl)
    {
        _logger.LogError(
            "Failed to read message {MessageId} from SQS queue {Queue}",
            sqsMessage.MessageId,
            queueUrl
        );

        if (_deadLetterStore is not null)
        {
            try
            {
                await _deadLetterStore
                    .StoreAsync(
                        new DeadLetterMessage
                        {
                            Id = Guid.NewGuid().ToString("N"),
                            TaskId = sqsMessage.MessageId ?? "unknown",
                            TaskName = "unknown",
                            Queue = queueUrl,
                            Reason = DeadLetterReason.DeserializationFailed,
                            OriginalMessage = Encoding.UTF8.GetBytes(
                                sqsMessage.Body ?? string.Empty
                            ),
                            ExceptionMessage = "The message body is not a task message",
                            ExceptionType = nameof(JsonException),
                            Timestamp = DateTimeOffset.UtcNow,
                        },
                        CancellationToken.None
                    )
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to store an unreadable message in the dead letter queue"
                );
                return; // Leave it in SQS for redelivery after the visibility timeout.
            }
        }

        // The message can never be read, so it is removed instead of being delivered forever
        try
        {
            await _client
                .DeleteMessageAsync(queueUrl, sqsMessage.ReceiptHandle, CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to remove an unreadable message from its queue");
        }
    }

    private void ForgetDelivery(string receiptHandle)
    {
        if (_deliveries.TryRemove(receiptHandle, out var session))
        {
            session.Forget(receiptHandle);
        }
    }

    private BrokerMessage? ParseMessage(Message sqsMessage, string queue)
    {
        if (string.IsNullOrEmpty(sqsMessage.Body))
        {
            return null;
        }

        try
        {
            if (
                _options.MaxMessageSizeBytes > 0
                && Encoding.UTF8.GetByteCount(sqsMessage.Body) > _options.MaxMessageSizeBytes
            )
            {
                _logger.LogWarning(
                    "Rejecting oversized SQS message {MessageId}",
                    sqsMessage.MessageId
                );
                return null;
            }
            var body = Encoding.UTF8.GetBytes(sqsMessage.Body);
            if (!_messageSecurityValidator.IsSignatureValid(body, GetSignature(sqsMessage)))
            {
                _logger.LogWarning(
                    "Rejecting SQS message {MessageId} with invalid signature",
                    sqsMessage.MessageId
                );
                return null;
            }
            var message = JsonSerializer.Deserialize(
                body,
                DotCeleryJsonContext.Default.TaskMessage
            );

            if (message is null)
            {
                return null;
            }

            return new BrokerMessage
            {
                Message = message,
                DeliveryTag = sqsMessage.ReceiptHandle,
                Queue = queue,
                ReceivedAt = DateTimeOffset.UtcNow,
                RawBody = body,
                Signature = GetSignature(sqsMessage),
            };
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static string? GetSignature(Message sqsMessage) =>
        sqsMessage.MessageAttributes is not null
        && sqsMessage.MessageAttributes.TryGetValue(SignatureAttribute, out var attribute)
            ? attribute.StringValue
            : null;
}
