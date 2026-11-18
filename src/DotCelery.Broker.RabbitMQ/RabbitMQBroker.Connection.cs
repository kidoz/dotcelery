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
        if (!_deliveries.TryGetValue(KeyOf(message), out var consumerChannel))
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
                arguments: new Dictionary<string, object?>(_queueArguments),
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);
    }
}
