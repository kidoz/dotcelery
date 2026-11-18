using System.Text;
using System.Text.Json;
using DotCelery.Core.Models;
using DotCelery.Core.Security;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace DotCelery.Broker.Redis;

public sealed partial class RedisBroker
{
    // Preserve the exact entry before acknowledging it. The Lua script makes quarantine
    // and removal atomic, including a retry after an ambiguous connection failure.
    private Task<RedisResult> QuarantineEntryAsync(
        IDatabase db,
        string streamKey,
        RedisValue entryId,
        string consumerName
    ) =>
        db.ScriptEvaluateAsync(
            PendingOwnerCheck
                + "local entries = redis.call('XRANGE', KEYS[1], ARGV[2], ARGV[2]); "
                + "if #entries == 0 then return 0 end; "
                + "redis.call('XADD', KEYS[2], '*', 'source-stream', KEYS[1], 'source-id', ARGV[2], "
                + "'reason', 'Unreadable or invalid payload', unpack(entries[1][2])); "
                + "redis.call('XACK', KEYS[1], ARGV[1], ARGV[2]); "
                + "return redis.call('XDEL', KEYS[1], ARGV[2])",
            [(RedisKey)streamKey, (RedisKey)(_options.DeadLetterStreamPrefix + streamKey)],
            [(RedisValue)_options.ConsumerGroupName, entryId, consumerName]
        );

    private async Task SettleEntryAsync(
        IDatabase db,
        RedisDelivery delivery,
        NameValueEntry[]? copy = null
    )
    {
        const string settle =
            "redis.call('XACK', KEYS[1], ARGV[1], ARGV[2]); redis.call('XDEL', KEYS[1], ARGV[2]); return 1";
        var script = PendingGenerationCheck;
        var arguments = DeliveryArguments(delivery);
        if (copy is not null)
        {
            script +=
                "local fields = {unpack(ARGV, 6)}; if ARGV[5] ~= '' then "
                + "redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[5], '*', unpack(fields)); "
                + "else redis.call('XADD', KEYS[1], '*', unpack(fields)); end; ";
            arguments =
            [
                .. arguments,
                _options.MaxStreamLength is { } maxLength ? (RedisValue)maxLength : "",
                .. copy.SelectMany(field => new[] { field.Name, field.Value }),
            ];
        }
        var result = await db.ScriptEvaluateAsync(
                script + settle,
                [(RedisKey)delivery.StreamKey],
                arguments
            )
            .ConfigureAwait(false);
        if ((long)result == 0)
        {
            await LoseDeliveryAsync(delivery).ConfigureAwait(false);
            throw new InvalidOperationException(
                "Redis refused to settle an entry this consumer no longer owns."
            );
        }
    }

    private RedisDelivery? ParseStreamEntry(
        string streamKey,
        StreamEntry entry,
        ConsumeSession session
    )
    {
        try
        {
            var payload = entry["payload"];
            if (payload.IsNullOrEmpty)
            {
                _logger.LogWarning("Stream entry {EntryId} has no payload, skipping", entry.Id);
                return null;
            }

            var signature = entry["signature"];
            var signatureText = signature.IsNullOrEmpty ? null : signature.ToString();
            var payloadBytes = (byte[])payload!;
            if (
                _options.MaxMessageSizeBytes > 0
                && payloadBytes.Length > _options.MaxMessageSizeBytes
            )
            {
                _logger.LogWarning("Rejecting oversized Redis stream entry {EntryId}", entry.Id);
                return null;
            }
            if (!_messageSecurityValidator.IsSignatureValid(payloadBytes, signatureText))
            {
                _logger.LogWarning(
                    "Stream entry {EntryId} failed message signature validation",
                    entry.Id
                );
                return null;
            }

            // Deserialize using AOT-friendly type info
            var taskMessage = JsonSerializer.Deserialize(payloadBytes, TaskMessageTypeInfo);

            if (taskMessage is null)
            {
                _logger.LogWarning(
                    "Failed to deserialize stream entry {EntryId}, skipping",
                    entry.Id
                );
                return null;
            }

            // Extract queue name from stream key
            var queue = streamKey.StartsWith(_options.StreamKeyPrefix, StringComparison.Ordinal)
                ? streamKey[_options.StreamKeyPrefix.Length..]
                : streamKey;

            return new RedisDelivery(
                session,
                streamKey,
                entry.Id!,
                taskMessage,
                queue,
                payloadBytes,
                signatureText
            );
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error parsing stream entry {EntryId}", entry.Id);
            return null;
        }
    }

    private async Task EnsureConsumerGroupAsync(IDatabase db, string streamKey)
    {
        lock (_initializedGroups)
        {
            if (_initializedGroups.Contains(streamKey))
            {
                return;
            }
        }

        try
        {
            // Try to create the consumer group
            // The '0' position means all messages (including historical)
            await db.StreamCreateConsumerGroupAsync(
                    streamKey,
                    _options.ConsumerGroupName,
                    "0",
                    createStream: true
                )
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Created consumer group {Group} for stream {Stream}",
                _options.ConsumerGroupName,
                streamKey
            );
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
        {
            // Group already exists, that's fine
            _logger.LogDebug(
                "Consumer group {Group} already exists for stream {Stream}",
                _options.ConsumerGroupName,
                streamKey
            );
        }

        lock (_initializedGroups)
        {
            _initializedGroups.Add(streamKey);
        }
    }

    private void ForgetConsumerGroups(string[] streamKeys)
    {
        lock (_initializedGroups)
        {
            foreach (var streamKey in streamKeys)
            {
                _initializedGroups.Remove(streamKey);
            }
        }
    }

    private NameValueEntry[] CreateStreamEntry(TaskMessage message)
    {
        // Serialize the message to JSON using AOT-friendly type info
        var payload = JsonSerializer.SerializeToUtf8Bytes(message, TaskMessageTypeInfo);
        var signature = _messageSecurityValidator?.Sign(payload);

        // Validate message size
        if (_options.MaxMessageSizeBytes > 0 && payload.Length > _options.MaxMessageSizeBytes)
        {
            throw new InvalidOperationException(
                $"Message size ({payload.Length} bytes) exceeds maximum allowed size ({_options.MaxMessageSizeBytes} bytes)."
            );
        }

        // Build stream entry
        return string.IsNullOrEmpty(signature)
            ? new NameValueEntry[]
            {
                new("payload", payload),
                new(
                    "timestamp",
                    DateTimeOffset
                        .UtcNow.ToUnixTimeMilliseconds()
                        .ToString(System.Globalization.CultureInfo.InvariantCulture)
                ),
            }
            : new NameValueEntry[]
            {
                new("payload", payload),
                new("signature", signature),
                new(
                    "timestamp",
                    DateTimeOffset
                        .UtcNow.ToUnixTimeMilliseconds()
                        .ToString(System.Globalization.CultureInfo.InvariantCulture)
                ),
            };
    }

    private async Task<IDatabase> GetDatabaseAsync(CancellationToken cancellationToken)
    {
        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        return connection.GetDatabase(_options.Database);
    }

    private async Task<ConnectionMultiplexer> GetConnectionAsync(
        CancellationToken cancellationToken
    )
    {
        // The multiplexer reconnects on its own after a connection drops, so it is created once
        // and reused. Replacing it while disconnected would leak connections.
        if (_connection is not null)
        {
            return _connection;
        }

        await _connectionLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_connection is not null)
            {
                return _connection;
            }

            var configOptions = ConfigurationOptions.Parse(_options.ConnectionString);
            configOptions.DefaultDatabase = _options.Database;
            configOptions.ConnectTimeout = (int)_options.ConnectTimeout.TotalMilliseconds;
            configOptions.SyncTimeout = (int)_options.SyncTimeout.TotalMilliseconds;
            configOptions.AbortOnConnectFail = _options.AbortOnConnectFail;

            _connection = await ConnectionMultiplexer
                .ConnectAsync(configOptions)
                .ConfigureAwait(false);

            // The connection string is not logged: it commonly embeds credentials
            _logger.LogInformation("Connected to Redis");

            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private string GetStreamKey(string queue) => $"{_options.StreamKeyPrefix}{queue}";
}
