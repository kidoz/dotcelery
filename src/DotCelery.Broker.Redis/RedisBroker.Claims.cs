using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Threading.Channels;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Instrumentation;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace DotCelery.Broker.Redis;

public sealed partial class RedisBroker
{
    private const int PendingPagesPerStream = 4;
    private const int RenewalBatchSize = 128;
    private const string PendingOwnerCheck =
        "local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1); "
        + "if #pending == 0 or pending[1][2] ~= ARGV[3] then return 0 end; ";

    private const string RenewScript = """
        local renewed = {}
        for i = 3, #ARGV, 2 do
            local id = ARGV[i]
            local pending = redis.call('XPENDING', KEYS[1], ARGV[1], id, id, 1)
            if #pending == 1 and pending[1][2] == ARGV[2] and pending[1][4] == tonumber(ARGV[i + 1]) then
                local claimed = redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], 0, id, 'JUSTID')
                if #claimed == 1 then renewed[#renewed + 1] = id end
            end
        end
        return renewed
        """;

    private static readonly Counter<long> ClaimRenewalFailures =
        DotCeleryMetrics.Meter.CreateCounter<long>(
            "dotcelery.broker.claim_renewal_failures",
            description: "Redis delivery claims that could not be renewed"
        );

    private async Task RenewClaimsAsync(
        IDatabase db,
        ConsumeSession session,
        CancellationToken cancellationToken
    )
    {
        var claimTimeout = _options.ClaimTimeout;
        var interval = claimTimeout / 3;
        if (interval <= TimeSpan.Zero)
            return;

        using var timer = new PeriodicTimer(interval);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                foreach (var stream in session.Deliveries.GroupBy(delivery => delivery.StreamKey))
                foreach (var batch in stream.Chunk(RenewalBatchSize))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await RenewBatchAsync(db, stream.Key, batch, session).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
    }

    private async Task RenewBatchAsync(
        IDatabase db,
        string stream,
        RedisDelivery[] batch,
        ConsumeSession session
    )
    {
        var args = new RedisValue[2 * batch.Length + 2];
        args[0] = _options.ConsumerGroupName;
        args[1] = session.ConsumerName;
        for (var i = 0; i < batch.Length; i++)
        {
            args[2 * i + 2] = batch[i].MessageId;
            args[2 * i + 3] = batch[i].DeliveryCount;
        }

        HashSet<RedisValue> renewed;
        var startedAt = Environment.TickCount64;
        try
        {
            var result = await db.ScriptEvaluateAsync(RenewScript, [(RedisKey)stream], args)
                .ConfigureAwait(false);
            renewed = ((RedisValue[]?)result ?? []).ToHashSet();
        }
        catch (RedisException exception)
        {
            ReportRenewalFailure(session, stream, batch.Length, exception);
            var now = Environment.TickCount64;
            foreach (var delivery in batch)
                if (now - delivery.LastRenewal >= _options.ClaimTimeout.TotalMilliseconds)
                    await LoseDeliveryAsync(delivery).ConfigureAwait(false);
            return;
        }

        var lost = 0;
        foreach (var delivery in batch)
        {
            if (renewed.Contains(delivery.MessageId))
                delivery.LastRenewal = startedAt;
            else if (await LoseDeliveryAsync(delivery).ConfigureAwait(false))
                lost++;
        }
        ReportRenewalFailure(session, stream, lost, null);
    }

    private void ReportRenewalFailure(
        ConsumeSession session,
        string stream,
        int failureCount,
        Exception? exception
    )
    {
        if (failureCount == 0)
            return;
        ClaimRenewalFailures.Add(failureCount);
        var now = Environment.TickCount64;
        if (session.LastRenewalWarning is { } previous && now - previous < 60_000)
            return;
        session.LastRenewalWarning = now;
        _logger.LogWarning(
            exception,
            "Could not renew {ClaimCount} delivery claims on Redis stream {Stream}",
            failureCount,
            stream
        );
    }

    private async Task<int> ProcessPendingMessagesAsync(
        IDatabase db,
        string[] streamKeys,
        ChannelWriter<BrokerMessage> writer,
        ConsumeSession session,
        CancellationToken cancellationToken
    )
    {
        var minIdleTime = (long)_options.ClaimTimeout.TotalMilliseconds;
        var pageSize = Math.Max(1, _options.PrefetchCount);
        var reclaimed = 0;
        foreach (var streamKey in streamKeys)
            for (var page = 0; page < PendingPagesPerStream; page++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var cursor = session.PendingCursors.GetValueOrDefault(streamKey, RedisValue.Null);
                var pending = await db.StreamPendingMessagesAsync(
                        streamKey,
                        _options.ConsumerGroupName,
                        pageSize,
                        RedisValue.Null,
                        minId: cursor.IsNull ? (RedisValue?)null : cursor
                    )
                    .ConfigureAwait(false);

                var next =
                    pending.Length == pageSize
                        ? NextPendingId(pending[^1].MessageId!)
                        : RedisValue.Null;
                var idleIds = pending
                    .Where(entry =>
                        entry.IdleTimeInMilliseconds >= minIdleTime
                        && (
                            entry.ConsumerName != session.ConsumerName
                            || !_deliveries.ContainsKey($"{streamKey}:{entry.MessageId}")
                        )
                    )
                    .Select(entry => entry.MessageId)
                    .ToArray();
                if (idleIds.Length > 0)
                {
                    var entries = await db.StreamClaimAsync(
                            streamKey,
                            _options.ConsumerGroupName,
                            session.ConsumerName,
                            minIdleTime,
                            idleIds
                        )
                        .ConfigureAwait(false);
                    var batch = new List<RedisDelivery>(entries.Length);
                    foreach (var entry in entries)
                    {
                        if (ParseStreamEntry(streamKey, entry, session) is { } delivery)
                            batch.Add(delivery);
                        else
                            await QuarantineEntryAsync(
                                    db,
                                    streamKey,
                                    entry.Id,
                                    session.ConsumerName
                                )
                                .ConfigureAwait(false);
                    }
                    await WriteBatchAsync(db, writer, batch, session, cancellationToken)
                        .ConfigureAwait(false);
                    reclaimed += batch.Count;
                    foreach (
                        var previous in pending
                            .Where(entry => entry.ConsumerName != session.ConsumerName)
                            .Select(entry => entry.ConsumerName)
                            .Distinct()
                    )
                        await RemoveIdleConsumerAsync(db, streamKey, previous)
                            .ConfigureAwait(false);
                }
                session.PendingCursors[streamKey] = next;
                if (next.IsNull)
                    break;
            }
        return reclaimed;
    }

    // Redis 5 only accepts inclusive bounds for XPENDING, so advance the numeric stream ID.
    private static RedisValue NextPendingId(string id)
    {
        var separator = id.IndexOf('-');
        var milliseconds = ulong.Parse(id.AsSpan(0, separator), CultureInfo.InvariantCulture);
        var sequence = ulong.Parse(id.AsSpan(separator + 1), CultureInfo.InvariantCulture);
        if (sequence < ulong.MaxValue)
            return string.Create(CultureInfo.InvariantCulture, $"{milliseconds}-{sequence + 1}");
        return milliseconds == ulong.MaxValue
            ? RedisValue.Null
            : string.Create(CultureInfo.InvariantCulture, $"{milliseconds + 1}-0");
    }

    private async Task WriteBatchAsync(
        IDatabase db,
        ChannelWriter<BrokerMessage> writer,
        List<RedisDelivery> batch,
        ConsumeSession session,
        CancellationToken cancellationToken
    )
    {
        if (batch.Count == 0)
            return;

        // Capture each Redis delivery generation before making it available to the worker.
        // Renewals and settlements check this generation as well as the session name.
        const string readGenerations = """
            local counts = {}
            for i = 3, #ARGV do
                local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[i], ARGV[i], 1)
                counts[#counts + 1] = (#pending == 1 and pending[1][2] == ARGV[2]) and pending[1][4] or 0
            end
            return counts
            """;
        var counts = (long[])
            (
                await db.ScriptEvaluateAsync(
                        readGenerations,
                        [(RedisKey)batch[0].StreamKey],
                        [
                            (RedisValue)_options.ConsumerGroupName,
                            session.ConsumerName,
                            .. batch.Select(delivery => (RedisValue)delivery.MessageId),
                        ]
                    )
                    .ConfigureAwait(false)
            )!;
        var owned = new List<RedisDelivery>(batch.Count);
        for (var i = 0; i < batch.Count; i++)
        {
            var delivery = batch[i];
            if (counts[i] == 0)
            {
                delivery.Dispose();
                continue;
            }
            delivery.DeliveryCount = counts[i];
            owned.Add(delivery);
            session.Track(delivery);
            while (!_deliveries.TryAdd(delivery.EntryKey, delivery))
            {
                if (_deliveries.TryGetValue(delivery.EntryKey, out var previous))
                    await LoseDeliveryAsync(previous).ConfigureAwait(false);
            }
        }

        for (var i = 0; i < owned.Count; i++)
        {
            try
            {
                await writer.WriteAsync(owned[i].Message, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                await ReturnUndeliveredAsync(
                        owned.Skip(i).Select(delivery => delivery.Message).ToList()
                    )
                    .ConfigureAwait(false);
                throw;
            }
        }
    }

    private async Task RemoveIdleConsumerAsync(IDatabase db, string streamKey, RedisValue consumer)
    {
        try
        {
            await db.ScriptEvaluateAsync(
                    "local p = redis.call('XPENDING', KEYS[1], ARGV[1], '-', '+', 1, ARGV[2]); "
                        + "if #p == 0 then return redis.call('XGROUP', 'DELCONSUMER', KEYS[1], ARGV[1], ARGV[2]) end; return 0",
                    [(RedisKey)streamKey],
                    [(RedisValue)_options.ConsumerGroupName, consumer]
                )
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not clean up Redis consumer {Consumer}", consumer);
        }
    }

    private RedisDelivery RequireOwned(BrokerMessage message, string tag)
    {
        var separator = tag.LastIndexOf(':');
        if (
            separator < 0
            || !_deliveries.TryGetValue(tag[..separator], out var delivery)
            || !Equals(delivery.Message.DeliveryTag, tag)
            || delivery.Message.ClaimLost.IsCancellationRequested
        )
            throw new InvalidOperationException(
                $"Message {message.Message.Id} cannot be settled because its delivery is no longer owned."
            );
        return delivery;
    }

    private const string PendingGenerationCheck =
        PendingOwnerCheck + "if pending[1][4] ~= tonumber(ARGV[4]) then return 0 end; ";

    private RedisValue[] DeliveryArguments(RedisDelivery delivery) =>
        [
            _options.ConsumerGroupName,
            delivery.MessageId,
            delivery.Session.ConsumerName,
            delivery.DeliveryCount,
        ];

    private async Task EnsurePendingOwnerAsync(IDatabase db, RedisDelivery delivery)
    {
        var result = await db.ScriptEvaluateAsync(
                PendingGenerationCheck + "return 1",
                [(RedisKey)delivery.StreamKey],
                DeliveryArguments(delivery)
            )
            .ConfigureAwait(false);
        if ((long)result == 0)
        {
            await LoseDeliveryAsync(delivery).ConfigureAwait(false);
            throw new InvalidOperationException(
                "Redis refused to requeue an entry this consumer no longer owns."
            );
        }
    }

    private void ForgetDelivery(RedisDelivery delivery)
    {
        if (
            _deliveries.TryRemove(
                new KeyValuePair<string, RedisDelivery>(delivery.EntryKey, delivery)
            )
        )
        {
            delivery.Session.Forget(delivery);
            delivery.Dispose();
        }
    }

    private async ValueTask<bool> LoseDeliveryAsync(RedisDelivery delivery)
    {
        if (
            !_deliveries.TryRemove(
                new KeyValuePair<string, RedisDelivery>(delivery.EntryKey, delivery)
            )
        )
            return false;
        delivery.Session.Forget(delivery);
        using (delivery)
        {
            try
            {
                await delivery.LoseClaimAsync().ConfigureAwait(false);
            }
            catch (AggregateException exception)
            {
                _logger.LogWarning(
                    exception,
                    "A callback failed while cancelling a lost Redis delivery claim"
                );
            }
        }
        return true;
    }

    private sealed class ConsumeSession(string consumerName)
    {
        public string ConsumerName { get; } = consumerName;
        private readonly ConcurrentDictionary<object, RedisDelivery> _deliveries = new();

        public ICollection<RedisDelivery> Deliveries => _deliveries.Values;
        public Dictionary<string, RedisValue> PendingCursors { get; } = new(StringComparer.Ordinal);
        public long? LastRenewalWarning { get; set; }

        public void Track(RedisDelivery delivery) =>
            _deliveries.TryAdd(delivery.Message.DeliveryTag, delivery);

        public void Forget(RedisDelivery delivery) =>
            _deliveries.TryRemove(delivery.Message.DeliveryTag, out _);
    }

    private sealed class RedisDelivery : IDisposable
    {
        private readonly CancellationTokenSource _claimLost = new();

        public RedisDelivery(
            ConsumeSession session,
            string streamKey,
            string messageId,
            TaskMessage task,
            string queue,
            byte[] body,
            string? signature
        )
        {
            Session = session;
            StreamKey = streamKey;
            MessageId = messageId;
            EntryKey = $"{streamKey}:{messageId}";
            Message = new BrokerMessage
            {
                Message = task,
                DeliveryTag = $"{EntryKey}:{Guid.NewGuid():N}",
                Queue = queue,
                ReceivedAt = DateTimeOffset.UtcNow,
                RawBody = body,
                Signature = signature,
                ClaimLost = _claimLost.Token,
            };
        }

        public ConsumeSession Session { get; }
        public string StreamKey { get; }
        public string MessageId { get; }
        public long DeliveryCount { get; set; }
        public string EntryKey { get; }
        public BrokerMessage Message { get; }
        public long LastRenewal { get; set; } = Environment.TickCount64;

        public Task LoseClaimAsync() => _claimLost.CancelAsync();

        public void Dispose() => _claimLost.Dispose();
    }
}
