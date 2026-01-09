using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Outbox;

namespace DotCelery.Backend.InMemory.Outbox;

/// <summary>
/// In-memory implementation of <see cref="IOutboxStore"/> for testing.
/// </summary>
public sealed class InMemoryOutboxStore : IOutboxStore
{
    private readonly ConcurrentDictionary<string, OutboxMessage> _messages = new();
    private readonly Lock _lock = new();
    private long _sequenceNumber;

    /// <inheritdoc />
    public ValueTask StoreAsync(
        OutboxMessage message,
        object? transaction = null,
        CancellationToken cancellationToken = default
    )
    {
        var sequenceNumber = Interlocked.Increment(ref _sequenceNumber);
        var storedMessage = message with { SequenceNumber = sequenceNumber };
        _messages[message.Id] = storedMessage;
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<OutboxMessage> GetPendingAsync(
        int limit = 100,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        var pending = _messages
            .Values.Where(m => m.Status == OutboxMessageStatus.Pending)
            .OrderBy(m => m.SequenceNumber)
            .Take(limit)
            .ToList();

        foreach (var message in pending)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                yield break;
            }

            yield return message;
        }

        await Task.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask MarkDispatchedAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        if (_messages.TryGetValue(messageId, out var message))
        {
            _messages[messageId] = message with
            {
                Status = OutboxMessageStatus.Dispatched,
                DispatchedAt = DateTimeOffset.UtcNow,
            };
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask MarkFailedAsync(
        string messageId,
        string errorMessage,
        CancellationToken cancellationToken = default
    )
    {
        if (_messages.TryGetValue(messageId, out var message))
        {
            _messages[messageId] = message with
            {
                Status = message.Attempts + 1 >= 5 ? OutboxMessageStatus.Failed : message.Status,
                Attempts = message.Attempts + 1,
                LastError = errorMessage,
            };
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<long> GetPendingCountAsync(CancellationToken cancellationToken = default)
    {
        var count = _messages.Values.Count(m => m.Status == OutboxMessageStatus.Pending);
        return ValueTask.FromResult((long)count);
    }

    /// <inheritdoc />
    public ValueTask<long> CleanupAsync(
        TimeSpan olderThan,
        CancellationToken cancellationToken = default
    )
    {
        var cutoff = DateTimeOffset.UtcNow - olderThan;
        var toRemove = _messages
            .Values.Where(m =>
                m.Status == OutboxMessageStatus.Dispatched
                && m.DispatchedAt.HasValue
                && m.DispatchedAt.Value < cutoff
            )
            .Select(m => m.Id)
            .ToList();

        foreach (var id in toRemove)
        {
            _messages.TryRemove(id, out _);
        }

        return ValueTask.FromResult((long)toRemove.Count);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _messages.Clear();
        return ValueTask.CompletedTask;
    }
}
