using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using DotCelery.Core.Abstractions;
using DotCelery.Core.DeadLetter;
using Microsoft.Extensions.Options;

namespace DotCelery.Backend.InMemory.DeadLetter;

/// <summary>
/// In-memory implementation of <see cref="IDeadLetterStore"/>.
/// </summary>
public sealed class InMemoryDeadLetterStore : IDeadLetterStore
{
    private readonly ConcurrentDictionary<string, DeadLetterMessage> _messages = new();
    private readonly IMessageBroker _broker;
    private readonly IMessageSerializer _serializer;
    private readonly DeadLetterOptions _options;

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryDeadLetterStore"/> class.
    /// </summary>
    public InMemoryDeadLetterStore(
        IMessageBroker broker,
        IMessageSerializer serializer,
        IOptions<DeadLetterOptions> options
    )
    {
        _broker = broker;
        _serializer = serializer;
        _options = options.Value;
    }

    /// <inheritdoc />
    public ValueTask StoreAsync(
        DeadLetterMessage message,
        CancellationToken cancellationToken = default
    )
    {
        _messages[message.Id] = message;

        // Enforce max messages limit
        EnforceMaxMessages();

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<DeadLetterMessage> GetAllAsync(
        int limit = 100,
        int offset = 0,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        var messages = _messages
            .Values.OrderByDescending(m => m.Timestamp)
            .Skip(offset)
            .Take(limit);

        foreach (var message in messages)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return message;
        }

        await Task.CompletedTask; // Keep async enumerable signature
    }

    /// <inheritdoc />
    public ValueTask<DeadLetterMessage?> GetAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        _messages.TryGetValue(messageId, out var message);
        return ValueTask.FromResult(message);
    }

    /// <inheritdoc />
    public async ValueTask<bool> RequeueAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        if (!_messages.TryRemove(messageId, out var message))
        {
            return false;
        }

        try
        {
            var taskMessage = _serializer.Deserialize<Core.Models.TaskMessage>(
                message.OriginalMessage
            );
            if (taskMessage is not null)
            {
                await _broker.PublishAsync(taskMessage, cancellationToken).ConfigureAwait(false);
            }
            return true;
        }
        catch
        {
            // If requeue fails, put message back
            _messages[messageId] = message;
            throw;
        }
    }

    /// <inheritdoc />
    public ValueTask<bool> DeleteAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        var removed = _messages.TryRemove(messageId, out _);
        return ValueTask.FromResult(removed);
    }

    /// <inheritdoc />
    public ValueTask<long> GetCountAsync(CancellationToken cancellationToken = default)
    {
        return ValueTask.FromResult((long)_messages.Count);
    }

    /// <inheritdoc />
    public ValueTask<long> PurgeAsync(CancellationToken cancellationToken = default)
    {
        var count = _messages.Count;
        _messages.Clear();
        return ValueTask.FromResult((long)count);
    }

    /// <inheritdoc />
    public ValueTask<long> CleanupExpiredAsync(CancellationToken cancellationToken = default)
    {
        var now = DateTimeOffset.UtcNow;
        var expiredIds = _messages
            .Values.Where(m => m.ExpiresAt.HasValue && m.ExpiresAt.Value < now)
            .Select(m => m.Id)
            .ToList();

        long count = 0;
        foreach (var id in expiredIds)
        {
            if (_messages.TryRemove(id, out _))
            {
                count++;
            }
        }

        return ValueTask.FromResult(count);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _messages.Clear();
        return ValueTask.CompletedTask;
    }

    private void EnforceMaxMessages()
    {
        if (_messages.Count <= _options.MaxMessages)
        {
            return;
        }

        // Remove oldest messages
        var oldestIds = _messages
            .Values.OrderBy(m => m.Timestamp)
            .Take(_messages.Count - _options.MaxMessages)
            .Select(m => m.Id)
            .ToList();

        foreach (var id in oldestIds)
        {
            _messages.TryRemove(id, out _);
        }
    }
}
