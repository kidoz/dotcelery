using System.Collections.Concurrent;
using DotCelery.Core.Abstractions;

namespace DotCelery.Backend.InMemory.Outbox;

/// <summary>
/// In-memory implementation of <see cref="IInboxStore"/> for testing.
/// </summary>
public sealed class InMemoryInboxStore : IInboxStore
{
    private readonly ConcurrentDictionary<string, DateTimeOffset> _processedMessages = new();

    /// <inheritdoc />
    public ValueTask<bool> IsProcessedAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        return ValueTask.FromResult(_processedMessages.ContainsKey(messageId));
    }

    /// <inheritdoc />
    public ValueTask MarkProcessedAsync(
        string messageId,
        object? transaction = null,
        CancellationToken cancellationToken = default
    )
    {
        _processedMessages[messageId] = DateTimeOffset.UtcNow;
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<long> GetCountAsync(CancellationToken cancellationToken = default)
    {
        return ValueTask.FromResult((long)_processedMessages.Count);
    }

    /// <inheritdoc />
    public ValueTask<long> CleanupAsync(
        TimeSpan olderThan,
        CancellationToken cancellationToken = default
    )
    {
        var cutoff = DateTimeOffset.UtcNow - olderThan;
        var toRemove = _processedMessages
            .Where(kvp => kvp.Value < cutoff)
            .Select(kvp => kvp.Key)
            .ToList();

        foreach (var id in toRemove)
        {
            _processedMessages.TryRemove(id, out _);
        }

        return ValueTask.FromResult((long)toRemove.Count);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _processedMessages.Clear();
        return ValueTask.CompletedTask;
    }
}
