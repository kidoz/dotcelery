using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Signals;

namespace DotCelery.Backend.InMemory.Signals;

/// <summary>
/// In-memory implementation of <see cref="ISignalStore"/> for testing.
/// </summary>
public sealed class InMemorySignalStore : ISignalStore
{
    private readonly ConcurrentQueue<SignalMessage> _queue = new();
    private readonly ConcurrentDictionary<string, SignalMessage> _processing = new();

    /// <inheritdoc />
    public ValueTask EnqueueAsync(
        SignalMessage message,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(message);

        _queue.Enqueue(message);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<SignalMessage> DequeueAsync(
        int batchSize = 100,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        var count = 0;

        while (count < batchSize && _queue.TryDequeue(out var message))
        {
            if (cancellationToken.IsCancellationRequested)
            {
                // Put it back if cancelled
                _queue.Enqueue(message);
                yield break;
            }

            // Track as processing
            _processing[message.Id] = message;
            count++;

            yield return message;
        }

        await Task.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask AcknowledgeAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        _processing.TryRemove(messageId, out _);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask RejectAsync(
        string messageId,
        bool requeue = true,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        if (_processing.TryRemove(messageId, out var message) && requeue)
        {
            _queue.Enqueue(message);
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<long> GetPendingCountAsync(CancellationToken cancellationToken = default)
    {
        return ValueTask.FromResult((long)_queue.Count);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _queue.Clear();
        _processing.Clear();
        return ValueTask.CompletedTask;
    }
}
