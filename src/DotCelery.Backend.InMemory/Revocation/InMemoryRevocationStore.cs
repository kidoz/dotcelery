using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;

namespace DotCelery.Backend.InMemory.Revocation;

/// <summary>
/// In-memory revocation store for testing and development.
/// </summary>
public sealed class InMemoryRevocationStore : IRevocationStore
{
    private readonly ConcurrentDictionary<string, RevocationEntry> _revocations = new();
    private readonly Channel<RevocationEvent> _eventChannel;
    private readonly TimeProvider _timeProvider;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryRevocationStore"/> class.
    /// </summary>
    /// <param name="timeProvider">Optional time provider for testing.</param>
    public InMemoryRevocationStore(TimeProvider? timeProvider = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _eventChannel = Channel.CreateUnbounded<RevocationEvent>(
            new UnboundedChannelOptions { SingleReader = false, SingleWriter = false }
        );
    }

    /// <inheritdoc />
    public async ValueTask RevokeAsync(
        string taskId,
        RevokeOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        options ??= RevokeOptions.Default;
        var now = _timeProvider.GetUtcNow();

        var entry = new RevocationEntry
        {
            TaskId = taskId,
            Options = options,
            RevokedAt = now,
            ExpiresAt = options.Expiry.HasValue ? now.Add(options.Expiry.Value) : null,
        };

        _revocations[taskId] = entry;

        // Notify subscribers
        var evt = new RevocationEvent
        {
            TaskId = taskId,
            Options = options,
            Timestamp = now,
        };

        await _eventChannel.Writer.WriteAsync(evt, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask RevokeAsync(
        IEnumerable<string> taskIds,
        RevokeOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(taskIds);

        foreach (var taskId in taskIds)
        {
            await RevokeAsync(taskId, options, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public ValueTask<bool> IsRevokedAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        if (!_revocations.TryGetValue(taskId, out var entry))
        {
            return ValueTask.FromResult(false);
        }

        // Check if expired
        if (entry.ExpiresAt.HasValue && entry.ExpiresAt.Value < _timeProvider.GetUtcNow())
        {
            _revocations.TryRemove(taskId, out _);
            return ValueTask.FromResult(false);
        }

        return ValueTask.FromResult(true);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> GetRevokedTaskIdsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var now = _timeProvider.GetUtcNow();

        foreach (var kvp in _revocations)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Skip expired entries
            if (kvp.Value.ExpiresAt.HasValue && kvp.Value.ExpiresAt.Value < now)
            {
                _revocations.TryRemove(kvp.Key, out _);
                continue;
            }

            yield return kvp.Key;
            await Task.Yield();
        }
    }

    /// <inheritdoc />
    public ValueTask<long> CleanupAsync(
        TimeSpan maxAge,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var cutoff = _timeProvider.GetUtcNow() - maxAge;
        long removed = 0;

        foreach (var kvp in _revocations)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var shouldRemove =
                kvp.Value.RevokedAt < cutoff
                || (
                    kvp.Value.ExpiresAt.HasValue
                    && kvp.Value.ExpiresAt.Value < _timeProvider.GetUtcNow()
                );

            if (shouldRemove && _revocations.TryRemove(kvp.Key, out _))
            {
                removed++;
            }
        }

        return ValueTask.FromResult(removed);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RevocationEvent> SubscribeAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await foreach (
            var evt in _eventChannel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false)
        )
        {
            yield return evt;
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return ValueTask.CompletedTask;
        }

        _disposed = true;
        _eventChannel.Writer.Complete();
        _revocations.Clear();

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Clears all revocations. For testing purposes.
    /// </summary>
    public void Clear()
    {
        _revocations.Clear();
    }

    /// <summary>
    /// Gets the count of active revocations. For testing purposes.
    /// </summary>
    public int Count => _revocations.Count;

    /// <summary>
    /// Gets the revocation options for a specific task. For testing purposes.
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <returns>The revocation options, or null if not revoked.</returns>
    public RevokeOptions? GetOptions(string taskId)
    {
        return _revocations.TryGetValue(taskId, out var entry) ? entry.Options : null;
    }

    private sealed record RevocationEntry
    {
        public required string TaskId { get; init; }
        public required RevokeOptions Options { get; init; }
        public required DateTimeOffset RevokedAt { get; init; }
        public DateTimeOffset? ExpiresAt { get; init; }
    }
}
