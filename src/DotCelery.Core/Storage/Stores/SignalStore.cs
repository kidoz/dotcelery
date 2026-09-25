using System.Runtime.CompilerServices;
using System.Text.Json;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Serialization;
using DotCelery.Core.Signals;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// <see cref="ISignalStore"/> on the storage primitives.
/// </summary>
/// <remarks>
/// Dequeued signals are claimed. A signal that is neither acknowledged nor rejected, for
/// example because its processor crashed, is delivered again after
/// <see cref="StorageStoreOptions.ClaimTimeout"/>.
/// </remarks>
public sealed class SignalStore : ISignalStore
{
    private readonly string _queue;
    private readonly IQueueStore _queues;
    private readonly StorageStoreOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly HeldClaims _claims = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="SignalStore"/> class.
    /// </summary>
    /// <param name="storage">The storage provider.</param>
    /// <param name="options">The store options.</param>
    /// <param name="timeProvider">The clock for enqueue times.</param>
    public SignalStore(
        IStorageProvider storage,
        IOptions<StorageStoreOptions>? options = null,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(storage);
        _queues = storage.Queues;
        _options = options?.Value ?? new StorageStoreOptions();
        _queue = _options.Name("signals");
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public async ValueTask EnqueueAsync(
        SignalMessage message,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(message);

        await _queues
            .EnqueueAsync(
                _queue,
                message.Id,
                JsonSerializer.SerializeToUtf8Bytes(
                    message,
                    DotCeleryJsonContext.Default.SignalMessage
                ),
                _timeProvider.GetUtcNow(),
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<SignalMessage> DequeueAsync(
        int batchSize = 100,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        _claims.RemoveExpired(_timeProvider.GetUtcNow());

        var items = await _queues
            .ClaimAsync(_queue, batchSize, _options.ClaimTimeout, cancellationToken)
            .ConfigureAwait(false);

        foreach (var item in items)
        {
            _claims.Add(item);
            yield return JsonSerializer.Deserialize(
                item.Payload.Span,
                DotCeleryJsonContext.Default.SignalMessage
            )!;
        }
    }

    /// <inheritdoc />
    public async ValueTask AcknowledgeAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        // If the claim expired, another consumer may hold the item now, but it is handled
        if (
            !_claims.TryTake(messageId, out var item)
            || !await _queues.CompleteAsync(item, cancellationToken).ConfigureAwait(false)
        )
        {
            await _queues.RemoveAsync(_queue, messageId, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async ValueTask RejectAsync(
        string messageId,
        bool requeue = true,
        CancellationToken cancellationToken = default
    )
    {
        if (_claims.TryTake(messageId, out var item))
        {
            if (requeue)
            {
                await _queues
                    .AbandonAsync(item, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                await _queues.CompleteAsync(item, cancellationToken).ConfigureAwait(false);
            }
        }
        else if (!requeue)
        {
            await _queues.RemoveAsync(_queue, messageId, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public ValueTask<long> GetPendingCountAsync(CancellationToken cancellationToken = default) =>
        _queues.CountAsync(_queue, cancellationToken);

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
