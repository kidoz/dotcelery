using System.Runtime.CompilerServices;
using System.Text.Json;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Outbox;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// <see cref="IOutboxStore"/> on the storage primitives.
/// </summary>
/// <remarks>
/// <para>
/// Pending messages are claimed, so dispatchers in several processes never publish the same
/// message at the same time. A message whose dispatcher crashes is claimed again after
/// <see cref="StorageStoreOptions.ClaimTimeout"/>, so it can be published twice but is not lost.
/// </para>
/// <para>
/// A failed message is retried after <see cref="StorageStoreOptions.OutboxRetryDelay"/> until
/// <see cref="StorageStoreOptions.OutboxMaxAttempts"/>, and then kept as a failed message for
/// <see cref="StorageStoreOptions.OutboxFailedRetention"/>. Dispatched messages are removed at once.
/// </para>
/// <para>
/// Messages are not written in the caller's database transaction; the <c>transaction</c>
/// argument of <see cref="StoreAsync"/> is ignored.
/// </para>
/// </remarks>
public sealed class OutboxStore : IOutboxStore
{
    private readonly string _queue;
    private readonly string _failedCollection;
    private readonly IQueueStore _queues;
    private readonly IDocumentStore _documents;
    private readonly StorageStoreOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly HeldClaims _claims = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="OutboxStore"/> class.
    /// </summary>
    /// <param name="storage">The storage provider.</param>
    /// <param name="options">The store options.</param>
    /// <param name="timeProvider">The clock for retry times.</param>
    public OutboxStore(
        IStorageProvider storage,
        IOptions<StorageStoreOptions>? options = null,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(storage);
        _queues = storage.Queues;
        _documents = storage.Documents;
        _options = options?.Value ?? new StorageStoreOptions();
        _queue = _options.Name("outbox");
        _failedCollection = _options.Name("outbox.failed");
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public async ValueTask StoreAsync(
        OutboxMessage message,
        object? transaction = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(message);

        await EnqueueAsync(
                message with
                {
                    Status = OutboxMessageStatus.Pending,
                },
                _timeProvider.GetUtcNow(),
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<OutboxMessage> GetPendingAsync(
        int limit = 100,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        _claims.RemoveExpired(_timeProvider.GetUtcNow());

        var items = await _queues
            .ClaimAsync(_queue, limit, _options.ClaimTimeout, cancellationToken)
            .ConfigureAwait(false);

        foreach (var item in items)
        {
            _claims.Add(item);
            yield return Deserialize(item);
        }
    }

    /// <inheritdoc />
    public async ValueTask MarkDispatchedAsync(
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
    public async ValueTask MarkFailedAsync(
        string messageId,
        string errorMessage,
        CancellationToken cancellationToken = default
    )
    {
        // Without a live claim, the message is retried by whoever claims it next
        var now = _timeProvider.GetUtcNow();
        if (!_claims.TryTake(messageId, out var item) || item.ClaimedUntil <= now)
        {
            return;
        }

        var stored = Deserialize(item);
        var message = stored with { Attempts = stored.Attempts + 1, LastError = errorMessage };

        if (message.Attempts < _options.OutboxMaxAttempts)
        {
            // Replacing the item releases the claim and schedules the retry
            await EnqueueAsync(message, now + _options.OutboxRetryDelay, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        await _documents
            .UpsertAsync(
                _failedCollection,
                message.Id,
                message with
                {
                    Status = OutboxMessageStatus.Failed,
                },
                DotCeleryJsonContext.Default.OutboxMessage,
                new DocumentWriteOptions
                {
                    TimeToLive = _options.OutboxFailedRetention,
                    SortKey = now,
                },
                cancellationToken
            )
            .ConfigureAwait(false);
        await _queues.CompleteAsync(item, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public ValueTask<long> GetPendingCountAsync(CancellationToken cancellationToken = default) =>
        _queues.CountAsync(_queue, cancellationToken);

    /// <inheritdoc />
    /// <remarks>
    /// Dispatched messages are removed when they are dispatched, so this removes failed
    /// messages older than <paramref name="olderThan"/>.
    /// </remarks>
    public ValueTask<long> CleanupAsync(
        TimeSpan olderThan,
        CancellationToken cancellationToken = default
    ) =>
        _documents.DeleteManyAsync(
            _failedCollection,
            new DocumentFilter { SortKeyBefore = _timeProvider.GetUtcNow() - olderThan },
            cancellationToken
        );

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    // Attempts counts failed publishes; a redelivery after a crash is not an attempt
    private static OutboxMessage Deserialize(QueueItem item) =>
        JsonSerializer.Deserialize(item.Payload.Span, DotCeleryJsonContext.Default.OutboxMessage)!;

    private ValueTask EnqueueAsync(
        OutboxMessage message,
        DateTimeOffset dueAt,
        CancellationToken cancellationToken
    ) =>
        _queues.EnqueueAsync(
            _queue,
            message.Id,
            JsonSerializer.SerializeToUtf8Bytes(
                message,
                DotCeleryJsonContext.Default.OutboxMessage
            ),
            dueAt,
            cancellationToken
        );
}
