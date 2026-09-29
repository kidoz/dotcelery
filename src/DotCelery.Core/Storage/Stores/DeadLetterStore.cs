using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Core.Abstractions;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Models;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// <see cref="IDeadLetterStore"/> on the storage primitives.
/// </summary>
/// <remarks>
/// A message expires at its <see cref="DeadLetterMessage.ExpiresAt"/>, or after
/// <see cref="DeadLetterOptions.RetentionPeriod"/> when it has none. When there are more than
/// <see cref="DeadLetterOptions.MaxMessages"/> messages, the oldest are removed.
/// </remarks>
public sealed class DeadLetterStore : IDeadLetterStore
{
    private static readonly JsonTypeInfo<DeadLetterMessage> MessageTypeInfo =
        StoreJson.TypeInfo<DeadLetterMessage>();

    private readonly IDocumentStore _documents;
    private readonly IMessageBroker _broker;
    private readonly IMessageSerializer _serializer;
    private readonly DeadLetterOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly string _collection;

    /// <summary>
    /// Initializes a new instance of the <see cref="DeadLetterStore"/> class.
    /// </summary>
    /// <param name="storage">The storage provider.</param>
    /// <param name="broker">The broker that requeued messages are published to.</param>
    /// <param name="serializer">The serializer of the original messages.</param>
    /// <param name="options">The dead letter options.</param>
    /// <param name="storeOptions">The store options.</param>
    /// <param name="timeProvider">The clock for expiry.</param>
    public DeadLetterStore(
        IStorageProvider storage,
        IMessageBroker broker,
        IMessageSerializer serializer,
        IOptions<DeadLetterOptions>? options = null,
        IOptions<StorageStoreOptions>? storeOptions = null,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentNullException.ThrowIfNull(broker);
        ArgumentNullException.ThrowIfNull(serializer);
        _documents = storage.Documents;
        _broker = broker;
        _serializer = serializer;
        _options = options?.Value ?? new DeadLetterOptions();
        _timeProvider = timeProvider ?? TimeProvider.System;
        _collection = (storeOptions?.Value ?? new StorageStoreOptions()).Name("dead-letters");
    }

    /// <inheritdoc />
    public async ValueTask StoreAsync(
        DeadLetterMessage message,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(message);

        var timeToLive = message.ExpiresAt is { } expiresAt
            ? expiresAt - _timeProvider.GetUtcNow()
            : _options.RetentionPeriod;
        if (timeToLive <= TimeSpan.Zero)
        {
            return;
        }

        await _documents
            .UpsertAsync(
                _collection,
                message.Id,
                message,
                MessageTypeInfo,
                new DocumentWriteOptions { TimeToLive = timeToLive, SortKey = message.Timestamp },
                cancellationToken
            )
            .ConfigureAwait(false);

        await TrimAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    /// <remarks>Messages are returned newest first.</remarks>
    public async IAsyncEnumerable<DeadLetterMessage> GetAllAsync(
        int limit = 100,
        int offset = 0,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        await foreach (
            var document in _documents
                .QueryAsync(
                    _collection,
                    DocumentFilter.All,
                    MessageTypeInfo,
                    new DocumentPage
                    {
                        Descending = true,
                        Offset = offset,
                        Limit = limit,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false)
        )
        {
            yield return document.Value;
        }
    }

    /// <inheritdoc />
    public async ValueTask<DeadLetterMessage?> GetAsync(
        string messageId,
        CancellationToken cancellationToken = default
    ) =>
        (
            await _documents
                .GetAsync(_collection, messageId, MessageTypeInfo, cancellationToken)
                .ConfigureAwait(false)
        )?.Value;

    /// <inheritdoc />
    /// <remarks>
    /// The message is removed before it is published, so concurrent requeues publish it once;
    /// if publishing fails, it is stored again.
    /// </remarks>
    public async ValueTask<bool> RequeueAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        var document = await _documents
            .GetAsync(_collection, messageId, cancellationToken)
            .ConfigureAwait(false);
        if (
            document is null
            || !await _documents
                .DeleteAsync(_collection, messageId, document.Version, cancellationToken)
                .ConfigureAwait(false)
        )
        {
            return false;
        }

        try
        {
            var message = JsonSerializer.Deserialize(document.Value.Span, MessageTypeInfo)!;
            var taskMessage = _serializer.Deserialize<TaskMessage>(message.OriginalMessage);
            await _broker.PublishAsync(taskMessage, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch
        {
            // Unless it expired in the meantime
            var timeToLive = document.ExpiresAt - _timeProvider.GetUtcNow();
            if (timeToLive is null || timeToLive > TimeSpan.Zero)
            {
                await _documents
                    .TryInsertAsync(
                        _collection,
                        messageId,
                        document.Value,
                        new DocumentWriteOptions
                        {
                            TimeToLive = timeToLive,
                            SortKey = document.SortKey,
                        },
                        CancellationToken.None
                    )
                    .ConfigureAwait(false);
            }

            throw;
        }
    }

    /// <inheritdoc />
    public ValueTask<bool> DeleteAsync(
        string messageId,
        CancellationToken cancellationToken = default
    ) => _documents.DeleteAsync(_collection, messageId, cancellationToken: cancellationToken);

    /// <inheritdoc />
    public ValueTask<long> GetCountAsync(CancellationToken cancellationToken = default) =>
        _documents.CountAsync(_collection, DocumentFilter.All, cancellationToken);

    /// <inheritdoc />
    public ValueTask<long> PurgeAsync(CancellationToken cancellationToken = default) =>
        _documents.DeleteManyAsync(_collection, DocumentFilter.All, cancellationToken);

    /// <inheritdoc />
    /// <remarks>
    /// Expired messages are never returned, and the storage purge deletes them; this removes
    /// messages dead-lettered more than <see cref="DeadLetterOptions.RetentionPeriod"/> ago.
    /// </remarks>
    public ValueTask<long> CleanupExpiredAsync(CancellationToken cancellationToken = default) =>
        _documents.DeleteManyAsync(
            _collection,
            new DocumentFilter
            {
                SortKeyBefore = _timeProvider.GetUtcNow() - _options.RetentionPeriod,
            },
            cancellationToken
        );

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private async ValueTask TrimAsync(CancellationToken cancellationToken)
    {
        var excess =
            await _documents
                .CountAsync(_collection, DocumentFilter.All, cancellationToken)
                .ConfigureAwait(false) - _options.MaxMessages;
        if (excess <= 0)
        {
            return;
        }

        var oldest = new List<StoredDocument>();
        await foreach (
            var document in _documents
                .QueryAsync(
                    _collection,
                    DocumentFilter.All,
                    new DocumentPage { Limit = (int)Math.Min(excess, int.MaxValue) },
                    cancellationToken
                )
                .ConfigureAwait(false)
        )
        {
            oldest.Add(document);
        }

        foreach (var document in oldest)
        {
            await _documents
                .DeleteAsync(_collection, document.Id, document.Version, cancellationToken)
                .ConfigureAwait(false);
        }
    }
}
