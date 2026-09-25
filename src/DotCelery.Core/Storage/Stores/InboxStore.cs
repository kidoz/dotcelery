using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// <see cref="IInboxStore"/> on the storage primitives.
/// </summary>
/// <remarks>
/// Processed messages are remembered for <see cref="StorageStoreOptions.InboxRetention"/>.
/// Records are not written in the caller's database transaction; the <c>transaction</c>
/// argument of <see cref="MarkProcessedAsync"/> is ignored.
/// </remarks>
public sealed class InboxStore : IInboxStore
{
    private readonly string _collection;

    private readonly IDocumentStore _documents;
    private readonly StorageStoreOptions _options;
    private readonly TimeProvider _timeProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="InboxStore"/> class.
    /// </summary>
    /// <param name="storage">The storage provider.</param>
    /// <param name="options">The store options.</param>
    /// <param name="timeProvider">The clock for processing times.</param>
    public InboxStore(
        IStorageProvider storage,
        IOptions<StorageStoreOptions>? options = null,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(storage);
        _documents = storage.Documents;
        _options = options?.Value ?? new StorageStoreOptions();
        _collection = _options.Name("inbox");
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsProcessedAsync(
        string messageId,
        CancellationToken cancellationToken = default
    ) =>
        await _documents.GetAsync(_collection, messageId, cancellationToken).ConfigureAwait(false)
            is not null;

    /// <inheritdoc />
    public async ValueTask MarkProcessedAsync(
        string messageId,
        object? transaction = null,
        CancellationToken cancellationToken = default
    )
    {
        await _documents
            .UpsertAsync(
                _collection,
                messageId,
                ReadOnlyMemory<byte>.Empty,
                new DocumentWriteOptions
                {
                    TimeToLive = _options.InboxRetention,
                    SortKey = _timeProvider.GetUtcNow(),
                },
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public ValueTask<long> GetCountAsync(CancellationToken cancellationToken = default) =>
        _documents.CountAsync(_collection, DocumentFilter.All, cancellationToken);

    /// <inheritdoc />
    public ValueTask<long> CleanupAsync(
        TimeSpan olderThan,
        CancellationToken cancellationToken = default
    ) =>
        _documents.DeleteManyAsync(
            _collection,
            new DocumentFilter { SortKeyBefore = _timeProvider.GetUtcNow() - olderThan },
            cancellationToken
        );

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
