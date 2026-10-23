using DotCelery.Core.Abstractions;
using DotCelery.Core.Security;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// <see cref="IInboxStore"/> on the storage primitives.
/// </summary>
/// <remarks>
/// <para>
/// Processed messages are remembered for <see cref="StorageStoreOptions.InboxRetention"/> and for
/// at least as long as a signed message can be fresh
/// (<see cref="MessageSecurityOptions.MaxMessageAge"/>), so a replay inside the signing window is
/// refused by the inbox even when the retention is shorter.
/// </para>
/// <para>
/// A record written with the caller's transaction is written in that transaction, so the message
/// counts as processed only if the caller commits. Providers that cannot write in a caller's
/// transaction refuse the record rather than store it outside the transaction.
/// </para>
/// </remarks>
public sealed class InboxStore : IInboxStore, IStorageBackedStore
{
    private readonly string _collection;

    private readonly IStorageProvider _storage;
    private readonly IDocumentStore _documents;
    private readonly StorageStoreOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _retention;

    /// <summary>
    /// Initializes a new instance of the <see cref="InboxStore"/> class.
    /// </summary>
    /// <param name="storage">The storage provider.</param>
    /// <param name="options">The store options.</param>
    /// <param name="timeProvider">The clock for processing times.</param>
    /// <param name="securityOptions">The security options, whose message age the retention covers.</param>
    public InboxStore(
        IStorageProvider storage,
        IOptions<StorageStoreOptions>? options = null,
        TimeProvider? timeProvider = null,
        IOptions<MessageSecurityOptions>? securityOptions = null
    )
    {
        ArgumentNullException.ThrowIfNull(storage);
        _storage = storage;
        _documents = TenantStorageScope.For(storage, options).Documents;
        _options = options?.Value ?? new StorageStoreOptions();
        _collection = _options.Name("inbox");
        _timeProvider = timeProvider ?? TimeProvider.System;
        _retention =
            securityOptions?.Value.MaxMessageAge is { } maxAge && maxAge > _options.InboxRetention
                ? maxAge
                : _options.InboxRetention;
    }

    /// <inheritdoc />
    public IStorageProvider Storage => _storage;

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
        var options = new DocumentWriteOptions
        {
            TimeToLive = _retention,
            SortKey = _timeProvider.GetUtcNow(),
        };

        if (transaction is null)
        {
            await UpsertAsync(messageId, options, cancellationToken).ConfigureAwait(false);
            return;
        }

        var transactional = StorageTransactions.Resolve(_storage, transaction);
        await transactional
            .WriteInAsync(transaction, ct => UpsertAsync(messageId, options, ct), cancellationToken)
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

    private async ValueTask UpsertAsync(
        string messageId,
        DocumentWriteOptions options,
        CancellationToken cancellationToken
    )
    {
        await _documents
            .UpsertAsync(
                _collection,
                messageId,
                ReadOnlyMemory<byte>.Empty,
                options,
                cancellationToken
            )
            .ConfigureAwait(false);
    }
}
