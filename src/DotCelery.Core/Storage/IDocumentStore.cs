namespace DotCelery.Core.Storage;

/// <summary>
/// Stores versioned documents in named collections.
/// </summary>
/// <remarks>
/// <para>
/// Values are opaque bytes. Every write gives the document a new version, and a version is
/// never reused for the same document, so <see cref="TryReplaceAsync"/> detects any write made
/// since the document was read.
/// </para>
/// <para>
/// A document whose expiry has passed is treated as deleted by every operation. Providers
/// remove expired documents physically in <see cref="IStorageProvider.PurgeExpiredAsync"/>.
/// </para>
/// </remarks>
public interface IDocumentStore
{
    /// <summary>
    /// Gets a document.
    /// </summary>
    /// <param name="collection">The collection name.</param>
    /// <param name="id">The document id.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The document, or <c>null</c> if it does not exist.</returns>
    ValueTask<StoredDocument?> GetAsync(
        string collection,
        string id,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Inserts a document if none exists with the same id.
    /// </summary>
    /// <param name="collection">The collection name.</param>
    /// <param name="id">The document id.</param>
    /// <param name="value">The document value.</param>
    /// <param name="options">Expiry and query keys for the document.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The new version, or <c>null</c> if a document with this id already exists.</returns>
    ValueTask<long?> TryInsertAsync(
        string collection,
        string id,
        ReadOnlyMemory<byte> value,
        DocumentWriteOptions? options = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Replaces a document if its version still matches.
    /// </summary>
    /// <param name="collection">The collection name.</param>
    /// <param name="id">The document id.</param>
    /// <param name="value">The new value.</param>
    /// <param name="expectedVersion">The version read before the change.</param>
    /// <param name="options">Expiry and query keys for the document; they replace the current ones.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// The new version, or <c>null</c> if the document was changed or deleted since
    /// <paramref name="expectedVersion"/>.
    /// </returns>
    ValueTask<long?> TryReplaceAsync(
        string collection,
        string id,
        ReadOnlyMemory<byte> value,
        long expectedVersion,
        DocumentWriteOptions? options = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Inserts a document, or replaces it regardless of its version.
    /// </summary>
    /// <param name="collection">The collection name.</param>
    /// <param name="id">The document id.</param>
    /// <param name="value">The document value.</param>
    /// <param name="options">Expiry and query keys for the document; they replace the current ones.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The new version.</returns>
    ValueTask<long> UpsertAsync(
        string collection,
        string id,
        ReadOnlyMemory<byte> value,
        DocumentWriteOptions? options = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Deletes a document.
    /// </summary>
    /// <param name="collection">The collection name.</param>
    /// <param name="id">The document id.</param>
    /// <param name="expectedVersion">When set, the document is deleted only if its version matches.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><c>true</c> if a document was deleted.</returns>
    ValueTask<bool> DeleteAsync(
        string collection,
        string id,
        long? expectedVersion = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Queries documents, ordered by sort key and then id.
    /// </summary>
    /// <remarks>
    /// In ascending order, documents without a sort key come first; descending order is the
    /// exact reverse.
    /// </remarks>
    /// <param name="collection">The collection name.</param>
    /// <param name="filter">Which documents to return.</param>
    /// <param name="page">Order and paging; by default all matches in ascending order.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The matching documents.</returns>
    IAsyncEnumerable<StoredDocument> QueryAsync(
        string collection,
        DocumentFilter filter,
        DocumentPage? page = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Counts documents.
    /// </summary>
    /// <param name="collection">The collection name.</param>
    /// <param name="filter">Which documents to count.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of matching documents.</returns>
    ValueTask<long> CountAsync(
        string collection,
        DocumentFilter filter,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Deletes all matching documents.
    /// </summary>
    /// <param name="collection">The collection name.</param>
    /// <param name="filter">Which documents to delete.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of documents deleted.</returns>
    ValueTask<long> DeleteManyAsync(
        string collection,
        DocumentFilter filter,
        CancellationToken cancellationToken = default
    );
}

/// <summary>
/// A stored document.
/// </summary>
/// <param name="Id">The document id.</param>
/// <param name="Value">The document value.</param>
/// <param name="Version">The version, which changes on every write.</param>
/// <param name="IndexKey">The key that <see cref="DocumentFilter.IndexKey"/> matches, if any.</param>
/// <param name="SortKey">The time that queries sort and filter by, if any.</param>
/// <param name="ExpiresAt">When the document expires, if it does.</param>
public sealed record StoredDocument(
    string Id,
    ReadOnlyMemory<byte> Value,
    long Version,
    string? IndexKey,
    DateTimeOffset? SortKey,
    DateTimeOffset? ExpiresAt
);

/// <summary>
/// Expiry and query keys of a document write.
/// </summary>
public sealed record DocumentWriteOptions
{
    /// <summary>
    /// Gets how long the document lives. It never expires when not set.
    /// </summary>
    public TimeSpan? TimeToLive { get; init; }

    /// <summary>
    /// Gets the key that <see cref="DocumentFilter.IndexKey"/> matches, such as <c>state:Executing</c>.
    /// </summary>
    public string? IndexKey { get; init; }

    /// <summary>
    /// Gets the time that queries sort and filter by.
    /// </summary>
    public DateTimeOffset? SortKey { get; init; }
}

/// <summary>
/// Selects documents in a collection.
/// </summary>
public sealed record DocumentFilter
{
    /// <summary>
    /// Gets a filter that matches every document.
    /// </summary>
    public static DocumentFilter All { get; } = new();

    /// <summary>
    /// Gets the index key that documents must have. Any index key matches when not set.
    /// </summary>
    public string? IndexKey { get; init; }

    /// <summary>
    /// Gets the earliest sort key to match, inclusive. Documents without a sort key never match a range.
    /// </summary>
    public DateTimeOffset? SortKeyFrom { get; init; }

    /// <summary>
    /// Gets the sort key that matches must be earlier than. Documents without a sort key never match a range.
    /// </summary>
    public DateTimeOffset? SortKeyBefore { get; init; }
}

/// <summary>
/// Order and paging of a document query.
/// </summary>
public sealed record DocumentPage
{
    /// <summary>
    /// Gets whether documents are returned in descending order.
    /// </summary>
    public bool Descending { get; init; }

    /// <summary>
    /// Gets the number of matching documents to skip.
    /// </summary>
    public int Offset { get; init; }

    /// <summary>
    /// Gets the maximum number of documents to return. All matches are returned when not set.
    /// </summary>
    public int? Limit { get; init; }
}
