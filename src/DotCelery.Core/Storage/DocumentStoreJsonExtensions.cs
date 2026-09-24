using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace DotCelery.Core.Storage;

/// <summary>
/// Typed access to an <see cref="IDocumentStore"/>, serializing values as JSON with
/// source-generated type information.
/// </summary>
public static class DocumentStoreJsonExtensions
{
    /// <summary>
    /// Gets a document and deserializes its value.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="store">The document store.</param>
    /// <param name="collection">The collection name.</param>
    /// <param name="id">The document id.</param>
    /// <param name="typeInfo">The JSON type information of the value.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The document, or <c>null</c> if it does not exist.</returns>
    public static async ValueTask<StoredDocument<T>?> GetAsync<T>(
        this IDocumentStore store,
        string collection,
        string id,
        JsonTypeInfo<T> typeInfo,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(typeInfo);

        var document = await store
            .GetAsync(collection, id, cancellationToken)
            .ConfigureAwait(false);

        return document is null ? null : Deserialize(document, typeInfo);
    }

    /// <summary>
    /// Serializes a value and inserts it if no document exists with the same id.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="store">The document store.</param>
    /// <param name="collection">The collection name.</param>
    /// <param name="id">The document id.</param>
    /// <param name="value">The value.</param>
    /// <param name="typeInfo">The JSON type information of the value.</param>
    /// <param name="options">Expiry and query keys for the document.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The new version, or <c>null</c> if a document with this id already exists.</returns>
    public static ValueTask<long?> TryInsertAsync<T>(
        this IDocumentStore store,
        string collection,
        string id,
        T value,
        JsonTypeInfo<T> typeInfo,
        DocumentWriteOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(store);
        return store.TryInsertAsync(
            collection,
            id,
            Serialize(value, typeInfo),
            options,
            cancellationToken
        );
    }

    /// <summary>
    /// Serializes a value and replaces the document if its version still matches.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="store">The document store.</param>
    /// <param name="collection">The collection name.</param>
    /// <param name="id">The document id.</param>
    /// <param name="value">The new value.</param>
    /// <param name="expectedVersion">The version read before the change.</param>
    /// <param name="typeInfo">The JSON type information of the value.</param>
    /// <param name="options">Expiry and query keys for the document.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The new version, or <c>null</c> if the document changed or was deleted.</returns>
    public static ValueTask<long?> TryReplaceAsync<T>(
        this IDocumentStore store,
        string collection,
        string id,
        T value,
        long expectedVersion,
        JsonTypeInfo<T> typeInfo,
        DocumentWriteOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(store);
        return store.TryReplaceAsync(
            collection,
            id,
            Serialize(value, typeInfo),
            expectedVersion,
            options,
            cancellationToken
        );
    }

    /// <summary>
    /// Serializes a value and inserts or replaces the document regardless of its version.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="store">The document store.</param>
    /// <param name="collection">The collection name.</param>
    /// <param name="id">The document id.</param>
    /// <param name="value">The value.</param>
    /// <param name="typeInfo">The JSON type information of the value.</param>
    /// <param name="options">Expiry and query keys for the document.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The new version.</returns>
    public static ValueTask<long> UpsertAsync<T>(
        this IDocumentStore store,
        string collection,
        string id,
        T value,
        JsonTypeInfo<T> typeInfo,
        DocumentWriteOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(store);
        return store.UpsertAsync(
            collection,
            id,
            Serialize(value, typeInfo),
            options,
            cancellationToken
        );
    }

    /// <summary>
    /// Queries documents and deserializes their values.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="store">The document store.</param>
    /// <param name="collection">The collection name.</param>
    /// <param name="filter">Which documents to return.</param>
    /// <param name="typeInfo">The JSON type information of the values.</param>
    /// <param name="page">Order and paging.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The matching documents.</returns>
    public static async IAsyncEnumerable<StoredDocument<T>> QueryAsync<T>(
        this IDocumentStore store,
        string collection,
        DocumentFilter filter,
        JsonTypeInfo<T> typeInfo,
        DocumentPage? page = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(typeInfo);

        await foreach (
            var document in store
                .QueryAsync(collection, filter, page, cancellationToken)
                .ConfigureAwait(false)
        )
        {
            yield return Deserialize(document, typeInfo);
        }
    }

    private static byte[] Serialize<T>(T value, JsonTypeInfo<T> typeInfo)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);
        return JsonSerializer.SerializeToUtf8Bytes(value, typeInfo);
    }

    private static StoredDocument<T> Deserialize<T>(
        StoredDocument document,
        JsonTypeInfo<T> typeInfo
    )
    {
        var value =
            JsonSerializer.Deserialize(document.Value.Span, typeInfo)
            ?? throw new InvalidOperationException($"Document '{document.Id}' contains null.");

        return new StoredDocument<T>(
            document.Id,
            value,
            document.Version,
            document.IndexKey,
            document.SortKey,
            document.ExpiresAt
        );
    }
}

/// <summary>
/// A stored document with a deserialized value.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="Id">The document id.</param>
/// <param name="Value">The document value.</param>
/// <param name="Version">The version, which changes on every write.</param>
/// <param name="IndexKey">The index key, if any.</param>
/// <param name="SortKey">The sort key, if any.</param>
/// <param name="ExpiresAt">When the document expires, if it does.</param>
public sealed record StoredDocument<T>(
    string Id,
    T Value,
    long Version,
    string? IndexKey,
    DateTimeOffset? SortKey,
    DateTimeOffset? ExpiresAt
);
