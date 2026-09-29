using System.Text.Json.Serialization.Metadata;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// Read-modify-write of a document that is retried while other writers change it in between.
/// </summary>
internal static class DocumentUpdates
{
    /// <summary>
    /// Replaces the document with <paramref name="update"/> applied to its current value.
    /// </summary>
    /// <returns>
    /// The stored value, which is the current value when <paramref name="update"/> returns
    /// <c>null</c> to leave it unchanged, or <c>null</c> if there is no document.
    /// </returns>
    public static async ValueTask<T?> UpdateAsync<T>(
        this IDocumentStore documents,
        string collection,
        string id,
        JsonTypeInfo<T> typeInfo,
        Func<StoredDocument<T>, T?> update,
        Func<T, DocumentWriteOptions?> writeOptions,
        CancellationToken cancellationToken
    )
        where T : class
    {
        while (true)
        {
            var current = await documents
                .GetAsync(collection, id, typeInfo, cancellationToken)
                .ConfigureAwait(false);
            if (current is null)
            {
                return null;
            }

            var updated = update(current);
            if (updated is null)
            {
                return current.Value;
            }

            var replaced = await documents
                .TryReplaceAsync(
                    collection,
                    id,
                    updated,
                    current.Version,
                    typeInfo,
                    writeOptions(updated),
                    cancellationToken
                )
                .ConfigureAwait(false);
            if (replaced is not null)
            {
                return updated;
            }
        }
    }
}
