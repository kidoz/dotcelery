using DotCelery.Core.Storage;

namespace DotCelery.Backend.InMemory.Storage;

/// <summary>
/// In-memory <see cref="IDocumentStore"/>.
/// </summary>
internal sealed class InMemoryDocumentStore : IDocumentStore
{
    private readonly Dictionary<(string Collection, string Id), Entry> _documents = [];
    private readonly Lock _lock = new();
    private readonly TimeProvider _timeProvider;
    private long _lastVersion;

    public InMemoryDocumentStore(TimeProvider timeProvider)
    {
        _timeProvider = timeProvider;
    }

    public ValueTask<StoredDocument?> GetAsync(
        string collection,
        string id,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(collection);
        StorageGuard.ThrowIfInvalidKey(id);

        lock (_lock)
        {
            return ValueTask.FromResult(
                TryGetLive(collection, id, out var entry) ? ToDocument(id, entry) : null
            );
        }
    }

    public ValueTask<long?> TryInsertAsync(
        string collection,
        string id,
        ReadOnlyMemory<byte> value,
        DocumentWriteOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ValidateWrite(collection, id, options);

        lock (_lock)
        {
            if (TryGetLive(collection, id, out _))
            {
                return ValueTask.FromResult<long?>(null);
            }

            return ValueTask.FromResult<long?>(Write(collection, id, value, options));
        }
    }

    public ValueTask<long?> TryReplaceAsync(
        string collection,
        string id,
        ReadOnlyMemory<byte> value,
        long expectedVersion,
        DocumentWriteOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ValidateWrite(collection, id, options);

        lock (_lock)
        {
            if (!TryGetLive(collection, id, out var entry) || entry.Version != expectedVersion)
            {
                return ValueTask.FromResult<long?>(null);
            }

            return ValueTask.FromResult<long?>(Write(collection, id, value, options));
        }
    }

    public ValueTask<long> UpsertAsync(
        string collection,
        string id,
        ReadOnlyMemory<byte> value,
        DocumentWriteOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ValidateWrite(collection, id, options);

        lock (_lock)
        {
            return ValueTask.FromResult(Write(collection, id, value, options));
        }
    }

    public ValueTask<bool> DeleteAsync(
        string collection,
        string id,
        long? expectedVersion = null,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(collection);
        StorageGuard.ThrowIfInvalidKey(id);

        lock (_lock)
        {
            if (
                !TryGetLive(collection, id, out var entry)
                || (expectedVersion is not null && entry.Version != expectedVersion)
            )
            {
                return ValueTask.FromResult(false);
            }

            _documents.Remove((collection, id));
            return ValueTask.FromResult(true);
        }
    }

    public IAsyncEnumerable<StoredDocument> QueryAsync(
        string collection,
        DocumentFilter filter,
        DocumentPage? page = null,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(collection);
        ValidateFilter(filter);
        page ??= new DocumentPage();
        ArgumentOutOfRangeException.ThrowIfNegative(page.Offset, nameof(page));
        if (page.Limit is < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(page), "Limit must be at least 1.");
        }

        List<StoredDocument> results;
        lock (_lock)
        {
            var matches = Match(collection, filter)
                .OrderBy(m => m.Entry.SortKey.HasValue)
                .ThenBy(m => m.Entry.SortKey)
                .ThenBy(m => m.Id, StringComparer.Ordinal)
                .Select(m => ToDocument(m.Id, m.Entry));

            if (page.Descending)
            {
                matches = matches.Reverse();
            }

            matches = matches.Skip(page.Offset);
            if (page.Limit is { } limit)
            {
                matches = matches.Take(limit);
            }

            results = [.. matches];
        }

        return results.ToAsyncEnumerable();
    }

    public ValueTask<long> CountAsync(
        string collection,
        DocumentFilter filter,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(collection);
        ValidateFilter(filter);

        lock (_lock)
        {
            return ValueTask.FromResult((long)Match(collection, filter).Count());
        }
    }

    public ValueTask<long> DeleteManyAsync(
        string collection,
        DocumentFilter filter,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(collection);
        ValidateFilter(filter);

        lock (_lock)
        {
            var ids = Match(collection, filter).Select(m => m.Id).ToList();
            foreach (var id in ids)
            {
                _documents.Remove((collection, id));
            }

            return ValueTask.FromResult((long)ids.Count);
        }
    }

    internal long PurgeExpired()
    {
        lock (_lock)
        {
            var now = _timeProvider.GetUtcNow();
            var expired = _documents
                .Where(d => IsExpired(d.Value, now))
                .Select(d => d.Key)
                .ToList();
            foreach (var key in expired)
            {
                _documents.Remove(key);
            }

            return expired.Count;
        }
    }

    private static void ValidateWrite(string collection, string id, DocumentWriteOptions? options)
    {
        StorageGuard.ThrowIfInvalidName(collection);
        StorageGuard.ThrowIfInvalidKey(id);

        if (options is not null)
        {
            StorageGuard.ThrowIfInvalidOptionalKey(options.IndexKey);
            if (options.TimeToLive is { } timeToLive)
            {
                StorageGuard.ThrowIfNotPositive(timeToLive);
            }
        }
    }

    private static void ValidateFilter(DocumentFilter filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        StorageGuard.ThrowIfInvalidOptionalKey(filter.IndexKey);
    }

    private static bool IsExpired(Entry entry, DateTimeOffset now) => entry.ExpiresAt <= now;

    private static StoredDocument ToDocument(string id, Entry entry) =>
        new(id, entry.Value, entry.Version, entry.IndexKey, entry.SortKey, entry.ExpiresAt);

    private IEnumerable<(string Id, Entry Entry)> Match(string collection, DocumentFilter filter)
    {
        var now = _timeProvider.GetUtcNow();

        foreach (var ((entryCollection, id), entry) in _documents)
        {
            if (
                entryCollection == collection
                && !IsExpired(entry, now)
                && (filter.IndexKey is null || filter.IndexKey == entry.IndexKey)
                && (filter.SortKeyFrom is null || entry.SortKey >= filter.SortKeyFrom)
                && (filter.SortKeyBefore is null || entry.SortKey < filter.SortKeyBefore)
            )
            {
                yield return (id, entry);
            }
        }
    }

    private bool TryGetLive(string collection, string id, out Entry entry)
    {
        if (_documents.TryGetValue((collection, id), out entry!))
        {
            if (!IsExpired(entry, _timeProvider.GetUtcNow()))
            {
                return true;
            }

            _documents.Remove((collection, id));
        }

        return false;
    }

    private long Write(
        string collection,
        string id,
        ReadOnlyMemory<byte> value,
        DocumentWriteOptions? options
    )
    {
        var version = ++_lastVersion;
        _documents[(collection, id)] = new Entry(
            value.ToArray(),
            version,
            options?.IndexKey,
            options?.SortKey,
            options?.TimeToLive is { } timeToLive ? _timeProvider.GetUtcNow() + timeToLive : null
        );

        return version;
    }

    private sealed record Entry(
        byte[] Value,
        long Version,
        string? IndexKey,
        DateTimeOffset? SortKey,
        DateTimeOffset? ExpiresAt
    );
}
