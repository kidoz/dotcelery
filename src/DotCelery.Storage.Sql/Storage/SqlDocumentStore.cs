using System.Data.Common;
using System.Runtime.CompilerServices;
using DotCelery.Core.Storage;
using DotCelery.Storage.Sql.Execution;

namespace DotCelery.Storage.Sql.Storage;

/// <summary>
/// <see cref="IDocumentStore"/> over <see cref="SqlStorageStatements"/>.
/// </summary>
internal sealed class SqlDocumentStore : IDocumentStore
{
    private readonly SqlStorageStatements _statements;
    private readonly SqlExecutor _sql;
    private readonly TimeProvider _timeProvider;

    public SqlDocumentStore(
        SqlStorageStatements statements,
        SqlExecutor sql,
        TimeProvider timeProvider
    )
    {
        _statements = statements;
        _sql = sql;
        _timeProvider = timeProvider;
    }

    public async ValueTask<StoredDocument?> GetAsync(
        string collection,
        string id,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(collection);
        StorageGuard.ThrowIfInvalidKey(id);

        return await _sql.QuerySingleAsync(
                _statements.DocumentGet,
                p =>
                    p.Text("collection", collection)
                        .Text("id", id)
                        .Timestamp("now", _timeProvider.GetUtcNow()),
                r => ReadDocument(id, r, firstColumn: 0),
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    public async ValueTask<long?> TryInsertAsync(
        string collection,
        string id,
        ReadOnlyMemory<byte> value,
        DocumentWriteOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ValidateWrite(collection, id, options);
        var now = _timeProvider.GetUtcNow();

        return await _sql.QuerySingleAsync<long?>(
                _statements.DocumentInsert,
                p => BindWrite(p, collection, id, value, options, now).Timestamp("now", now),
                r => r.GetInt64(0),
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    public async ValueTask<long?> TryReplaceAsync(
        string collection,
        string id,
        ReadOnlyMemory<byte> value,
        long expectedVersion,
        DocumentWriteOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ValidateWrite(collection, id, options);
        var now = _timeProvider.GetUtcNow();

        return await _sql.QuerySingleAsync<long?>(
                _statements.DocumentReplace,
                p =>
                    BindWrite(p, collection, id, value, options, now)
                        .Timestamp("now", now)
                        .Integer64("expected_version", expectedVersion),
                r => r.GetInt64(0),
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    public async ValueTask<long> UpsertAsync(
        string collection,
        string id,
        ReadOnlyMemory<byte> value,
        DocumentWriteOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ValidateWrite(collection, id, options);
        var now = _timeProvider.GetUtcNow();

        var version = await _sql.QuerySingleAsync<long?>(
                _statements.DocumentUpsert,
                p => BindWrite(p, collection, id, value, options, now),
                r => r.GetInt64(0),
                cancellationToken
            )
            .ConfigureAwait(false);

        return version
            ?? throw new InvalidOperationException("The upsert statement returned no version.");
    }

    public async ValueTask<bool> DeleteAsync(
        string collection,
        string id,
        long? expectedVersion = null,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(collection);
        StorageGuard.ThrowIfInvalidKey(id);

        var deleted = await _sql.ExecuteAsync(
                expectedVersion is null
                    ? _statements.DocumentDelete
                    : _statements.DocumentDeleteVersion,
                p =>
                {
                    p.Text("collection", collection)
                        .Text("id", id)
                        .Timestamp("now", _timeProvider.GetUtcNow());
                    if (expectedVersion is not null)
                    {
                        p.Integer64("expected_version", expectedVersion);
                    }
                },
                cancellationToken
            )
            .ConfigureAwait(false);

        return deleted > 0;
    }

    public IAsyncEnumerable<StoredDocument> QueryAsync(
        string collection,
        DocumentFilter filter,
        DocumentPage? page = null,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(collection);
        StorageGuard.ThrowIfInvalid(filter);
        StorageGuard.ThrowIfInvalid(page);

        return QueryIteratorAsync(collection, filter, page, cancellationToken);
    }

    public async ValueTask<long> CountAsync(
        string collection,
        DocumentFilter filter,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(collection);
        StorageGuard.ThrowIfInvalid(filter);

        return await _sql.QuerySingleAsync(
                _statements.DocumentCount(DocumentFilterShape.Of(filter)),
                p => BindFilter(p, collection, filter),
                r => r.GetInt64(0),
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    public async ValueTask<long> DeleteManyAsync(
        string collection,
        DocumentFilter filter,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(collection);
        StorageGuard.ThrowIfInvalid(filter);

        return await _sql.ExecuteAsync(
                _statements.DocumentDeleteMany(DocumentFilterShape.Of(filter)),
                p => BindFilter(p, collection, filter),
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    private static void ValidateWrite(string collection, string id, DocumentWriteOptions? options)
    {
        StorageGuard.ThrowIfInvalidName(collection);
        StorageGuard.ThrowIfInvalidKey(id);
        StorageGuard.ThrowIfInvalid(options);
    }

    private static SqlParameters BindWrite(
        SqlParameters parameters,
        string collection,
        string id,
        ReadOnlyMemory<byte> value,
        DocumentWriteOptions? options,
        DateTimeOffset now
    ) =>
        parameters
            .Text("collection", collection)
            .Text("id", id)
            .Binary("value", value)
            .Text("index_key", options?.IndexKey)
            .Timestamp("sort_key", options?.SortKey)
            .Timestamp("expires_at", now + options?.TimeToLive);

    private static StoredDocument ReadDocument(string id, DbDataReader reader, int firstColumn) =>
        new(
            id,
            reader.GetFieldValue<byte[]>(firstColumn),
            reader.GetInt64(firstColumn + 1),
            reader.IsDBNull(firstColumn + 2) ? null : reader.GetString(firstColumn + 2),
            SqlRead.NullableTimestamp(reader, firstColumn + 3),
            SqlRead.NullableTimestamp(reader, firstColumn + 4)
        );

    private async IAsyncEnumerable<StoredDocument> QueryIteratorAsync(
        string collection,
        DocumentFilter filter,
        DocumentPage? page,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        var shape = DocumentQueryShape.Of(filter, page);

        var documents = await _sql.QueryAsync(
                _statements.DocumentQuery(shape),
                p =>
                {
                    BindFilter(p, collection, filter);
                    if (shape.Limit)
                    {
                        p.Integer32("limit", page!.Limit);
                    }

                    if (shape.Offset)
                    {
                        p.Integer32("offset", page!.Offset);
                    }
                },
                r => ReadDocument(r.GetString(0), r, firstColumn: 1),
                cancellationToken
            )
            .ConfigureAwait(false);

        foreach (var document in documents)
        {
            yield return document;
        }
    }

    private void BindFilter(SqlParameters parameters, string collection, DocumentFilter filter)
    {
        parameters.Text("collection", collection).Timestamp("now", _timeProvider.GetUtcNow());

        if (filter.IndexKey is not null)
        {
            parameters.Text("index_key", filter.IndexKey);
        }

        if (filter.SortKeyFrom is not null)
        {
            parameters.Timestamp("sort_from", filter.SortKeyFrom);
        }

        if (filter.SortKeyBefore is not null)
        {
            parameters.Timestamp("sort_before", filter.SortKeyBefore);
        }
    }
}

/// <summary>
/// Reads nullable values from data readers.
/// </summary>
internal static class SqlRead
{
    public static DateTimeOffset? NullableTimestamp(DbDataReader reader, int column) =>
        reader.IsDBNull(column) ? null : reader.GetFieldValue<DateTimeOffset>(column);
}
