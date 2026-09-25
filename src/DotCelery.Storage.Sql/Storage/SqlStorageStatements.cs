using DotCelery.Core.Storage;

namespace DotCelery.Storage.Sql.Storage;

/// <summary>
/// The statements of the storage primitives in one database's SQL, for the tables in
/// <see cref="SqlStorageSchema"/>. A dialect implements each statement with the parameters
/// and result columns documented here; the primitives are implemented once on top of them.
/// </summary>
/// <remarks>
/// Parameters are named <c>@name</c>. <c>@now</c> is always the caller's current time: a row
/// with <c>expires_at &lt;= @now</c> is expired and must be treated as absent.
/// </remarks>
public abstract class SqlStorageStatements
{
    /// <summary>
    /// Gets a query for a live document. Parameters: <c>@collection</c>, <c>@id</c>, <c>@now</c>.
    /// Columns: value, version, index_key, sort_key, expires_at.
    /// </summary>
    public abstract string DocumentGet { get; }

    /// <summary>
    /// Gets a statement that inserts a document, or replaces an expired one, and returns the
    /// new version; it returns no row if a live document exists. Parameters: <c>@collection</c>,
    /// <c>@id</c>, <c>@value</c>, <c>@index_key</c>, <c>@sort_key</c>, <c>@expires_at</c>, <c>@now</c>.
    /// </summary>
    public abstract string DocumentInsert { get; }

    /// <summary>
    /// Gets a statement that replaces a live document whose version is <c>@expected_version</c>
    /// and returns the new version. Parameters: those of <see cref="DocumentInsert"/> and
    /// <c>@expected_version</c>.
    /// </summary>
    public abstract string DocumentReplace { get; }

    /// <summary>
    /// Gets a statement that inserts or replaces a document and returns the new version.
    /// Parameters: <c>@collection</c>, <c>@id</c>, <c>@value</c>, <c>@index_key</c>,
    /// <c>@sort_key</c>, <c>@expires_at</c>.
    /// </summary>
    public abstract string DocumentUpsert { get; }

    /// <summary>
    /// Gets a statement that deletes a live document. Parameters: <c>@collection</c>, <c>@id</c>, <c>@now</c>.
    /// </summary>
    public abstract string DocumentDelete { get; }

    /// <summary>
    /// Gets a statement that deletes a live document whose version is <c>@expected_version</c>.
    /// Parameters: <c>@collection</c>, <c>@id</c>, <c>@expected_version</c>, <c>@now</c>.
    /// </summary>
    public abstract string DocumentDeleteVersion { get; }

    /// <summary>
    /// Gets a statement that inserts or replaces a queue item, releasing any claim and
    /// resetting the delivery count. Parameters: <c>@queue</c>, <c>@id</c>, <c>@payload</c>, <c>@due_at</c>.
    /// </summary>
    public abstract string QueueEnqueue { get; }

    /// <summary>
    /// Gets a statement that atomically claims up to <c>@max_items</c> due, unclaimed items in
    /// order of due time and then id, skipping items other transactions are claiming.
    /// Parameters: <c>@queue</c>, <c>@now</c>, <c>@max_items</c>, <c>@claim_token</c>,
    /// <c>@claimed_until</c>. Columns: id, payload, due_at, delivery_count, claim_token, claimed_until.
    /// </summary>
    public abstract string QueueClaim { get; }

    /// <summary>
    /// Gets a statement that removes an item still claimed with <c>@claim_token</c>.
    /// Parameters: <c>@queue</c>, <c>@id</c>, <c>@claim_token</c>.
    /// </summary>
    public abstract string QueueComplete { get; }

    /// <summary>
    /// Gets a statement that releases a claim held with <c>@claim_token</c> and sets the due
    /// time. Parameters: <c>@queue</c>, <c>@id</c>, <c>@claim_token</c>, <c>@due_at</c>.
    /// </summary>
    public abstract string QueueAbandon { get; }

    /// <summary>
    /// Gets a statement that removes an item. Parameters: <c>@queue</c>, <c>@id</c>.
    /// </summary>
    public abstract string QueueRemove { get; }

    /// <summary>
    /// Gets a query for the number of items in a queue. Parameter: <c>@queue</c>.
    /// </summary>
    public abstract string QueueCount { get; }

    /// <summary>
    /// Gets a query for the earliest due time of unclaimed items, or null.
    /// Parameters: <c>@queue</c>, <c>@now</c>.
    /// </summary>
    public abstract string QueueNextDue { get; }

    /// <summary>
    /// Gets a statement that acquires a lease if the key is free, expired, or already held by
    /// <c>@owner</c> (which keeps its token), and returns it. Parameters: <c>@key</c>,
    /// <c>@owner</c>, <c>@expires_at</c>, <c>@now</c>. Columns: key, owner, token, acquired_at,
    /// expires_at. The same owner keeps its token and acquisition time; a new holder gets new ones.
    /// </summary>
    public abstract string LeaseAcquire { get; }

    /// <summary>
    /// Gets a statement that extends a live lease with <c>@token</c> and returns it.
    /// Parameters: <c>@key</c>, <c>@token</c>, <c>@expires_at</c>, <c>@now</c>.
    /// </summary>
    public abstract string LeaseRenew { get; }

    /// <summary>
    /// Gets a statement that deletes a lease with <c>@token</c>. Parameters: <c>@key</c>, <c>@token</c>.
    /// </summary>
    public abstract string LeaseRelease { get; }

    /// <summary>
    /// Gets a query for a live lease. Parameters: <c>@key</c>, <c>@now</c>.
    /// </summary>
    public abstract string LeaseGet { get; }

    /// <summary>
    /// Gets a query for live leases whose keys start with <c>@prefix</c>, in ordinal key order.
    /// Parameters: <c>@prefix</c>, <c>@now</c>.
    /// </summary>
    public abstract string LeaseList { get; }

    /// <summary>
    /// Gets a statement that adds <c>@delta</c> to a live counter, or creates it with
    /// <c>@expires_at</c> if it is missing or expired, and returns the value.
    /// Parameters: <c>@key</c>, <c>@delta</c>, <c>@expires_at</c>, <c>@now</c>.
    /// </summary>
    public abstract string CounterIncrement { get; }

    /// <summary>
    /// Gets a query for the value of a live counter. Parameters: <c>@key</c>, <c>@now</c>.
    /// </summary>
    public abstract string CounterGet { get; }

    /// <summary>
    /// Gets a statement that deletes a counter and returns whether it was live.
    /// Parameters: <c>@key</c>, <c>@now</c>.
    /// </summary>
    public abstract string CounterDelete { get; }

    /// <summary>
    /// Gets a statement that creates or updates the row of a window and locks it until the
    /// transaction ends, so window operations on a key run one at a time.
    /// Parameters: <c>@key</c>, <c>@window_ms</c>.
    /// </summary>
    public abstract string WindowLock { get; }

    /// <summary>
    /// Gets a statement that deletes the events of a window at or before <c>@window_start</c>.
    /// Parameters: <c>@key</c>, <c>@window_start</c>.
    /// </summary>
    public abstract string WindowPrune { get; }

    /// <summary>
    /// Gets a query for the number of events of a window and the oldest event time.
    /// Parameter: <c>@key</c>.
    /// </summary>
    public abstract string WindowState { get; }

    /// <summary>
    /// Gets a statement that records an event at <c>@now</c>. Parameters: <c>@key</c>, <c>@now</c>.
    /// </summary>
    public abstract string WindowAdd { get; }

    /// <summary>
    /// Gets a query for the number of events after <c>@window_start</c> and the oldest of them.
    /// Parameters: <c>@key</c>, <c>@window_start</c>.
    /// </summary>
    public abstract string WindowSnapshot { get; }

    /// <summary>
    /// Gets the statements that delete expired rows and events that have left their window.
    /// Parameter: <c>@now</c>.
    /// </summary>
    public abstract IReadOnlyList<string> Purge { get; }

    /// <summary>
    /// Gets a query that succeeds when the database is reachable.
    /// </summary>
    public abstract string HealthCheck { get; }

    /// <summary>
    /// Gets a query for live documents, in order. Parameters: <c>@collection</c>, <c>@now</c>,
    /// and depending on the shape <c>@index_key</c>, <c>@sort_from</c>, <c>@sort_before</c>,
    /// <c>@limit</c>, <c>@offset</c>. Columns: id, value, version, index_key, sort_key, expires_at.
    /// </summary>
    /// <param name="shape">Which filters and paging the query uses.</param>
    /// <returns>The query.</returns>
    public abstract string DocumentQuery(DocumentQueryShape shape);

    /// <summary>
    /// Gets a query for the number of live matching documents. Parameters as for <see cref="DocumentQuery"/>.
    /// </summary>
    /// <param name="shape">Which filters the query uses.</param>
    /// <returns>The query.</returns>
    public abstract string DocumentCount(DocumentFilterShape shape);

    /// <summary>
    /// Gets a statement that deletes live matching documents. Parameters as for <see cref="DocumentQuery"/>.
    /// </summary>
    /// <param name="shape">Which filters the statement uses.</param>
    /// <returns>The statement.</returns>
    public abstract string DocumentDeleteMany(DocumentFilterShape shape);

    /// <summary>
    /// Lists every statement, including each document query shape, so tests can validate
    /// them all against a migrated database.
    /// </summary>
    /// <returns>Statement names and SQL.</returns>
    public IEnumerable<KeyValuePair<string, string>> All()
    {
        yield return new(nameof(DocumentGet), DocumentGet);
        yield return new(nameof(DocumentInsert), DocumentInsert);
        yield return new(nameof(DocumentReplace), DocumentReplace);
        yield return new(nameof(DocumentUpsert), DocumentUpsert);
        yield return new(nameof(DocumentDelete), DocumentDelete);
        yield return new(nameof(DocumentDeleteVersion), DocumentDeleteVersion);
        yield return new(nameof(QueueEnqueue), QueueEnqueue);
        yield return new(nameof(QueueClaim), QueueClaim);
        yield return new(nameof(QueueComplete), QueueComplete);
        yield return new(nameof(QueueAbandon), QueueAbandon);
        yield return new(nameof(QueueRemove), QueueRemove);
        yield return new(nameof(QueueCount), QueueCount);
        yield return new(nameof(QueueNextDue), QueueNextDue);
        yield return new(nameof(LeaseAcquire), LeaseAcquire);
        yield return new(nameof(LeaseRenew), LeaseRenew);
        yield return new(nameof(LeaseRelease), LeaseRelease);
        yield return new(nameof(LeaseGet), LeaseGet);
        yield return new(nameof(LeaseList), LeaseList);
        yield return new(nameof(CounterIncrement), CounterIncrement);
        yield return new(nameof(CounterGet), CounterGet);
        yield return new(nameof(CounterDelete), CounterDelete);
        yield return new(nameof(WindowLock), WindowLock);
        yield return new(nameof(WindowPrune), WindowPrune);
        yield return new(nameof(WindowState), WindowState);
        yield return new(nameof(WindowAdd), WindowAdd);
        yield return new(nameof(WindowSnapshot), WindowSnapshot);
        yield return new(nameof(HealthCheck), HealthCheck);

        for (var i = 0; i < Purge.Count; i++)
        {
            yield return new($"{nameof(Purge)}[{i}]", Purge[i]);
        }

        foreach (var filter in DocumentFilterShape.All)
        {
            yield return new($"{nameof(DocumentCount)}({filter})", DocumentCount(filter));
            yield return new($"{nameof(DocumentDeleteMany)}({filter})", DocumentDeleteMany(filter));
        }

        foreach (var query in DocumentQueryShape.All)
        {
            yield return new($"{nameof(DocumentQuery)}({query})", DocumentQuery(query));
        }
    }
}

/// <summary>
/// Which filters a document statement uses.
/// </summary>
/// <param name="IndexKey">Whether it filters by <c>@index_key</c>.</param>
/// <param name="SortKeyFrom">Whether it filters by <c>@sort_from</c>.</param>
/// <param name="SortKeyBefore">Whether it filters by <c>@sort_before</c>.</param>
public readonly record struct DocumentFilterShape(
    bool IndexKey,
    bool SortKeyFrom,
    bool SortKeyBefore
)
{
    internal static readonly bool[] Booleans = [false, true];

    /// <summary>
    /// Gets every combination of filters.
    /// </summary>
    public static IEnumerable<DocumentFilterShape> All =>
        from indexKey in Booleans
        from sortKeyFrom in Booleans
        from sortKeyBefore in Booleans
        select new DocumentFilterShape(indexKey, sortKeyFrom, sortKeyBefore);

    /// <summary>
    /// Gets the shape of a filter.
    /// </summary>
    /// <param name="filter">The filter.</param>
    /// <returns>The shape.</returns>
    public static DocumentFilterShape Of(DocumentFilter filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        return new(
            filter.IndexKey is not null,
            filter.SortKeyFrom is not null,
            filter.SortKeyBefore is not null
        );
    }
}

/// <summary>
/// Which filters, order, and paging a document query uses.
/// </summary>
/// <param name="Filter">The filters.</param>
/// <param name="Descending">Whether documents are returned in descending order.</param>
/// <param name="Limit">Whether it limits the rows to <c>@limit</c>.</param>
/// <param name="Offset">Whether it skips <c>@offset</c> rows.</param>
public readonly record struct DocumentQueryShape(
    DocumentFilterShape Filter,
    bool Descending,
    bool Limit,
    bool Offset
)
{
    /// <summary>
    /// Gets every combination of filters, order, and paging.
    /// </summary>
    public static IEnumerable<DocumentQueryShape> All =>
        from filter in DocumentFilterShape.All
        from isDescending in DocumentFilterShape.Booleans
        from limit in DocumentFilterShape.Booleans
        from offset in DocumentFilterShape.Booleans
        select new DocumentQueryShape(filter, isDescending, limit, offset);

    /// <summary>
    /// Gets the shape of a query.
    /// </summary>
    /// <param name="filter">The filter.</param>
    /// <param name="page">The paging.</param>
    /// <returns>The shape.</returns>
    public static DocumentQueryShape Of(DocumentFilter filter, DocumentPage? page) =>
        new(
            DocumentFilterShape.Of(filter),
            page?.Descending == true,
            page?.Limit is not null,
            page?.Offset > 0
        );
}
