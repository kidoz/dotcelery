using DotCelery.Storage.Sql.Schema;
using DotCelery.Storage.Sql.Storage;

namespace DotCelery.Backend.SqlServer.Storage;

/// <summary>
/// The storage primitives' statements for SQL Server.
/// </summary>
/// <remarks>
/// Upserts use <c>MERGE</c> with <c>UPDLOCK</c> and <c>HOLDLOCK</c> on the primary key, so
/// concurrent writers of a missing row queue up instead of both inserting it. Naming the index
/// keeps SQL Server from scanning a small table, which would lock all of it and deadlock. Sequence values are drawn before a <c>MERGE</c>, which cannot use them
/// directly. Text columns have a binary collation, so ids and keys compare and sort ordinally.
/// </remarks>
public sealed class SqlServerStorageStatements : SqlStorageStatements
{
    private const string LiveDocument = "(expires_at IS NULL OR expires_at > @now)";

    private readonly string _documents;

    /// <summary>
    /// Initializes a new instance of the <see cref="SqlServerStorageStatements"/> class.
    /// </summary>
    /// <param name="schema">The schema of the storage tables.</param>
    public SqlServerStorageStatements(string schema)
    {
        SqlIdentifier.ThrowIfInvalid(schema);

        var dialect = SqlServerDialect.Instance;
        _documents = dialect.Qualify(schema, SqlStorageSchema.DocumentsTable);
        var leases = dialect.Qualify(schema, SqlStorageSchema.LeasesTable);
        var queueItems = dialect.Qualify(schema, SqlStorageSchema.QueueItemsTable);
        var counters = dialect.Qualify(schema, SqlStorageSchema.CountersTable);
        var windowKeys = dialect.Qualify(schema, SqlStorageSchema.WindowKeysTable);
        var windowEvents = dialect.Qualify(schema, SqlStorageSchema.WindowEventsTable);
        var nextVersion =
            $"DECLARE @version bigint = NEXT VALUE FOR {dialect.Qualify(schema, SqlStorageSchema.DocumentVersionSequence)};";
        var nextToken =
            $"DECLARE @token bigint = NEXT VALUE FOR {dialect.Qualify(schema, SqlStorageSchema.LeaseTokenSequence)};";
        const string leaseColumns =
            "[key], owner, token, COALESCE(acquired_at, expires_at), expires_at";

        DocumentGet = $"""
            SELECT [value], version, index_key, sort_key, expires_at FROM {_documents}
            WHERE collection = @collection AND id = @id AND {LiveDocument}
            """;

        // Replacing an expired document counts as inserting it
        DocumentInsert = $"""
            {nextVersion}
            MERGE {_documents} WITH (UPDLOCK, HOLDLOCK, INDEX({Key(
                SqlStorageSchema.DocumentsTable
            )})) AS d
            USING (SELECT @collection AS collection, @id AS id) AS s
                ON d.collection = s.collection AND d.id = s.id
            WHEN MATCHED AND d.expires_at <= @now THEN UPDATE SET
                [value] = @value, version = @version, index_key = @index_key,
                sort_key = @sort_key, expires_at = @expires_at
            WHEN NOT MATCHED THEN
                INSERT (collection, id, [value], version, index_key, sort_key, expires_at)
                VALUES (@collection, @id, @value, @version, @index_key, @sort_key, @expires_at)
            OUTPUT inserted.version;
            """;

        DocumentReplace = $"""
            {nextVersion}
            UPDATE {_documents} SET
                [value] = @value, version = @version, index_key = @index_key,
                sort_key = @sort_key, expires_at = @expires_at
            OUTPUT inserted.version
            WHERE collection = @collection AND id = @id AND version = @expected_version AND {LiveDocument};
            """;

        DocumentUpsert = $"""
            {nextVersion}
            MERGE {_documents} WITH (UPDLOCK, HOLDLOCK, INDEX({Key(
                SqlStorageSchema.DocumentsTable
            )})) AS d
            USING (SELECT @collection AS collection, @id AS id) AS s
                ON d.collection = s.collection AND d.id = s.id
            WHEN MATCHED THEN UPDATE SET
                [value] = @value, version = @version, index_key = @index_key,
                sort_key = @sort_key, expires_at = @expires_at
            WHEN NOT MATCHED THEN
                INSERT (collection, id, [value], version, index_key, sort_key, expires_at)
                VALUES (@collection, @id, @value, @version, @index_key, @sort_key, @expires_at)
            OUTPUT inserted.version;
            """;

        DocumentDelete =
            $"DELETE FROM {_documents} WHERE collection = @collection AND id = @id AND {LiveDocument}";

        DocumentDeleteVersion = $"""
            DELETE FROM {_documents}
            WHERE collection = @collection AND id = @id AND version = @expected_version AND {LiveDocument}
            """;

        QueueEnqueue = $"""
            MERGE {queueItems} WITH (UPDLOCK, HOLDLOCK, INDEX({Key(
                SqlStorageSchema.QueueItemsTable
            )})) AS q
            USING (SELECT @queue AS queue, @id AS id) AS s ON q.queue = s.queue AND q.id = s.id
            WHEN MATCHED THEN UPDATE SET
                payload = @payload, due_at = @due_at,
                delivery_count = 0, claim_token = NULL, claimed_until = NULL
            WHEN NOT MATCHED THEN
                INSERT (queue, id, payload, due_at, delivery_count, claim_token, claimed_until)
                VALUES (@queue, @id, @payload, @due_at, 0, NULL, NULL);
            """;

        // READPAST lets concurrent consumers claim different items instead of waiting
        QueueClaim = $"""
            WITH claimable AS (
                SELECT TOP (@max_items) * FROM {queueItems} WITH (UPDLOCK, READPAST, ROWLOCK)
                WHERE queue = @queue AND due_at <= @now
                    AND (claimed_until IS NULL OR claimed_until <= @now)
                ORDER BY due_at, id
            )
            UPDATE claimable SET
                claim_token = @claim_token, claimed_until = @claimed_until,
                delivery_count = delivery_count + 1
            OUTPUT inserted.id, inserted.payload, inserted.due_at, inserted.delivery_count,
                inserted.claim_token, inserted.claimed_until;
            """;

        QueueComplete =
            $"DELETE FROM {queueItems} WHERE queue = @queue AND id = @id AND claim_token = @claim_token";

        QueueAbandon = $"""
            UPDATE {queueItems} SET claim_token = NULL, claimed_until = NULL, due_at = @due_at
            WHERE queue = @queue AND id = @id AND claim_token = @claim_token
            """;

        QueueRemove = $"DELETE FROM {queueItems} WHERE queue = @queue AND id = @id";

        QueueCount = $"SELECT COUNT_BIG(*) FROM {queueItems} WHERE queue = @queue";

        QueueNextDue = $"""
            SELECT MIN(due_at) FROM {queueItems}
            WHERE queue = @queue AND (claimed_until IS NULL OR claimed_until <= @now)
            """;

        // The same owner keeps its token and acquisition time while the lease is live; any new
        // holder gets new ones. acquired_at was added by a later migration, so it may be null.
        LeaseAcquire = $"""
            {nextToken}
            MERGE {leases} WITH (UPDLOCK, HOLDLOCK, INDEX({Key(SqlStorageSchema.LeasesTable)})) AS l
            USING (SELECT @key AS [key]) AS s ON l.[key] = s.[key]
            WHEN MATCHED AND (l.expires_at <= @now OR l.owner = @owner) THEN UPDATE SET
                token = CASE WHEN l.owner = @owner AND l.expires_at > @now
                    THEN l.token ELSE @token END,
                acquired_at = CASE WHEN l.owner = @owner AND l.expires_at > @now
                    THEN l.acquired_at ELSE @now END,
                owner = @owner,
                expires_at = @expires_at
            WHEN NOT MATCHED THEN
                INSERT ([key], owner, token, acquired_at, expires_at)
                VALUES (@key, @owner, @token, @now, @expires_at)
            OUTPUT inserted.[key], inserted.owner, inserted.token,
                COALESCE(inserted.acquired_at, inserted.expires_at), inserted.expires_at;
            """;

        LeaseRenew = $"""
            UPDATE {leases} SET expires_at = @expires_at
            OUTPUT inserted.[key], inserted.owner, inserted.token,
                COALESCE(inserted.acquired_at, inserted.expires_at), inserted.expires_at
            WHERE [key] = @key AND token = @token AND expires_at > @now
            """;

        LeaseRelease = $"DELETE FROM {leases} WHERE [key] = @key AND token = @token";

        LeaseGet = $"SELECT {leaseColumns} FROM {leases} WHERE [key] = @key AND expires_at > @now";

        // The prefix's own wildcard characters are escaped, so it matches literally
        LeaseList = $"""
            SELECT {leaseColumns} FROM {leases}
            WHERE [key] LIKE REPLACE(REPLACE(REPLACE(REPLACE(@prefix, N'\', N'\\'),
                    N'%', N'\%'), N'_', N'\_'), N'[', N'\[') + N'%' ESCAPE N'\'
                AND expires_at > @now
            ORDER BY [key]
            """;

        CounterIncrement = $"""
            MERGE {counters} WITH (UPDLOCK, HOLDLOCK, INDEX({Key(
                SqlStorageSchema.CountersTable
            )})) AS c
            USING (SELECT @key AS [key]) AS s ON c.[key] = s.[key]
            WHEN MATCHED THEN UPDATE SET
                [value] = CASE WHEN c.expires_at <= @now THEN @delta ELSE c.[value] + @delta END,
                expires_at = CASE WHEN c.expires_at <= @now THEN @expires_at ELSE c.expires_at END
            WHEN NOT MATCHED THEN
                INSERT ([key], [value], expires_at) VALUES (@key, @delta, @expires_at)
            OUTPUT inserted.[value];
            """;

        CounterGet =
            $"SELECT [value] FROM {counters} WHERE [key] = @key AND (expires_at IS NULL OR expires_at > @now)";

        CounterDelete = $"""
            DELETE FROM {counters}
            OUTPUT CAST(CASE WHEN deleted.expires_at IS NULL OR deleted.expires_at > @now
                THEN 1 ELSE 0 END AS bit)
            WHERE [key] = @key
            """;

        // The upsert locks the window row until the transaction ends
        WindowLock = $"""
            MERGE {windowKeys} WITH (UPDLOCK, HOLDLOCK, INDEX({Key(
                SqlStorageSchema.WindowKeysTable
            )})) AS w
            USING (SELECT @key AS [key]) AS s ON w.[key] = s.[key]
            WHEN MATCHED THEN UPDATE SET window_ms = @window_ms
            WHEN NOT MATCHED THEN INSERT ([key], window_ms) VALUES (@key, @window_ms);
            """;

        WindowPrune =
            $"DELETE FROM {windowEvents} WHERE [key] = @key AND occurred_at <= @window_start";

        WindowState =
            $"SELECT COUNT_BIG(*), MIN(occurred_at) FROM {windowEvents} WHERE [key] = @key";

        WindowAdd = $"INSERT INTO {windowEvents} ([key], occurred_at) VALUES (@key, @now)";

        WindowSnapshot =
            $"SELECT COUNT_BIG(*), MIN(occurred_at) FROM {windowEvents} WHERE [key] = @key AND occurred_at > @window_start";

        // DATEADD takes an int, so the window is added in seconds and the remaining milliseconds
        Purge =
        [
            $"DELETE FROM {_documents} WHERE expires_at <= @now",
            $"DELETE FROM {leases} WHERE expires_at <= @now",
            $"DELETE FROM {counters} WHERE expires_at <= @now",
            $"""
                DELETE e FROM {windowEvents} AS e
                JOIN {windowKeys} AS k ON e.[key] = k.[key]
                WHERE DATEADD(second, k.window_ms / 1000,
                    DATEADD(millisecond, k.window_ms % 1000, e.occurred_at)) <= @now
                """,
        ];
    }

    /// <inheritdoc />
    public override string DocumentGet { get; }

    /// <inheritdoc />
    public override string DocumentInsert { get; }

    /// <inheritdoc />
    public override string DocumentReplace { get; }

    /// <inheritdoc />
    public override string DocumentUpsert { get; }

    /// <inheritdoc />
    public override string DocumentDelete { get; }

    /// <inheritdoc />
    public override string DocumentDeleteVersion { get; }

    /// <inheritdoc />
    public override string QueueEnqueue { get; }

    /// <inheritdoc />
    public override string QueueClaim { get; }

    /// <inheritdoc />
    public override string QueueComplete { get; }

    /// <inheritdoc />
    public override string QueueAbandon { get; }

    /// <inheritdoc />
    public override string QueueRemove { get; }

    /// <inheritdoc />
    public override string QueueCount { get; }

    /// <inheritdoc />
    public override string QueueNextDue { get; }

    /// <inheritdoc />
    public override string LeaseAcquire { get; }

    /// <inheritdoc />
    public override string LeaseRenew { get; }

    /// <inheritdoc />
    public override string LeaseRelease { get; }

    /// <inheritdoc />
    public override string LeaseGet { get; }

    /// <inheritdoc />
    public override string LeaseList { get; }

    /// <inheritdoc />
    public override string CounterIncrement { get; }

    /// <inheritdoc />
    public override string CounterGet { get; }

    /// <inheritdoc />
    public override string CounterDelete { get; }

    /// <inheritdoc />
    public override string WindowLock { get; }

    /// <inheritdoc />
    public override string WindowPrune { get; }

    /// <inheritdoc />
    public override string WindowState { get; }

    /// <inheritdoc />
    public override string WindowAdd { get; }

    /// <inheritdoc />
    public override string WindowSnapshot { get; }

    /// <inheritdoc />
    public override IReadOnlyList<string> Purge { get; }

    /// <inheritdoc />
    public override string HealthCheck => "SELECT 1";

    /// <inheritdoc />
    public override string DocumentQuery(DocumentQueryShape shape)
    {
        // SQL Server sorts nulls first ascending and last descending, as the contract requires
        var order = shape.Descending
            ? "ORDER BY sort_key DESC, id DESC"
            : "ORDER BY sort_key ASC, id ASC";
        var paging = (shape.Offset, shape.Limit) switch
        {
            (false, false) => "",
            (true, false) => " OFFSET @offset ROWS",
            (false, true) => " OFFSET 0 ROWS FETCH NEXT @limit ROWS ONLY",
            (true, true) => " OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY",
        };

        return $"SELECT id, [value], version, index_key, sort_key, expires_at FROM {_documents} "
            + $"WHERE {Filter(shape.Filter)} {order}{paging}";
    }

    /// <inheritdoc />
    public override string DocumentCount(DocumentFilterShape shape) =>
        $"SELECT COUNT_BIG(*) FROM {_documents} WHERE {Filter(shape)}";

    /// <inheritdoc />
    public override string DocumentDeleteMany(DocumentFilterShape shape) =>
        $"DELETE FROM {_documents} WHERE {Filter(shape)}";

    private static string Key(string table) => $"[{SqlServerDialect.PrimaryKeyName(table)}]";

    private static string Filter(DocumentFilterShape shape) =>
        $"collection = @collection AND {LiveDocument}"
        + (shape.IndexKey ? " AND index_key = @index_key" : "")
        + (shape.SortKeyFrom ? " AND sort_key >= @sort_from" : "")
        + (shape.SortKeyBefore ? " AND sort_key < @sort_before" : "");
}
