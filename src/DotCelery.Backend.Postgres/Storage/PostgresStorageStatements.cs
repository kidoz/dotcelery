using DotCelery.Storage.Sql.Schema;
using DotCelery.Storage.Sql.Storage;

namespace DotCelery.Backend.Postgres.Storage;

/// <summary>
/// The storage primitives in PostgreSQL SQL, for the tables of one schema.
/// </summary>
public sealed class PostgresStorageStatements : SqlStorageStatements
{
    // Ordinal order, so that every provider sorts ids and keys the same way
    private const string Ordinal = "COLLATE \"C\"";

    private const string LiveDocument = "(expires_at IS NULL OR expires_at > @now)";

    private readonly string _documents;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresStorageStatements"/> class.
    /// </summary>
    /// <param name="schema">The schema of the storage tables.</param>
    public PostgresStorageStatements(string schema)
    {
        SqlIdentifier.ThrowIfInvalid(schema);
        var dialect = PostgresDialect.Instance;

        _documents = dialect.Qualify(schema, SqlStorageSchema.DocumentsTable);
        var leases = dialect.Qualify(schema, SqlStorageSchema.LeasesTable);
        var queueItems = dialect.Qualify(schema, SqlStorageSchema.QueueItemsTable);
        var counters = dialect.Qualify(schema, SqlStorageSchema.CountersTable);
        var windowKeys = dialect.Qualify(schema, SqlStorageSchema.WindowKeysTable);
        var windowEvents = dialect.Qualify(schema, SqlStorageSchema.WindowEventsTable);
        var nextVersion =
            $"nextval('{dialect.Qualify(schema, SqlStorageSchema.DocumentVersionSequence)}')";
        var nextToken =
            $"nextval('{dialect.Qualify(schema, SqlStorageSchema.LeaseTokenSequence)}')";

        DocumentGet = $"""
            SELECT value, version, index_key, sort_key, expires_at FROM {_documents}
            WHERE collection = @collection AND id = @id AND {LiveDocument}
            """;

        // Replacing an expired document counts as inserting it
        DocumentInsert = $"""
            INSERT INTO {_documents} AS d (collection, id, value, version, index_key, sort_key, expires_at)
            VALUES (@collection, @id, @value, {nextVersion}, @index_key, @sort_key, @expires_at)
            ON CONFLICT (collection, id) DO UPDATE SET
                value = EXCLUDED.value, version = EXCLUDED.version, index_key = EXCLUDED.index_key,
                sort_key = EXCLUDED.sort_key, expires_at = EXCLUDED.expires_at
            WHERE d.expires_at <= @now
            RETURNING version
            """;

        DocumentReplace = $"""
            UPDATE {_documents} SET
                value = @value, version = {nextVersion}, index_key = @index_key,
                sort_key = @sort_key, expires_at = @expires_at
            WHERE collection = @collection AND id = @id AND version = @expected_version AND {LiveDocument}
            RETURNING version
            """;

        DocumentUpsert = $"""
            INSERT INTO {_documents} (collection, id, value, version, index_key, sort_key, expires_at)
            VALUES (@collection, @id, @value, {nextVersion}, @index_key, @sort_key, @expires_at)
            ON CONFLICT (collection, id) DO UPDATE SET
                value = EXCLUDED.value, version = EXCLUDED.version, index_key = EXCLUDED.index_key,
                sort_key = EXCLUDED.sort_key, expires_at = EXCLUDED.expires_at
            RETURNING version
            """;

        DocumentDelete =
            $"DELETE FROM {_documents} WHERE collection = @collection AND id = @id AND {LiveDocument}";

        DocumentDeleteVersion = $"""
            DELETE FROM {_documents}
            WHERE collection = @collection AND id = @id AND version = @expected_version AND {LiveDocument}
            """;

        QueueEnqueue = $"""
            INSERT INTO {queueItems} (queue, id, payload, due_at, delivery_count, claim_token, claimed_until)
            VALUES (@queue, @id, @payload, @due_at, 0, NULL, NULL)
            ON CONFLICT (queue, id) DO UPDATE SET
                payload = EXCLUDED.payload, due_at = EXCLUDED.due_at,
                delivery_count = 0, claim_token = NULL, claimed_until = NULL
            """;

        // SKIP LOCKED lets concurrent consumers claim different items instead of waiting
        QueueClaim = $"""
            WITH claimable AS (
                SELECT queue, id FROM {queueItems}
                WHERE queue = @queue AND due_at <= @now
                    AND (claimed_until IS NULL OR claimed_until <= @now)
                ORDER BY due_at, id {Ordinal}
                LIMIT @max_items
                FOR UPDATE SKIP LOCKED
            )
            UPDATE {queueItems} AS q SET
                claim_token = @claim_token, claimed_until = @claimed_until,
                delivery_count = q.delivery_count + 1
            FROM claimable AS c
            WHERE q.queue = c.queue AND q.id = c.id
            RETURNING q.id, q.payload, q.due_at, q.delivery_count, q.claim_token, q.claimed_until
            """;

        QueueComplete =
            $"DELETE FROM {queueItems} WHERE queue = @queue AND id = @id AND claim_token = @claim_token";

        QueueAbandon = $"""
            UPDATE {queueItems} SET claim_token = NULL, claimed_until = NULL, due_at = @due_at
            WHERE queue = @queue AND id = @id AND claim_token = @claim_token
            """;

        QueueRemove = $"DELETE FROM {queueItems} WHERE queue = @queue AND id = @id";

        QueueCount = $"SELECT COUNT(*) FROM {queueItems} WHERE queue = @queue";

        QueueNextDue = $"""
            SELECT MIN(due_at) FROM {queueItems}
            WHERE queue = @queue AND (claimed_until IS NULL OR claimed_until <= @now)
            """;

        // The same owner keeps its token while the lease is live; any new holder gets a new one
        LeaseAcquire = $"""
            INSERT INTO {leases} AS l (key, owner, token, expires_at)
            VALUES (@key, @owner, {nextToken}, @expires_at)
            ON CONFLICT (key) DO UPDATE SET
                token = CASE WHEN l.owner = EXCLUDED.owner AND l.expires_at > @now
                    THEN l.token ELSE EXCLUDED.token END,
                owner = EXCLUDED.owner,
                expires_at = EXCLUDED.expires_at
            WHERE l.expires_at <= @now OR l.owner = EXCLUDED.owner
            RETURNING key, owner, token, expires_at
            """;

        LeaseRenew = $"""
            UPDATE {leases} SET expires_at = @expires_at
            WHERE key = @key AND token = @token AND expires_at > @now
            RETURNING key, owner, token, expires_at
            """;

        LeaseRelease = $"DELETE FROM {leases} WHERE key = @key AND token = @token";

        LeaseGet =
            $"SELECT key, owner, token, expires_at FROM {leases} WHERE key = @key AND expires_at > @now";

        LeaseList = $"""
            SELECT key, owner, token, expires_at FROM {leases}
            WHERE starts_with(key, @prefix) AND expires_at > @now
            ORDER BY key {Ordinal}
            """;

        CounterIncrement = $"""
            INSERT INTO {counters} AS c (key, value, expires_at) VALUES (@key, @delta, @expires_at)
            ON CONFLICT (key) DO UPDATE SET
                value = CASE WHEN c.expires_at <= @now THEN EXCLUDED.value ELSE c.value + EXCLUDED.value END,
                expires_at = CASE WHEN c.expires_at <= @now THEN EXCLUDED.expires_at ELSE c.expires_at END
            RETURNING value
            """;

        CounterGet =
            $"SELECT value FROM {counters} WHERE key = @key AND (expires_at IS NULL OR expires_at > @now)";

        CounterDelete =
            $"DELETE FROM {counters} WHERE key = @key RETURNING (expires_at IS NULL OR expires_at > @now)";

        // The upsert locks the window row until the transaction ends
        WindowLock = $"""
            INSERT INTO {windowKeys} (key, window_ms) VALUES (@key, @window_ms)
            ON CONFLICT (key) DO UPDATE SET window_ms = EXCLUDED.window_ms
            """;

        WindowPrune =
            $"DELETE FROM {windowEvents} WHERE key = @key AND occurred_at <= @window_start";

        WindowState = $"SELECT COUNT(*), MIN(occurred_at) FROM {windowEvents} WHERE key = @key";

        WindowAdd = $"INSERT INTO {windowEvents} (key, occurred_at) VALUES (@key, @now)";

        WindowCount =
            $"SELECT COUNT(*) FROM {windowEvents} WHERE key = @key AND occurred_at > @window_start";

        Purge =
        [
            $"DELETE FROM {_documents} WHERE expires_at <= @now",
            $"DELETE FROM {leases} WHERE expires_at <= @now",
            $"DELETE FROM {counters} WHERE expires_at <= @now",
            $"""
                DELETE FROM {windowEvents} AS e USING {windowKeys} AS k
                WHERE e.key = k.key AND e.occurred_at + k.window_ms * INTERVAL '1 millisecond' <= @now
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
    public override string WindowCount { get; }

    /// <inheritdoc />
    public override IReadOnlyList<string> Purge { get; }

    /// <inheritdoc />
    public override string HealthCheck => "SELECT 1";

    /// <inheritdoc />
    public override string DocumentQuery(DocumentQueryShape shape)
    {
        var order = shape.Descending
            ? $"ORDER BY sort_key DESC NULLS LAST, id {Ordinal} DESC"
            : $"ORDER BY sort_key ASC NULLS FIRST, id {Ordinal} ASC";

        return $"SELECT id, value, version, index_key, sort_key, expires_at FROM {_documents} "
            + $"WHERE {Filter(shape.Filter)} {order}"
            + (shape.Limit ? " LIMIT @limit" : "")
            + (shape.Offset ? " OFFSET @offset" : "");
    }

    /// <inheritdoc />
    public override string DocumentCount(DocumentFilterShape shape) =>
        $"SELECT COUNT(*) FROM {_documents} WHERE {Filter(shape)}";

    /// <inheritdoc />
    public override string DocumentDeleteMany(DocumentFilterShape shape) =>
        $"DELETE FROM {_documents} WHERE {Filter(shape)}";

    private static string Filter(DocumentFilterShape shape) =>
        $"collection = @collection AND {LiveDocument}"
        + (shape.IndexKey ? " AND index_key = @index_key" : "")
        + (shape.SortKeyFrom ? " AND sort_key >= @sort_from" : "")
        + (shape.SortKeyBefore ? " AND sort_key < @sort_before" : "");
}
