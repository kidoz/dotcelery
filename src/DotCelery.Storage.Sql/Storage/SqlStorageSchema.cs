using DotCelery.Storage.Sql.Migrations;
using DotCelery.Storage.Sql.Schema;

namespace DotCelery.Storage.Sql.Storage;

/// <summary>
/// The tables that hold the storage primitives, identical in every SQL database.
/// </summary>
/// <remarks>
/// Applied migrations must never change. Schema changes are added as new migrations.
/// </remarks>
public static class SqlStorageSchema
{
    /// <summary>The migration module name.</summary>
    public const string ModuleName = "storage";

    /// <summary>The documents table.</summary>
    public const string DocumentsTable = "dotcelery_documents";

    /// <summary>The leases table.</summary>
    public const string LeasesTable = "dotcelery_leases";

    /// <summary>The queue items table.</summary>
    public const string QueueItemsTable = "dotcelery_queue_items";

    /// <summary>The counters table.</summary>
    public const string CountersTable = "dotcelery_counters";

    /// <summary>The sliding windows table, with one row per window key.</summary>
    public const string WindowKeysTable = "dotcelery_window_keys";

    /// <summary>The sliding window events table.</summary>
    public const string WindowEventsTable = "dotcelery_window_events";

    /// <summary>The sequence of document versions.</summary>
    public const string DocumentVersionSequence = "dotcelery_document_versions";

    /// <summary>The sequence of lease fencing tokens.</summary>
    public const string LeaseTokenSequence = "dotcelery_lease_tokens";

    /// <summary>
    /// Gets the migrations that create and evolve the storage tables.
    /// </summary>
    public static IReadOnlyList<SqlMigration> Migrations { get; } =
    [
        new SqlMigration(
            1,
            "Create storage tables",
            [
                new SchemaOperation.CreateSequence(DocumentVersionSequence),
                new SchemaOperation.CreateSequence(LeaseTokenSequence),
                new SchemaOperation.CreateTable(
                    new SqlTable(
                        DocumentsTable,
                        [
                            SqlColumn.Text("collection", 128),
                            SqlColumn.Text("id", 512),
                            SqlColumn.Binary("value"),
                            SqlColumn.Integer64("version"),
                            SqlColumn.Text("index_key", 512, nullable: true),
                            SqlColumn.Timestamp("sort_key", nullable: true),
                            SqlColumn.Timestamp("expires_at", nullable: true),
                        ],
                        ["collection", "id"]
                    )
                ),
                new SchemaOperation.CreateIndex(
                    DocumentsTable,
                    new SqlIndex(
                        "ix_dotcelery_documents_index",
                        ["collection", "index_key", "sort_key"]
                    )
                ),
                new SchemaOperation.CreateIndex(
                    DocumentsTable,
                    new SqlIndex("ix_dotcelery_documents_sort", ["collection", "sort_key"])
                ),
                new SchemaOperation.CreateIndex(
                    DocumentsTable,
                    new SqlIndex("ix_dotcelery_documents_expiry", ["expires_at"])
                ),
                new SchemaOperation.CreateTable(
                    new SqlTable(
                        LeasesTable,
                        [
                            SqlColumn.Text("key", 512),
                            SqlColumn.Text("owner", 512),
                            SqlColumn.Integer64("token"),
                            SqlColumn.Timestamp("expires_at"),
                        ],
                        ["key"]
                    )
                ),
                new SchemaOperation.CreateIndex(
                    LeasesTable,
                    new SqlIndex("ix_dotcelery_leases_expiry", ["expires_at"])
                ),
                new SchemaOperation.CreateTable(
                    new SqlTable(
                        QueueItemsTable,
                        [
                            SqlColumn.Text("queue", 128),
                            SqlColumn.Text("id", 512),
                            SqlColumn.Binary("payload"),
                            SqlColumn.Timestamp("due_at"),
                            SqlColumn.Integer32("delivery_count"),
                            SqlColumn.Uuid("claim_token", nullable: true),
                            SqlColumn.Timestamp("claimed_until", nullable: true),
                        ],
                        ["queue", "id"]
                    )
                ),
                new SchemaOperation.CreateIndex(
                    QueueItemsTable,
                    new SqlIndex("ix_dotcelery_queue_items_due", ["queue", "due_at"])
                ),
                new SchemaOperation.CreateTable(
                    new SqlTable(
                        CountersTable,
                        [
                            SqlColumn.Text("key", 512),
                            SqlColumn.Integer64("value"),
                            SqlColumn.Timestamp("expires_at", nullable: true),
                        ],
                        ["key"]
                    )
                ),
                new SchemaOperation.CreateIndex(
                    CountersTable,
                    new SqlIndex("ix_dotcelery_counters_expiry", ["expires_at"])
                ),
                new SchemaOperation.CreateTable(
                    new SqlTable(
                        WindowKeysTable,
                        [SqlColumn.Text("key", 512), SqlColumn.Integer64("window_ms")],
                        ["key"]
                    )
                ),
                new SchemaOperation.CreateTable(
                    new SqlTable(
                        WindowEventsTable,
                        [SqlColumn.Text("key", 512), SqlColumn.Timestamp("occurred_at")]
                    )
                ),
                new SchemaOperation.CreateIndex(
                    WindowEventsTable,
                    new SqlIndex("ix_dotcelery_window_events_key", ["key", "occurred_at"])
                ),
            ]
        ),
        new SqlMigration(
            2,
            "Record when leases were acquired",
            [
                new SchemaOperation.AddColumn(
                    LeasesTable,
                    SqlColumn.Timestamp("acquired_at", nullable: true)
                ),
            ]
        ),
    ];

    /// <summary>
    /// Creates the migration module for the storage tables in a schema.
    /// </summary>
    /// <param name="connectionString">The connection string of the database.</param>
    /// <param name="schema">The schema of the tables.</param>
    /// <returns>The migration module.</returns>
    public static SqlMigrationModule CreateModule(string connectionString, string schema) =>
        new(ModuleName, connectionString, schema, Migrations);
}
