using DotCelery.Backend.Postgres;
using DotCelery.Backend.Postgres.Outbox;
using DotCelery.Backend.Postgres.Storage;
using DotCelery.Storage.Sql.Migrations;
using DotCelery.Storage.Sql.Schema;
using DotCelery.Storage.Sql.Storage;

namespace DotCelery.Tests.Integration.Postgres;

/// <summary>
/// Tests for the SQL migration model and the PostgreSQL dialect. No database is needed.
/// </summary>
public sealed class SqlMigrationTests
{
    private const string ConnectionString = "Host=localhost;Database=celery";

    [Theory]
    [InlineData("has space")]
    [InlineData("1starts_with_digit")]
    [InlineData("semi;colon")]
    [InlineData("quote\"")]
    [InlineData("")]
    public void SqlIdentifier_InvalidName_IsRejected(string name)
    {
        Assert.False(SqlIdentifier.IsValid(name));
        Assert.Throws<ArgumentException>(() => SqlColumn.Text(name));
    }

    [Fact]
    public void SqlIdentifier_TooLong_IsRejected()
    {
        Assert.False(SqlIdentifier.IsValid(new string('a', SqlIdentifier.MaxLength + 1)));
        Assert.True(SqlIdentifier.IsValid(new string('a', SqlIdentifier.MaxLength)));
    }

    [Fact]
    public void SqlTable_PrimaryKeyThatIsNotAColumn_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            new SqlTable("items", [SqlColumn.Text("id")], ["missing"])
        );
    }

    [Fact]
    public void SqlTable_DuplicateColumns_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            new SqlTable("items", [SqlColumn.Text("id"), SqlColumn.Integer64("ID")])
        );
    }

    [Fact]
    public void AddColumn_NotNullable_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            new SchemaOperation.AddColumn("items", SqlColumn.Text("name"))
        );
    }

    [Fact]
    public void SqlMigration_WithoutOperations_Throws()
    {
        Assert.Throws<ArgumentException>(() => new SqlMigration(1, "Empty", []));
    }

    [Fact]
    public void SqlMigration_NonPositiveVersion_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new SqlMigration(0, "Zero", [new SchemaOperation.ExecuteSql("SELECT 1")])
        );
    }

    [Fact]
    public void SqlMigrationModule_VersionsNotIncreasing_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            CreateModule(Sql(2, "SELECT 1"), Sql(1, "SELECT 1"))
        );
    }

    [Fact]
    public void Checksum_IgnoresLineEndingsInSql()
    {
        Assert.Equal(
            Sql(1, "CREATE TABLE app.items (\n    id INT\n)").Checksum,
            Sql(1, "CREATE TABLE app.items (\r\n    id INT\r\n)").Checksum
        );
    }

    [Fact]
    public void Checksum_ChangesWhenAnOperationChanges()
    {
        var text = SqlColumn.Text("name", nullable: true);
        var longText = SqlColumn.Text("name", 100, nullable: true);

        Assert.NotEqual(
            new SqlMigration(
                1,
                "Add name",
                [new SchemaOperation.AddColumn("items", text)]
            ).Checksum,
            new SqlMigration(
                1,
                "Add name",
                [new SchemaOperation.AddColumn("items", longText)]
            ).Checksum
        );
    }

    [Fact]
    public void StorageMigrations_AreUnchanged()
    {
        // Applied migrations must never change. If this fails, restore the migration and
        // add a new one instead.
        Assert.Equal(
            "f9a5773811851465f9d7bd49fe170c715dd4d4be1170af5ba8f5d352e13d8dbd",
            Assert.Single(SqlStorageSchema.Migrations).Checksum
        );
    }

    [Fact]
    public void PostgresDialect_Quote_LowersAndQuotes()
    {
        Assert.Equal("\"myschema\"", PostgresDialect.Instance.Quote("MySchema"));
        Assert.Equal("\"order\"", PostgresDialect.Instance.Quote("order"));
    }

    [Fact]
    public void PostgresDialect_RendersSchemaOperations()
    {
        var dialect = PostgresDialect.Instance;
        var table = new SqlTable(
            "items",
            [
                SqlColumn.Text("id", 64),
                SqlColumn.Binary("payload", nullable: true),
                SqlColumn.Timestamp("created_at"),
            ],
            ["id"]
        );

        Assert.Equal(
            ["CREATE SEQUENCE \"app\".\"item_numbers\""],
            dialect.Render("app", new SchemaOperation.CreateSequence("item_numbers"))
        );
        Assert.Equal(
            [
                "CREATE TABLE \"app\".\"items\" (\n"
                    + "    \"id\" text NOT NULL,\n"
                    + "    \"payload\" bytea,\n"
                    + "    \"created_at\" timestamptz NOT NULL,\n"
                    + "    PRIMARY KEY (\"id\")\n"
                    + ")",
            ],
            dialect.Render("app", new SchemaOperation.CreateTable(table))
        );
        Assert.Equal(
            [
                "CREATE UNIQUE INDEX \"ix_items_created\" ON \"app\".\"items\" (\"created_at\", \"id\")",
            ],
            dialect.Render(
                "app",
                new SchemaOperation.CreateIndex(
                    "items",
                    new SqlIndex("ix_items_created", ["created_at", "id"], unique: true)
                )
            )
        );
        Assert.Equal(
            ["ALTER TABLE \"app\".\"items\" ADD COLUMN \"attempts\" integer"],
            dialect.Render(
                "app",
                new SchemaOperation.AddColumn(
                    "items",
                    SqlColumn.Integer32("attempts", nullable: true)
                )
            )
        );
    }

    [Fact]
    public void GenerateScript_GuardsEachMigrationWithItsHistoryRecord()
    {
        var script = CreateMigrator(
                CreateModule(
                    Sql(1, "CREATE TABLE app.items (id INT)"),
                    Sql(2, "ALTER TABLE app.items ADD COLUMN name TEXT")
                )
            )
            .GenerateScript();

        Assert.Equal(
            1,
            Count(script, "CREATE TABLE IF NOT EXISTS \"app\".\"dotcelery_migrations\"")
        );
        Assert.Equal(2, Count(script, "DO $dotcelery$"));
        Assert.Contains(
            "\"module\" = 'test/items' AND \"version\" = 2",
            script,
            StringComparison.Ordinal
        );
    }

    [Fact]
    public void GenerateScript_EscapesQuotesInDescriptions()
    {
        var module = new SqlMigrationModule(
            "test/items",
            ConnectionString,
            "app",
            [
                new SqlMigration(
                    1,
                    "Create 'items' table",
                    [new SchemaOperation.ExecuteSql("CREATE TABLE app.items (id INT)")]
                ),
            ]
        );

        var script = CreateMigrator(module).GenerateScript();

        Assert.Contains("'Create ''items'' table'", script, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("SELECT $dotcelery$x$dotcelery$")]
    [InlineData("SELECT $dotcelery_migration$x$dotcelery_migration$")]
    public void GenerateScript_ReservedDollarQuoteTag_Throws(string statement)
    {
        var migrator = CreateMigrator(CreateModule(Sql(1, statement)));

        Assert.Throws<InvalidOperationException>(() => migrator.GenerateScript());
    }

    [Fact]
    public void Migrator_SameModuleRegisteredTwice_KeepsOne()
    {
        var migrator = CreateMigrator(
            CreateModule(Sql(1, "SELECT 1")),
            CreateModule(Sql(1, "SELECT 1"))
        );

        Assert.Single(migrator.Modules);
    }

    [Fact]
    public void StoreModule_UsesConfiguredSchemaAndTableNames()
    {
        var module = PostgresOutboxMigrations.CreateModule(
            new PostgresOutboxStoreOptions
            {
                ConnectionString = ConnectionString,
                Schema = "jobs",
                TableName = "my_outbox",
            }
        );

        Assert.Equal("outbox/my_outbox", module.Name);
        Assert.Equal("jobs", module.Schema);
        Assert.Contains(
            module.Migrations.SelectMany(m => m.Operations).OfType<SchemaOperation.ExecuteSql>(),
            s =>
                s.Statement.Contains(
                    "CREATE TABLE IF NOT EXISTS jobs.my_outbox",
                    StringComparison.Ordinal
                )
        );
    }

    private static SqlMigration Sql(long version, string statement) =>
        new(version, $"Migration {version}", [new SchemaOperation.ExecuteSql(statement)]);

    private static SqlMigrationModule CreateModule(params SqlMigration[] migrations) =>
        new("test/items", ConnectionString, "app", migrations);

    private static SqlMigrator CreateMigrator(params SqlMigrationModule[] modules) =>
        PostgresTestMigrator.Create(new PostgresDataSourceProvider(), modules);

    private static int Count(string text, string value)
    {
        var count = 0;
        for (
            var index = text.IndexOf(value, StringComparison.Ordinal);
            index >= 0;
            index = text.IndexOf(value, index + value.Length, StringComparison.Ordinal)
        )
        {
            count++;
        }

        return count;
    }
}
