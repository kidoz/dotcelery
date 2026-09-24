using DotCelery.Backend.Postgres;
using DotCelery.Backend.Postgres.Migrations;
using DotCelery.Backend.Postgres.Outbox;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Integration.Postgres;

/// <summary>
/// Tests for migration module validation and script generation. No database is needed.
/// </summary>
public sealed class PostgresMigrationModuleTests
{
    private const string ConnectionString = "Host=localhost;Database=celery";

    [Fact]
    public void Constructor_VersionsNotIncreasing_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            CreateModule(Migration(2, "CREATE TABLE a (id INT)"), Migration(1, "SELECT 1"))
        );
    }

    [Fact]
    public void Constructor_NonPositiveVersion_Throws()
    {
        Assert.Throws<ArgumentException>(() => CreateModule(Migration(0, "SELECT 1")));
    }

    [Fact]
    public void Constructor_EmptyStatement_Throws()
    {
        Assert.Throws<ArgumentException>(() => CreateModule(Migration(1, " ")));
    }

    [Theory]
    [InlineData("SELECT $dotcelery$x$dotcelery$")]
    [InlineData("SELECT $dotcelery_migration$x$dotcelery_migration$")]
    public void Constructor_ReservedDollarQuoteTag_Throws(string statement)
    {
        Assert.Throws<ArgumentException>(() => CreateModule(Migration(1, statement)));
    }

    [Fact]
    public void Constructor_InvalidSchema_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            new PostgresMigrationModule(
                "test/items",
                ConnectionString,
                "app; DROP SCHEMA public",
                [Migration(1, "SELECT 1")]
            )
        );
    }

    [Fact]
    public void GenerateScript_GuardsEachMigrationWithItsHistoryRecord()
    {
        var script = CreateMigrator(
                CreateModule(
                    Migration(1, "CREATE TABLE app.items (id INT)"),
                    Migration(2, "ALTER TABLE app.items ADD COLUMN name TEXT")
                )
            )
            .GenerateScript();

        Assert.Equal(1, Count(script, "CREATE TABLE IF NOT EXISTS app.dotcelery_migrations"));
        Assert.Equal(2, Count(script, "DO $dotcelery$"));
        Assert.Contains("module = 'test/items' AND version = 2", script, StringComparison.Ordinal);
    }

    [Fact]
    public void GenerateScript_EscapesQuotesInDescriptions()
    {
        var module = new PostgresMigrationModule(
            "test/items",
            ConnectionString,
            "app",
            [new PostgresMigration(1, "Create 'items' table", ["CREATE TABLE app.items (id INT)"])]
        );

        var script = CreateMigrator(module).GenerateScript();

        Assert.Contains("'Create ''items'' table'", script, StringComparison.Ordinal);
    }

    [Fact]
    public void GenerateScript_IgnoresLineEndingDifferences()
    {
        var unix = CreateModule(Migration(1, "CREATE TABLE app.items (\n    id INT\n)"));
        var windows = CreateModule(Migration(1, "CREATE TABLE app.items (\r\n    id INT\r\n)"));

        Assert.Equal(
            CreateMigrator(unix).GenerateScript(),
            CreateMigrator(windows).GenerateScript()
        );
    }

    [Fact]
    public void Migrator_SameModuleRegisteredTwice_KeepsOne()
    {
        var migrator = CreateMigrator(
            CreateModule(Migration(1, "SELECT 1")),
            CreateModule(Migration(1, "SELECT 1"))
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
            module.Migrations.SelectMany(m => m.Statements),
            s => s.Contains("CREATE TABLE IF NOT EXISTS jobs.my_outbox", StringComparison.Ordinal)
        );
    }

    private static PostgresMigration Migration(long version, string statement) =>
        new(version, $"Migration {version}", [statement]);

    private static PostgresMigrationModule CreateModule(params PostgresMigration[] migrations) =>
        new("test/items", ConnectionString, "app", migrations);

    private static PostgresMigrator CreateMigrator(params PostgresMigrationModule[] modules) =>
        new(
            new PostgresDataSourceProvider(),
            modules,
            Options.Create(new PostgresMigrationOptions()),
            NullLogger<PostgresMigrator>.Instance
        );

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
