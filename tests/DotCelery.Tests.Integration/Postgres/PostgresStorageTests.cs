using System.Text.RegularExpressions;
using DotCelery.Backend.Postgres;
using DotCelery.Backend.Postgres.Storage;
using DotCelery.Core.Storage;
using DotCelery.Storage.Sql.Storage;
using DotCelery.Tests.Conformance.Storage;
using DotCelery.Tests.Conformance.Stores;
using Npgsql;
using NpgsqlTypes;
using Testcontainers.PostgreSql;

namespace DotCelery.Tests.Integration.Postgres;

/// <summary>
/// A PostgreSQL container with the storage tables migrated once for all storage tests.
/// Tests use unique names, so they can share the tables.
/// </summary>
public sealed class PostgresStorageFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:16-alpine")
        .WithDatabase("celery")
        .WithUsername("celery")
        .WithPassword("celery")
        .Build();

    public PostgresDataSourceProvider DataSources { get; } = new();

    public PostgresStorageOptions Options { get; private set; } = null!;

    public NpgsqlDataSource DataSource => DataSources.GetDataSource(Options.ConnectionString);

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();

        Options = new PostgresStorageOptions
        {
            ConnectionString = _container.GetConnectionString(),
            Schema = "storage_tests",
        };

        await PostgresTestMigrator
            .Create(DataSources, [PostgresStorage.CreateModule(Options)])
            .MigrateAsync();
    }

    public IStorageProvider CreateProvider(TimeProvider timeProvider) =>
        PostgresStorage.CreateProvider(DataSource, Options, timeProvider);

    public async ValueTask DisposeAsync()
    {
        await DataSources.DisposeAsync();
        await _container.DisposeAsync();
    }
}

[CollectionDefinition(Name)]
public sealed class PostgresStorageTestGroup : ICollectionFixture<PostgresStorageFixture>
{
    public const string Name = "PostgresStorage";
}

[Collection(PostgresStorageTestGroup.Name)]
public sealed class PostgresDocumentStoreTests(PostgresStorageFixture fixture)
    : DocumentStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(PostgresStorageTestGroup.Name)]
public sealed class PostgresLeaseStoreTests(PostgresStorageFixture fixture)
    : LeaseStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(PostgresStorageTestGroup.Name)]
public sealed class PostgresQueueStoreTests(PostgresStorageFixture fixture)
    : QueueStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(PostgresStorageTestGroup.Name)]
public sealed class PostgresCounterStoreTests(PostgresStorageFixture fixture)
    : CounterStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(PostgresStorageTestGroup.Name)]
public sealed class PostgresNotificationChannelTests(PostgresStorageFixture fixture)
    : NotificationChannelConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(PostgresStorageTestGroup.Name)]
public sealed class PostgresStorageProviderTests(PostgresStorageFixture fixture)
    : StorageProviderConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(PostgresStorageTestGroup.Name)]
public sealed class PostgresDelayedMessageStoreTests(PostgresStorageFixture fixture)
    : DelayedMessageStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(PostgresStorageTestGroup.Name)]
public sealed class PostgresOutboxStoreTests(PostgresStorageFixture fixture)
    : OutboxStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(PostgresStorageTestGroup.Name)]
public sealed class PostgresSignalStoreTests(PostgresStorageFixture fixture)
    : SignalStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(PostgresStorageTestGroup.Name)]
public sealed class PostgresRevocationStoreTests(PostgresStorageFixture fixture)
    : RevocationStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(PostgresStorageTestGroup.Name)]
public sealed class PostgresLeaseBackedStoreTests(PostgresStorageFixture fixture)
    : LeaseBackedStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(PostgresStorageTestGroup.Name)]
public sealed class PostgresWindowRateLimiterTests(PostgresStorageFixture fixture)
    : WindowRateLimiterConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

/// <summary>
/// Checks every storage statement against the migrated schema.
/// </summary>
[Collection(PostgresStorageTestGroup.Name)]
public sealed partial class PostgresStorageStatementTests(PostgresStorageFixture fixture)
{
    [Fact]
    public async Task EveryStatement_PreparesAgainstTheMigratedSchema()
    {
        var statements = new PostgresStorageStatements(fixture.Options.Schema).All().ToList();
        await using var connection = await fixture.DataSource.OpenConnectionAsync();
        var failures = new List<string>();

        foreach (var (name, sql) in statements)
        {
            // Preparing makes PostgreSQL parse and analyze the statement against the real
            // tables, without running it. Parameter types are left for the server to infer.
            await using var command = new NpgsqlCommand(sql, connection);
            foreach (var parameter in ParameterNames(sql))
            {
                command.Parameters.Add(
                    new NpgsqlParameter(parameter, NpgsqlDbType.Unknown) { Value = DBNull.Value }
                );
            }

            try
            {
                await command.PrepareAsync();
                await command.UnprepareAsync();
            }
            catch (PostgresException ex)
            {
                failures.Add($"{name}: {ex.MessageText}");
            }
        }

        Assert.Empty(failures);
        Assert.True(statements.Count > 100, $"Expected every query shape, got {statements.Count}.");
    }

    [Fact]
    public async Task Migrations_CreateTheStorageTables()
    {
        await using var command = fixture.DataSource.CreateCommand(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = @schema ORDER BY tablename"
        );
        command.Parameters.AddWithValue("schema", fixture.Options.Schema);

        var tables = new List<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            tables.Add(reader.GetString(0));
        }

        Assert.Equal(
            [
                SqlStorageSchema.CountersTable,
                SqlStorageSchema.DocumentsTable,
                SqlStorageSchema.LeasesTable,
                "dotcelery_migrations",
                SqlStorageSchema.QueueItemsTable,
                SqlStorageSchema.WindowEventsTable,
                SqlStorageSchema.WindowKeysTable,
            ],
            tables
        );
    }

    private static IEnumerable<string> ParameterNames(string sql) =>
        ParameterPattern().Matches(sql).Select(m => m.Groups[1].Value).Distinct();

    [GeneratedRegex(@"@(\w+)")]
    private static partial Regex ParameterPattern();
}
