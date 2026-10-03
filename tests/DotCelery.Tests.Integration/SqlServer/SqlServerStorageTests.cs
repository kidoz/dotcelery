using System.Data.Common;
using System.Text.RegularExpressions;
using DotCelery.Backend.SqlServer;
using DotCelery.Backend.SqlServer.Extensions;
using DotCelery.Backend.SqlServer.Storage;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Batches;
using DotCelery.Core.Execution;
using DotCelery.Core.Models;
using DotCelery.Core.Outbox;
using DotCelery.Core.Partitioning;
using DotCelery.Core.RateLimiting;
using DotCelery.Core.Sagas;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage;
using DotCelery.Core.Storage.Stores;
using DotCelery.Storage.Sql.Migrations;
using DotCelery.Storage.Sql.Schema;
using DotCelery.Storage.Sql.Storage;
using DotCelery.Tests.Conformance.Storage;
using DotCelery.Tests.Conformance.Stores;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Testcontainers.MsSql;

namespace DotCelery.Tests.Integration.SqlServer;

/// <summary>
/// A SQL Server container with the storage tables migrated once for all storage tests.
/// Tests use unique names, so they can share the tables.
/// </summary>
/// <remarks>
/// SQL Server images are x86-64 only; on ARM hosts Docker must emulate them.
/// </remarks>
public sealed class SqlServerStorageFixture : IAsyncLifetime
{
    private readonly MsSqlContainer _container = new MsSqlBuilder(
        "mcr.microsoft.com/mssql/server:2022-latest"
    ).Build();

    public SqlServerDataSourceProvider DataSources { get; } = new();

    public SqlServerStorageOptions Options { get; private set; } = null!;

    public string ConnectionString => Options.ConnectionString;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();

        var master = _container.GetConnectionString();
        await using (var connection = new SqlConnection(master))
        {
            await connection.OpenAsync();
            await using var command = new SqlCommand("CREATE DATABASE dotcelery_tests", connection);
            await command.ExecuteNonQueryAsync();
        }

        Options = new SqlServerStorageOptions
        {
            ConnectionString = new SqlConnectionStringBuilder(master)
            {
                InitialCatalog = "dotcelery_tests",
            }.ConnectionString,
            Schema = "storage_tests",
        };

        await CreateMigrator(DataSources, [SqlServerStorage.CreateModule(Options)]).MigrateAsync();
    }

    public IStorageProvider CreateProvider(TimeProvider timeProvider) =>
        SqlServerStorage.CreateProvider(
            DataSources.GetDataSource(Options.ConnectionString),
            Options,
            timeProvider
        );

    public static SqlMigrator CreateMigrator(
        SqlServerDataSourceProvider dataSources,
        IEnumerable<SqlMigrationModule> modules
    ) =>
        new(
            SqlServerDialect.Instance,
            dataSources,
            modules,
            Microsoft.Extensions.Options.Options.Create(new SqlMigrationOptions()),
            NullLogger<SqlMigrator>.Instance
        );

    public async ValueTask DisposeAsync()
    {
        await DataSources.DisposeAsync();
        await _container.DisposeAsync();
    }
}

[CollectionDefinition(Name)]
public sealed class SqlServerStorageTestGroup : ICollectionFixture<SqlServerStorageFixture>
{
    public const string Name = "SqlServerStorage";
}

/// <summary>
/// Checks every storage statement against the migrated schema.
/// </summary>
[Collection(SqlServerStorageTestGroup.Name)]
public sealed partial class SqlServerStorageStatementTests(SqlServerStorageFixture fixture)
{
    [Fact]
    public async Task EveryStatement_CompilesAgainstTheMigratedSchema()
    {
        var statements = new SqlServerStorageStatements(fixture.Options.Schema).All().ToList();
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        var failures = new List<string>();

        foreach (var (name, sql) in statements)
        {
            // SQL Server compiles the statement against the real tables without running it.
            // Parameters are declared with the types the stores bind them as.
            await using var command = new SqlCommand(
                "sys.sp_describe_undeclared_parameters",
                connection
            )
            {
                CommandType = System.Data.CommandType.StoredProcedure,
            };
            command.Parameters.AddWithValue("@tsql", sql);
            command.Parameters.AddWithValue("@params", Declarations(sql));

            try
            {
                await using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync()) { }
            }
            catch (SqlException ex)
            {
                failures.Add($"{name}: {ex.Message}");
            }
        }

        Assert.Empty(failures);
        Assert.True(statements.Count > 100, $"Expected every query shape, got {statements.Count}.");
    }

    private static readonly Dictionary<string, string> ParameterTypes = new(StringComparer.Ordinal)
    {
        ["collection"] = "nvarchar(512)",
        ["id"] = "nvarchar(512)",
        ["index_key"] = "nvarchar(512)",
        ["key"] = "nvarchar(512)",
        ["owner"] = "nvarchar(512)",
        ["prefix"] = "nvarchar(512)",
        ["queue"] = "nvarchar(512)",
        ["value"] = "varbinary(max)",
        ["payload"] = "varbinary(max)",
        ["limit"] = "int",
        ["max_items"] = "int",
        ["offset"] = "int",
        ["delta"] = "bigint",
        ["expected_version"] = "bigint",
        ["token"] = "bigint",
        ["window_ms"] = "bigint",
        ["claimed_until"] = "datetimeoffset(6)",
        ["due_at"] = "datetimeoffset(6)",
        ["expires_at"] = "datetimeoffset(6)",
        ["now"] = "datetimeoffset(6)",
        ["sort_before"] = "datetimeoffset(6)",
        ["sort_from"] = "datetimeoffset(6)",
        ["sort_key"] = "datetimeoffset(6)",
        ["window_start"] = "datetimeoffset(6)",
        ["claim_token"] = "uniqueidentifier",
    };

    // Variables the statement declares itself are not parameters
    private static string Declarations(string sql)
    {
        var declared = DeclaredVariable().Matches(sql).Select(m => m.Groups[1].Value).ToHashSet();
        return string.Join(
            ", ",
            Parameter()
                .Matches(sql)
                .Select(m => m.Groups[1].Value)
                .Distinct()
                .Where(name => !declared.Contains(name))
                .Select(name => $"@{name} {ParameterTypes[name]}")
        );
    }

    [GeneratedRegex(@"DECLARE @([a-z_]+)")]
    private static partial Regex DeclaredVariable();

    [GeneratedRegex(@"@([a-z_]+)")]
    private static partial Regex Parameter();

    [Fact]
    public async Task Migrations_CreateTheStorageTables()
    {
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(
            "SELECT t.name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id "
                + "WHERE s.name = @schema ORDER BY t.name",
            connection
        );
        command.Parameters.AddWithValue("@schema", fixture.Options.Schema);

        var tables = new List<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            tables.Add(reader.GetString(0));
        }

        Assert.Equal(
            [
                "dotcelery_counters",
                "dotcelery_documents",
                "dotcelery_leases",
                "dotcelery_migrations",
                "dotcelery_queue_items",
                "dotcelery_window_events",
                "dotcelery_window_keys",
            ],
            tables
        );
    }
}

/// <summary>
/// Migration behavior on SQL Server: idempotence, the migration lock, and generated scripts.
/// </summary>
[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerMigrationTests(SqlServerStorageFixture fixture)
{
    [Fact]
    public void Dialect_RendersNonclusteredKeysBinaryTextAndSequencesFromOne()
    {
        var dialect = SqlServerDialect.Instance;
        var table = new SqlTable(
            "items",
            [
                SqlColumn.Text("id", 512),
                SqlColumn.Text("note", nullable: true),
                SqlColumn.Timestamp("at"),
            ],
            ["id"]
        );

        var create = dialect.Render("app", new SchemaOperation.CreateTable(table)).Single();
        var sequence = dialect.Render("app", new SchemaOperation.CreateSequence("ids")).Single();
        var addColumn = dialect
            .Render(
                "app",
                new SchemaOperation.AddColumn("items", SqlColumn.Integer64("n", nullable: true))
            )
            .Single();

        Assert.Contains(
            "[id] nvarchar(512) COLLATE Latin1_General_100_BIN2 NOT NULL",
            create,
            StringComparison.Ordinal
        );
        Assert.Contains(
            "[note] nvarchar(max) COLLATE Latin1_General_100_BIN2,",
            create,
            StringComparison.Ordinal
        );
        Assert.Contains("[at] datetimeoffset(6) NOT NULL", create, StringComparison.Ordinal);
        Assert.Contains(
            "CONSTRAINT [pk_items] PRIMARY KEY NONCLUSTERED ([id])",
            create,
            StringComparison.Ordinal
        );
        Assert.Equal("CREATE SEQUENCE [app].[ids] AS bigint START WITH 1", sequence);
        Assert.Equal("ALTER TABLE [app].[items] ADD [n] bigint", addColumn);
    }

    [Fact]
    public async Task MigrateAsync_AlreadyMigrated_AppliesNothing()
    {
        var module = Module("again");
        await SqlServerStorageFixture.CreateMigrator(fixture.DataSources, [module]).MigrateAsync();

        Assert.Equal(
            0,
            await SqlServerStorageFixture
                .CreateMigrator(fixture.DataSources, [module])
                .MigrateAsync()
        );
    }

    [Fact]
    public async Task MigrateAsync_ConcurrentProcesses_ApplyEachMigrationOnce()
    {
        var module = Module("concurrent");
        var processes = Enumerable
            .Range(0, 5)
            .Select(_ => new SqlServerDataSourceProvider())
            .ToList();

        try
        {
            // Separate data sources behave like separate processes starting together
            var applied = await Task.WhenAll(
                processes.Select(p =>
                    SqlServerStorageFixture.CreateMigrator(p, [module]).MigrateAsync()
                )
            );

            Assert.Equal(module.Migrations.Count, applied.Sum());
            Assert.Equal(module.Migrations.Count, await CountHistoryAsync("concurrent"));
        }
        finally
        {
            foreach (var process in processes)
            {
                await process.DisposeAsync();
            }
        }
    }

    [Fact]
    public async Task GenerateScript_RunTwice_MatchesMigratedSchemaAndHistory()
    {
        var scripted = Module("scripted");
        var script = SqlServerStorageFixture
            .CreateMigrator(fixture.DataSources, [scripted])
            .GenerateScript();

        // Running the script again applies nothing new
        await ExecuteAsync(script);
        await ExecuteAsync(script);
        await SqlServerStorageFixture
            .CreateMigrator(fixture.DataSources, [Module("migrated")])
            .MigrateAsync();

        Assert.Equal(await GetColumnsAsync("migrated"), await GetColumnsAsync("scripted"));
        Assert.Equal(scripted.Migrations.Count, await CountHistoryAsync("scripted"));
        Assert.Equal(
            0,
            await SqlServerStorageFixture
                .CreateMigrator(fixture.DataSources, [scripted])
                .MigrateAsync()
        );
    }

    private SqlMigrationModule Module(string schema) =>
        SqlStorageSchema.CreateModule(fixture.ConnectionString, schema);

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<int> CountHistoryAsync(string schema)
    {
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(
            $"SELECT COUNT(*) FROM [{schema}].[dotcelery_migrations]",
            connection
        );
        return (int)(await command.ExecuteScalarAsync())!;
    }

    private async Task<List<string>> GetColumnsAsync(string schema)
    {
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(
            """
            SELECT t.name + '.' + c.name + ':' + ty.name + ':' + CAST(c.max_length AS nvarchar(10))
                + ':' + CAST(c.is_nullable AS nvarchar(1)) + ':' + COALESCE(c.collation_name, '')
            FROM sys.columns c
            JOIN sys.tables t ON t.object_id = c.object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            WHERE s.name = @schema AND t.name <> 'dotcelery_migrations'
            ORDER BY t.name, c.name
            """,
            connection
        );
        command.Parameters.AddWithValue("@schema", schema);

        var columns = new List<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            columns.Add(reader.GetString(0));
        }

        return columns;
    }
}

/// <summary>
/// Registration of every store on SQL Server, with migrations applied as the host starts.
/// </summary>
[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerStoreRegistrationTests(SqlServerStorageFixture fixture)
{
    [Fact]
    public async Task AddSqlServerStores_ShareOneStorageProviderAndWork()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IMessageBroker, RecordingBroker>();
        services.AddSingleton<IMessageSerializer, JsonMessageSerializer>();
        Action<SqlServerStorageOptions> configure = o =>
        {
            o.ConnectionString = fixture.ConnectionString;
            o.Schema = "shared";
        };
        services
            .AddSqlServerBackend(configure)
            .AddSqlServerBatchStore(configure)
            .AddSqlServerSagaStore(configure)
            .AddSqlServerDeadLetterStore(configure)
            .AddSqlServerQueueMetrics(configure)
            .AddSqlServerHistoricalDataStore(configure)
            .AddSqlServerDelayedMessageStore(configure)
            .AddSqlServerRevocationStore(configure)
            .AddSqlServerRateLimiter(configure)
            .AddSqlServerOutboxStore(configure)
            .AddSqlServerInboxStore(configure)
            .AddSqlServerSignalStore(configure)
            .AddSqlServerPartitionLockStore(configure)
            .AddSqlServerTaskExecutionTracker(configure);
        await using var provider = services.BuildServiceProvider();

        await StartHostedServicesAsync(provider);

        Assert.Single(provider.GetServices<IStorageProvider>());
        Assert.Single(provider.GetServices<IHostedService>().OfType<StoragePurgeService>());
        Assert.Single(provider.GetRequiredService<SqlMigrator>().Modules);
        await provider
            .GetRequiredService<IDelayedMessageStore>()
            .AddAsync(
                new TaskMessage
                {
                    Id = "task",
                    Task = "tests.task",
                    Args = [],
                    ContentType = "application/json",
                    Timestamp = DateTimeOffset.UtcNow,
                },
                DateTimeOffset.UtcNow.AddHours(1)
            );
        Assert.Equal(
            1,
            await provider.GetRequiredService<IDelayedMessageStore>().GetPendingCountAsync()
        );
        await provider.GetRequiredService<IRevocationStore>().RevokeAsync("task");
        Assert.True(await provider.GetRequiredService<IRevocationStore>().IsRevokedAsync("task"));
        Assert.True(
            (
                await provider
                    .GetRequiredService<IRateLimiter>()
                    .TryAcquireAsync("api", RateLimitPolicy.PerMinute(1))
            ).IsAcquired
        );
        Assert.Equal(0, await provider.GetRequiredService<IOutboxStore>().GetPendingCountAsync());
        await provider.GetRequiredService<IInboxStore>().MarkProcessedAsync("message");
        Assert.True(await provider.GetRequiredService<IInboxStore>().IsProcessedAsync("message"));
        Assert.Equal(0, await provider.GetRequiredService<ISignalStore>().GetPendingCountAsync());
        Assert.True(
            await provider
                .GetRequiredService<IPartitionLockStore>()
                .TryAcquireAsync("partition", "task", TimeSpan.FromMinutes(1))
        );
        Assert.True(
            await provider
                .GetRequiredService<ITaskExecutionTracker>()
                .TryStartAsync("tests.task", "task")
        );
        await provider
            .GetRequiredService<IResultBackend>()
            .UpdateStateAsync("task", TaskState.Started);
        Assert.Equal(
            TaskState.Started,
            await provider.GetRequiredService<IResultBackend>().GetStateAsync("task")
        );
        Assert.Null(await provider.GetRequiredService<IBatchStore>().GetAsync("batch"));
        Assert.Null(await provider.GetRequiredService<ISagaStore>().GetAsync("saga"));
        Assert.Equal(0, await provider.GetRequiredService<IDeadLetterStore>().GetCountAsync());
        await provider.GetRequiredService<IQueueMetrics>().RecordEnqueuedAsync("celery");
        Assert.Equal(
            1,
            await provider.GetRequiredService<IQueueMetrics>().GetWaitingCountAsync("celery")
        );
        Assert.Equal(
            0,
            await provider.GetRequiredService<IHistoricalDataStore>().GetSnapshotCountAsync()
        );
    }

    private static async Task StartHostedServicesAsync(IServiceProvider services)
    {
        foreach (
            var service in services.GetServices<IHostedService>().OfType<IHostedLifecycleService>()
        )
        {
            await service.StartingAsync(CancellationToken.None);
        }
    }
}

[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerDocumentStoreTests(SqlServerStorageFixture fixture)
    : DocumentStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerLeaseStoreTests(SqlServerStorageFixture fixture)
    : LeaseStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerQueueStoreTests(SqlServerStorageFixture fixture)
    : QueueStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerCounterStoreTests(SqlServerStorageFixture fixture)
    : CounterStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerNotificationChannelTests(SqlServerStorageFixture fixture)
    : NotificationChannelConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerStorageProviderTests(SqlServerStorageFixture fixture)
    : StorageProviderConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerDelayedMessageStoreTests(SqlServerStorageFixture fixture)
    : DelayedMessageStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerOutboxStoreTests(SqlServerStorageFixture fixture)
    : OutboxStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerSignalStoreTests(SqlServerStorageFixture fixture)
    : SignalStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerRevocationStoreTests(SqlServerStorageFixture fixture)
    : RevocationStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerLeaseBackedStoreTests(SqlServerStorageFixture fixture)
    : LeaseBackedStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerWindowRateLimiterTests(SqlServerStorageFixture fixture)
    : WindowRateLimiterConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerResultBackendTests(SqlServerStorageFixture fixture)
    : ResultBackendConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerBatchStoreTests(SqlServerStorageFixture fixture)
    : BatchStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerSagaStoreTests(SqlServerStorageFixture fixture)
    : SagaStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerDeadLetterStoreTests(SqlServerStorageFixture fixture)
    : DeadLetterStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerQueueMetricsTests(SqlServerStorageFixture fixture)
    : QueueMetricsConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerHistoricalDataStoreTests(SqlServerStorageFixture fixture)
    : HistoricalDataStoreConformanceTests
{
    protected override ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider) =>
        ValueTask.FromResult(fixture.CreateProvider(timeProvider));
}

/// <summary>
/// Checks that the outbox and inbox join a transaction the caller owns.
/// </summary>
[Collection(SqlServerStorageTestGroup.Name)]
public sealed class SqlServerTransactionalStorageTests(SqlServerStorageFixture fixture)
{
    private readonly StorageStoreOptions _storeOptions = new()
    {
        Prefix = $"txn-{Guid.NewGuid():N}",
    };

    [Fact]
    public async Task OutboxMessage_StoredInTheCallersTransaction_IsVisibleAfterItCommits()
    {
        var outbox = CreateOutbox(fixture.CreateProvider(TimeProvider.System));

        await using (var connection = await OpenConnectionAsync())
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            await outbox.StoreAsync(CreateMessage("committed"), transaction);
            await transaction.CommitAsync();
        }

        Assert.Equal(1, await outbox.GetPendingCountAsync());
    }

    [Fact]
    public async Task OutboxMessage_StoredInTheCallersTransaction_IsDiscardedWhenItRollsBack()
    {
        var outbox = CreateOutbox(fixture.CreateProvider(TimeProvider.System));

        await using (var connection = await OpenConnectionAsync())
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            await outbox.StoreAsync(CreateMessage("rolled-back"), transaction);
            await transaction.RollbackAsync();
        }

        Assert.Equal(0, await outbox.GetPendingCountAsync());
    }

    [Fact]
    public async Task InboxRecord_MarkedInTheCallersTransaction_FollowsItsOutcome()
    {
        var inbox = new InboxStore(
            fixture.CreateProvider(TimeProvider.System),
            Options.Create(_storeOptions)
        );

        await using (var connection = await OpenConnectionAsync())
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            await inbox.MarkProcessedAsync("rolled-back", transaction);
            await transaction.RollbackAsync();
        }

        Assert.False(await inbox.IsProcessedAsync("rolled-back"));

        await using (var connection = await OpenConnectionAsync())
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            await inbox.MarkProcessedAsync("committed", transaction);
            await transaction.CommitAsync();
        }

        Assert.True(await inbox.IsProcessedAsync("committed"));
    }

    [Fact]
    public async Task RunInTransactionAsync_WhenTheWorkSucceeds_CommitsEveryWrite()
    {
        var provider = fixture.CreateProvider(TimeProvider.System);
        var documents = provider.Documents;
        var collection = $"{_storeOptions.Prefix}.txn-commit";

        await ((ITransactionalStorage)provider).RunInTransactionAsync(async token =>
            await documents.UpsertAsync(
                collection,
                "a",
                ReadOnlyMemory<byte>.Empty,
                cancellationToken: token
            )
        );

        Assert.NotNull(await documents.GetAsync(collection, "a"));
    }

    [Fact]
    public async Task RunInTransactionAsync_WhenTheWorkFails_RollsBackEveryWrite()
    {
        var provider = fixture.CreateProvider(TimeProvider.System);
        var documents = provider.Documents;
        var collection = $"{_storeOptions.Prefix}.txn-rollback";

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await ((ITransactionalStorage)provider).RunInTransactionAsync(async token =>
            {
                await documents.UpsertAsync(
                    collection,
                    "a",
                    ReadOnlyMemory<byte>.Empty,
                    cancellationToken: token
                );
                throw new InvalidOperationException("The work failed");
            })
        );

        Assert.Null(await documents.GetAsync(collection, "a"));
    }

    [Fact]
    public async Task OutcomeRecording_StoresTheResultAndTheInboxRecordTogether()
    {
        var provider = fixture.CreateProvider(TimeProvider.System);
        var results = new ResultBackend(provider, Options.Create(_storeOptions));
        var inbox = new InboxStore(provider, Options.Create(_storeOptions));
        var recorder = OutcomeRecorder.Create(results, inbox)!;

        await recorder.RecordAsync(Success("recorded"));

        Assert.Equal(TaskState.Success, (await results.GetResultAsync("recorded"))!.State);
        Assert.True(await inbox.IsProcessedAsync("recorded"));
    }

    [Fact]
    public async Task OutcomeRecording_WhenTheInboxWriteFails_LeavesNoResult()
    {
        var provider = fixture.CreateProvider(TimeProvider.System);
        var results = new ResultBackend(provider, Options.Create(_storeOptions));
        var recorder = OutcomeRecorder.Create(results, new FailingInboxStore(provider))!;

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await recorder.RecordAsync(Success("rolled-back"))
        );

        Assert.Null(await results.GetResultAsync("rolled-back"));
    }

    private static TaskResult Success(string taskId) =>
        new()
        {
            TaskId = taskId,
            State = TaskState.Success,
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromMilliseconds(1),
        };

    private OutboxStore CreateOutbox(IStorageProvider provider) =>
        new(provider, Options.Create(_storeOptions));

    private sealed class FailingInboxStore(IStorageProvider storage)
        : IInboxStore,
            IStorageBackedStore
    {
        public IStorageProvider Storage { get; } = storage;

        public ValueTask<bool> IsProcessedAsync(
            string messageId,
            CancellationToken cancellationToken = default
        ) => ValueTask.FromResult(false);

        public ValueTask MarkProcessedAsync(
            string messageId,
            object? transaction = null,
            CancellationToken cancellationToken = default
        ) => throw new InvalidOperationException("The inbox store is unavailable");

        public ValueTask<long> GetCountAsync(CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(0L);

        public ValueTask<long> CleanupAsync(
            TimeSpan olderThan,
            CancellationToken cancellationToken = default
        ) => ValueTask.FromResult(0L);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private async Task<DbConnection> OpenConnectionAsync() =>
        await fixture
            .DataSources.GetDataSource(fixture.Options.ConnectionString)
            .OpenConnectionAsync();

    private static OutboxMessage CreateMessage(string id) =>
        new()
        {
            Id = id,
            TaskMessage = new TaskMessage
            {
                Id = id,
                Task = "tests.task",
                Args = [1, 2, 3],
                ContentType = "application/json",
                Timestamp = DateTimeOffset.UtcNow,
            },
            CreatedAt = DateTimeOffset.UtcNow,
        };
}
