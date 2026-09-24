using DotCelery.Backend.Postgres;
using DotCelery.Backend.Postgres.Batches;
using DotCelery.Backend.Postgres.DeadLetter;
using DotCelery.Backend.Postgres.DelayedMessageStore;
using DotCelery.Backend.Postgres.Execution;
using DotCelery.Backend.Postgres.Extensions;
using DotCelery.Backend.Postgres.Historical;
using DotCelery.Backend.Postgres.Metrics;
using DotCelery.Backend.Postgres.Migrations;
using DotCelery.Backend.Postgres.Outbox;
using DotCelery.Backend.Postgres.Partitioning;
using DotCelery.Backend.Postgres.RateLimiting;
using DotCelery.Backend.Postgres.Revocation;
using DotCelery.Backend.Postgres.Sagas;
using DotCelery.Backend.Postgres.Signals;
using DotCelery.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Testcontainers.PostgreSql;

namespace DotCelery.Tests.Integration.Postgres;

/// <summary>
/// Integration tests for PostgreSQL schema migrations using Testcontainers.
/// These tests require Docker to be running.
/// </summary>
[Collection("Postgres")]
public sealed class PostgresMigrationIntegrationTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:16-alpine")
        .WithDatabase("celery")
        .WithUsername("celery")
        .WithPassword("celery")
        .Build();

    private readonly PostgresDataSourceProvider _dataSources = new();
    private string _connectionString = string.Empty;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        _connectionString = _container.GetConnectionString();
    }

    public async ValueTask DisposeAsync()
    {
        await _dataSources.DisposeAsync();
        await _container.DisposeAsync();
    }

    [Fact]
    public async Task MigrateAsync_EmptyDatabase_CreatesEveryStoreTableInConfiguredSchema()
    {
        var (modules, tables) = AllStores("celery_jobs");

        var applied = await CreateMigrator(_dataSources, modules).MigrateAsync();

        Assert.Equal(modules.Count, applied);
        Assert.Equal(
            tables.Append("dotcelery_migrations").Order(),
            await GetTablesAsync("celery_jobs")
        );
        Assert.Equal(modules.Count, await CountHistoryAsync("celery_jobs"));
    }

    [Fact]
    public async Task MigrateAsync_AlreadyMigrated_AppliesNothing()
    {
        var (modules, _) = AllStores("public");
        await CreateMigrator(_dataSources, modules).MigrateAsync();

        var applied = await CreateMigrator(_dataSources, modules).MigrateAsync();

        Assert.Equal(0, applied);
    }

    [Fact]
    public async Task MigrateAsync_ConcurrentProcesses_ApplyEachMigrationOnce()
    {
        var (modules, _) = AllStores("public");
        var processes = Enumerable
            .Range(0, 5)
            .Select(_ => new PostgresDataSourceProvider())
            .ToList();

        try
        {
            // Separate data sources behave like separate processes starting together
            var applied = await Task.WhenAll(
                processes.Select(p => CreateMigrator(p, modules).MigrateAsync())
            );

            Assert.Equal(modules.Count, applied.Sum());
            Assert.Equal(modules.Count, await CountHistoryAsync("public"));
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
    public async Task MigrateAsync_AppliedMigrationChanged_Fails()
    {
        await CreateMigrator(_dataSources, [CustomModule("CREATE TABLE public.items (id INT)")])
            .MigrateAsync();

        var changed = CustomModule("CREATE TABLE public.items (id BIGINT)");

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            CreateMigrator(_dataSources, [changed]).MigrateAsync()
        );
        Assert.Contains("differs", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MigrateAsync_FailingStatement_RollsBackTheWholeMigration()
    {
        var failing = CustomModule(
            "CREATE TABLE public.items (id INT)",
            "SELECT * FROM public.missing_table"
        );

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            CreateMigrator(_dataSources, [failing]).MigrateAsync()
        );

        Assert.DoesNotContain("items", await GetTablesAsync("public"));
        Assert.Equal(0, await CountHistoryAsync("public"));
    }

    [Fact]
    public async Task GenerateScript_RunTwice_MatchesMigratedSchemaAndHistory()
    {
        var (scripted, _) = AllStores("scripted");
        var (migrated, _) = AllStores("migrated");
        var script = CreateMigrator(_dataSources, scripted).GenerateScript();

        // Running the script again applies nothing new
        await ExecuteAsync(script);
        await ExecuteAsync(script);
        await CreateMigrator(_dataSources, migrated).MigrateAsync();

        Assert.Equal(await GetColumnsAsync("migrated"), await GetColumnsAsync("scripted"));
        Assert.Equal(0, await CreateMigrator(_dataSources, scripted).MigrateAsync());
    }

    [Fact]
    public async Task AddPostgresStore_HostStarting_AppliesMigrationsBeforeStoreUse()
    {
        await using var services = CreateServices(runAtStartup: true);

        await StartHostedServicesAsync(services);

        var outbox = services.GetRequiredService<IOutboxStore>();
        Assert.Equal(0, await outbox.GetPendingCountAsync());
        Assert.Contains("celery_outbox", await GetTablesAsync("hosted"));
        Assert.Contains("celery_task_results", await GetTablesAsync("hosted"));
    }

    [Fact]
    public async Task AddPostgresStore_RunAtStartupDisabled_LeavesDatabaseUnchanged()
    {
        await using var services = CreateServices(runAtStartup: false);

        await StartHostedServicesAsync(services);

        Assert.Empty(await GetTablesAsync("hosted"));
    }

    private ServiceProvider CreateServices(bool runAtStartup)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddPostgresMigrations(o => o.RunAtStartup = runAtStartup);
        services.AddPostgresOutboxStore(o =>
        {
            o.ConnectionString = _connectionString;
            o.Schema = "hosted";
        });
        services.AddPostgresBackend(o =>
        {
            o.ConnectionString = _connectionString;
            o.Schema = "hosted";
        });

        return services.BuildServiceProvider();
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

    private (List<PostgresMigrationModule> Modules, List<string> Tables) AllStores(string schema)
    {
        var results = new PostgresBackendOptions
        {
            ConnectionString = _connectionString,
            Schema = schema,
        };
        var outbox = new PostgresOutboxStoreOptions
        {
            ConnectionString = _connectionString,
            Schema = schema,
        };
        var inbox = new PostgresInboxStoreOptions
        {
            ConnectionString = _connectionString,
            Schema = schema,
        };
        var deadLetters = new PostgresDeadLetterStoreOptions
        {
            ConnectionString = _connectionString,
            Schema = schema,
        };
        var delayed = new PostgresDelayedMessageStoreOptions
        {
            ConnectionString = _connectionString,
            Schema = schema,
        };
        var revocations = new PostgresRevocationStoreOptions
        {
            ConnectionString = _connectionString,
            Schema = schema,
        };
        var rateLimits = new PostgresRateLimiterOptions
        {
            ConnectionString = _connectionString,
            Schema = schema,
        };
        var signals = new PostgresSignalStoreOptions
        {
            ConnectionString = _connectionString,
            Schema = schema,
        };
        var batches = new PostgresBatchStoreOptions
        {
            ConnectionString = _connectionString,
            Schema = schema,
        };
        var sagas = new PostgresSagaStoreOptions
        {
            ConnectionString = _connectionString,
            Schema = schema,
        };
        var partitionLocks = new PostgresPartitionLockStoreOptions
        {
            ConnectionString = _connectionString,
            Schema = schema,
        };
        var executions = new PostgresTaskExecutionTrackerOptions
        {
            ConnectionString = _connectionString,
            Schema = schema,
        };
        var metrics = new PostgresQueueMetricsOptions
        {
            ConnectionString = _connectionString,
            Schema = schema,
        };
        var historical = new PostgresHistoricalDataStoreOptions
        {
            ConnectionString = _connectionString,
            Schema = schema,
        };

        return (
            [
                PostgresResultBackendMigrations.CreateModule(results),
                PostgresOutboxMigrations.CreateModule(outbox),
                PostgresInboxMigrations.CreateModule(inbox),
                PostgresDeadLetterMigrations.CreateModule(deadLetters),
                PostgresDelayedMessageMigrations.CreateModule(delayed),
                PostgresRevocationMigrations.CreateModule(revocations),
                PostgresRateLimiterMigrations.CreateModule(rateLimits),
                PostgresSignalMigrations.CreateModule(signals),
                PostgresBatchMigrations.CreateModule(batches),
                PostgresSagaMigrations.CreateModule(sagas),
                PostgresPartitionLockMigrations.CreateModule(partitionLocks),
                PostgresTaskExecutionTrackerMigrations.CreateModule(executions),
                PostgresQueueMetricsMigrations.CreateModule(metrics),
                PostgresHistoricalDataMigrations.CreateModule(historical),
            ],
            [
                results.TableName,
                outbox.TableName,
                inbox.TableName,
                deadLetters.TableName,
                delayed.TableName,
                revocations.TableName,
                rateLimits.TableName,
                signals.TableName,
                batches.BatchesTableName,
                batches.BatchTasksTableName,
                sagas.SagasTableName,
                sagas.SagaStepsTableName,
                sagas.TaskSagaTableName,
                partitionLocks.TableName,
                executions.TableName,
                metrics.MetricsTableName,
                metrics.RunningTasksTableName,
                historical.SnapshotsTableName,
            ]
        );
    }

    private PostgresMigrationModule CustomModule(params string[] statements) =>
        new(
            "test/items",
            _connectionString,
            "public",
            [new PostgresMigration(1, "Create items", statements)]
        );

    private static PostgresMigrator CreateMigrator(
        IPostgresDataSourceProvider dataSources,
        IEnumerable<PostgresMigrationModule> modules
    ) =>
        new(
            dataSources,
            modules,
            Options.Create(new PostgresMigrationOptions()),
            NullLogger<PostgresMigrator>.Instance
        );

    private async Task ExecuteAsync(string sql)
    {
        await using var command = _dataSources.GetDataSource(_connectionString).CreateCommand(sql);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<List<string>> GetTablesAsync(string schema) =>
        await QueryAsync(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = @schema ORDER BY table_name",
            schema,
            r => r.GetString(0)
        );

    private async Task<List<string>> GetColumnsAsync(string schema) =>
        await QueryAsync(
            """
            SELECT table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = @schema
            ORDER BY table_name, column_name
            """,
            schema,
            r => $"{r.GetString(0)}.{r.GetString(1)} {r.GetString(2)} {r.GetString(3)}"
        );

    private async Task<long> CountHistoryAsync(string schema)
    {
        var tables = await GetTablesAsync(schema);
        if (!tables.Contains("dotcelery_migrations"))
        {
            return 0;
        }

        await using var command = _dataSources
            .GetDataSource(_connectionString)
            .CreateCommand($"SELECT COUNT(*) FROM {schema}.dotcelery_migrations");
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private async Task<List<T>> QueryAsync<T>(
        string sql,
        string schema,
        Func<NpgsqlDataReader, T> read
    )
    {
        await using var command = _dataSources.GetDataSource(_connectionString).CreateCommand(sql);
        command.Parameters.AddWithValue("schema", schema);

        var rows = new List<T>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(read(reader));
        }

        return rows;
    }
}
