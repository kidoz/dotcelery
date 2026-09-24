using System.Diagnostics;
using System.Globalization;
using System.Text;
using DotCelery.Storage.Sql.Execution;
using DotCelery.Storage.Sql.Schema;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Storage.Sql.Migrations;

/// <summary>
/// Applies the registered <see cref="SqlMigrationModule"/>s.
/// </summary>
/// <remarks>
/// <para>
/// Migrations for each database run under a database lock, so processes that start at the
/// same time apply each migration once. Each migration runs in its own transaction together
/// with its history record.
/// </para>
/// <para>
/// The history stores a checksum of each applied migration. Migrating fails if an applied
/// migration no longer matches its definition: applied migrations must not change, and
/// schema changes are added as new migrations.
/// </para>
/// </remarks>
public sealed class SqlMigrator
{
    // "DotCelry" in ASCII, to tell this lock apart from the application's locks
    private const long LockKey = 0x446F7443656C7279;

    private static readonly TimeSpan LockPollInterval = TimeSpan.FromMilliseconds(500);

    private readonly SqlDialect _dialect;
    private readonly ISqlDataSourceProvider _dataSources;
    private readonly SqlMigrationOptions _options;
    private readonly ILogger<SqlMigrator> _logger;
    private readonly TimeProvider _timeProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="SqlMigrator"/> class.
    /// </summary>
    /// <param name="dialect">The SQL dialect of the databases.</param>
    /// <param name="dataSources">Provides the data source for each connection string.</param>
    /// <param name="modules">The migration modules to apply.</param>
    /// <param name="options">The migration options.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="timeProvider">The clock for history records.</param>
    public SqlMigrator(
        SqlDialect dialect,
        ISqlDataSourceProvider dataSources,
        IEnumerable<SqlMigrationModule> modules,
        IOptions<SqlMigrationOptions> options,
        ILogger<SqlMigrator> logger,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(dialect);
        ArgumentNullException.ThrowIfNull(dataSources);
        ArgumentNullException.ThrowIfNull(modules);
        ArgumentNullException.ThrowIfNull(options);

        _dialect = dialect;
        _dataSources = dataSources;
        _options = options.Value;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;

        // Registering the same component twice yields identical modules
        Modules = modules.DistinctBy(m => (m.ConnectionString, m.Schema, m.Name)).ToList();
    }

    /// <summary>
    /// Gets the migration modules this migrator applies.
    /// </summary>
    public IReadOnlyList<SqlMigrationModule> Modules { get; }

    private SqlTable HistoryTable =>
        new(
            _options.HistoryTableName,
            [
                SqlColumn.Text("module", 256),
                SqlColumn.Integer64("version"),
                SqlColumn.Text("description"),
                SqlColumn.Text("checksum", 64),
                SqlColumn.Timestamp("applied_at"),
            ],
            ["module", "version"]
        );

    /// <summary>
    /// Applies all pending migrations.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of migrations applied.</returns>
    public async Task<int> MigrateAsync(CancellationToken cancellationToken = default)
    {
        var applied = 0;

        foreach (var database in Modules.GroupBy(m => m.ConnectionString, StringComparer.Ordinal))
        {
            applied += await MigrateDatabaseAsync(database.Key, [.. database], cancellationToken)
                .ConfigureAwait(false);
        }

        if (applied > 0)
        {
            _logger.LogInformation(
                "Applied {Count} {Database} migration(s)",
                applied,
                _dialect.Name
            );
        }
        else
        {
            _logger.LogDebug("{Database} schema is up to date", _dialect.Name);
        }

        return applied;
    }

    /// <summary>
    /// Generates a SQL script with every migration, for databases where schema changes are
    /// applied by hand. The script applies only migrations that are not recorded yet, so it
    /// can be run again after an upgrade.
    /// </summary>
    /// <remarks>
    /// The script covers all modules. If modules use different connection strings, run each
    /// module's section against its own database.
    /// </remarks>
    /// <returns>The SQL script.</returns>
    public string GenerateScript()
    {
        var script = new StringBuilder();
        script.AppendLine(CultureInfo.InvariantCulture, $"-- DotCelery {_dialect.Name} schema");
        script.AppendLine(
            "-- Applies only migrations that are not recorded yet, so it can be run again."
        );

        var bootstrappedSchemas = new HashSet<string>(StringComparer.Ordinal);

        foreach (var module in Modules)
        {
            script.AppendLine();
            script.AppendLine(
                CultureInfo.InvariantCulture,
                $"-- Module {module.Name} (schema {module.Schema})"
            );

            if (bootstrappedSchemas.Add(module.Schema))
            {
                script.Append(_dialect.RenderCreateSchema(module.Schema, ifNotExists: true));
                script.AppendLine(";");
                script.Append(
                    _dialect.RenderCreateTable(module.Schema, HistoryTable, ifNotExists: true)
                );
                script.AppendLine(";");
            }

            foreach (var migration in module.Migrations)
            {
                script.AppendLine(
                    _dialect.RenderGuardedMigration(
                        module.Schema,
                        _options.HistoryTableName,
                        module.Name,
                        migration
                    )
                );
            }
        }

        return script.ToString();
    }

    private async Task<int> MigrateDatabaseAsync(
        string connectionString,
        IReadOnlyList<SqlMigrationModule> modules,
        CancellationToken cancellationToken
    )
    {
        var executor = new SqlExecutor(
            _dataSources.GetDataSource(connectionString),
            _options.CommandTimeout
        );
        await using var session = await executor
            .OpenSessionAsync(cancellationToken)
            .ConfigureAwait(false);

        await AcquireLockAsync(session, cancellationToken).ConfigureAwait(false);

        try
        {
            var applied = 0;
            foreach (var module in modules)
            {
                applied += await MigrateModuleAsync(session, module, cancellationToken)
                    .ConfigureAwait(false);
            }

            return applied;
        }
        finally
        {
            await ReleaseLockAsync(session).ConfigureAwait(false);
        }
    }

    private async Task<int> MigrateModuleAsync(
        SqlSession session,
        SqlMigrationModule module,
        CancellationToken cancellationToken
    )
    {
        await EnsureHistoryTableAsync(session, module.Schema, cancellationToken)
            .ConfigureAwait(false);

        var history = (
            await session
                .QueryAsync(
                    _dialect.RenderSelectMigrations(module.Schema, _options.HistoryTableName),
                    p => p.Text("module", module.Name),
                    r => (Version: r.GetInt64(0), Checksum: r.GetString(1)),
                    cancellationToken
                )
                .ConfigureAwait(false)
        ).ToDictionary(h => h.Version, h => h.Checksum);

        var applied = 0;

        foreach (var migration in module.Migrations)
        {
            var checksum = migration.Checksum;

            if (history.TryGetValue(migration.Version, out var appliedChecksum))
            {
                if (!string.Equals(appliedChecksum, checksum, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(
                        $"Migration {migration.Version} of module '{module.Name}' in schema "
                            + $"'{module.Schema}' differs from the migration that was applied. "
                            + "Applied migrations must not change; add a new migration instead."
                    );
                }

                continue;
            }

            await ApplyAsync(session, module, migration, cancellationToken).ConfigureAwait(false);
            applied++;
        }

        var unknownVersions = history
            .Keys.Where(v => module.Migrations.All(m => m.Version != v))
            .Order()
            .ToList();

        if (unknownVersions.Count > 0)
        {
            _logger.LogWarning(
                "Module {Module} in schema {Schema} has migrations {Versions} that this version does not define; a newer version migrated the database",
                module.Name,
                module.Schema,
                string.Join(", ", unknownVersions)
            );
        }

        return applied;
    }

    private async Task ApplyAsync(
        SqlSession session,
        SqlMigrationModule module,
        SqlMigration migration,
        CancellationToken cancellationToken
    )
    {
        _logger.LogInformation(
            "Applying migration {Version} of {Module} in schema {Schema}: {Description}",
            migration.Version,
            module.Name,
            module.Schema,
            migration.Description
        );

        try
        {
            await session
                .InTransactionAsync(
                    async () =>
                    {
                        foreach (var operation in migration.Operations)
                        {
                            foreach (var statement in _dialect.Render(module.Schema, operation))
                            {
                                await session
                                    .ExecuteAsync(statement, bind: null, cancellationToken)
                                    .ConfigureAwait(false);
                            }
                        }

                        return await session
                            .ExecuteAsync(
                                _dialect.RenderInsertMigration(
                                    module.Schema,
                                    _options.HistoryTableName
                                ),
                                p =>
                                    p.Text("module", module.Name)
                                        .Integer64("version", migration.Version)
                                        .Text("description", migration.Description)
                                        .Text("checksum", migration.Checksum)
                                        .Timestamp("applied_at", _timeProvider.GetUtcNow()),
                                cancellationToken
                            )
                            .ConfigureAwait(false);
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new InvalidOperationException(
                $"Failed to apply migration {migration.Version} ({migration.Description}) of "
                    + $"module '{module.Name}' in schema '{module.Schema}'.",
                ex
            );
        }
    }

    private async Task EnsureHistoryTableAsync(
        SqlSession session,
        string schema,
        CancellationToken cancellationToken
    )
    {
        // Create only what is missing, so an up-to-date database needs no DDL privileges
        var schemaExists = await session
            .QuerySingleAsync(
                _dialect.SchemaExistsQuery,
                p => p.Text("schema", schema),
                r => r.GetBoolean(0),
                cancellationToken
            )
            .ConfigureAwait(false);

        if (!schemaExists)
        {
            await session
                .ExecuteAsync(
                    _dialect.RenderCreateSchema(schema, ifNotExists: true),
                    bind: null,
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        var tableExists = await session
            .QuerySingleAsync(
                _dialect.TableExistsQuery,
                p => p.Text("schema", schema).Text("table", _options.HistoryTableName),
                r => r.GetBoolean(0),
                cancellationToken
            )
            .ConfigureAwait(false);

        if (!tableExists)
        {
            await session
                .ExecuteAsync(
                    _dialect.RenderCreateTable(schema, HistoryTable, ifNotExists: true),
                    bind: null,
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
    }

    private async Task AcquireLockAsync(SqlSession session, CancellationToken cancellationToken)
    {
        var waited = Stopwatch.StartNew();
        var loggedWait = false;

        while (
            !await session
                .QuerySingleAsync(
                    _dialect.TryAcquireMigrationLockQuery,
                    p => p.Integer64("key", LockKey),
                    r => r.GetBoolean(0),
                    cancellationToken
                )
                .ConfigureAwait(false)
        )
        {
            if (waited.Elapsed >= _options.LockTimeout)
            {
                throw new TimeoutException(
                    $"Timed out after {_options.LockTimeout} waiting for another process to finish {_dialect.Name} migrations."
                );
            }

            if (!loggedWait)
            {
                _logger.LogInformation(
                    "Waiting for another process to finish {Database} migrations",
                    _dialect.Name
                );
                loggedWait = true;
            }

            await Task.Delay(LockPollInterval, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ReleaseLockAsync(SqlSession session)
    {
        try
        {
            await session
                .ExecuteAsync(
                    _dialect.ReleaseMigrationLockStatement,
                    p => p.Integer64("key", LockKey),
                    CancellationToken.None
                )
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // The lock ends with the database session as well
            _logger.LogWarning(
                ex,
                "Failed to release the {Database} migration lock",
                _dialect.Name
            );
        }
    }
}
