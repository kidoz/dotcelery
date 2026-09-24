using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace DotCelery.Backend.Postgres.Migrations;

/// <summary>
/// Applies the registered <see cref="PostgresMigrationModule"/>s.
/// </summary>
/// <remarks>
/// <para>
/// Migrations for each database run under a PostgreSQL advisory lock, so processes that
/// start at the same time apply each migration once. Each migration runs in its own
/// transaction together with its history record.
/// </para>
/// <para>
/// The history stores a checksum of each applied migration. Migrating fails if an applied
/// migration no longer matches its definition: applied migrations must not change, and
/// schema changes are added as new migrations.
/// </para>
/// </remarks>
public sealed class PostgresMigrator
{
    internal const string BlockQuote = "$dotcelery$";
    internal const string StatementQuote = "$dotcelery_migration$";

    // "DotCelry" in ASCII, to tell this lock apart from the application's advisory locks
    private const long LockKey = 0x446F7443656C7279;

    private static readonly TimeSpan LockPollInterval = TimeSpan.FromMilliseconds(500);

    private readonly IPostgresDataSourceProvider _dataSources;
    private readonly PostgresMigrationOptions _options;
    private readonly ILogger<PostgresMigrator> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresMigrator"/> class.
    /// </summary>
    /// <param name="dataSources">Provides the data source for each connection string.</param>
    /// <param name="modules">The migration modules of the registered stores.</param>
    /// <param name="options">The migration options.</param>
    /// <param name="logger">The logger.</param>
    public PostgresMigrator(
        IPostgresDataSourceProvider dataSources,
        IEnumerable<PostgresMigrationModule> modules,
        IOptions<PostgresMigrationOptions> options,
        ILogger<PostgresMigrator> logger
    )
    {
        ArgumentNullException.ThrowIfNull(modules);

        _dataSources = dataSources;
        _options = options.Value;
        _logger = logger;

        // Registering the same store twice yields identical modules
        Modules = modules.DistinctBy(m => (m.ConnectionString, m.Schema, m.Name)).ToList();
    }

    /// <summary>
    /// Gets the migration modules this migrator applies.
    /// </summary>
    public IReadOnlyList<PostgresMigrationModule> Modules { get; }

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
            _logger.LogInformation("Applied {Count} PostgreSQL migration(s)", applied);
        }
        else
        {
            _logger.LogDebug("PostgreSQL schema is up to date");
        }

        return applied;
    }

    /// <summary>
    /// Generates a SQL script with every migration, for databases where schema changes are
    /// applied by hand. The script applies only migrations that are not recorded yet, so it
    /// can be run again after an upgrade.
    /// </summary>
    /// <remarks>
    /// The script covers all modules. If stores use different connection strings, run each
    /// module's section against its own database.
    /// </remarks>
    /// <returns>The SQL script.</returns>
    public string GenerateScript()
    {
        var script = new StringBuilder();
        script.AppendLine("-- DotCelery PostgreSQL schema");
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
                script.AppendLine(
                    CultureInfo.InvariantCulture,
                    $"CREATE SCHEMA IF NOT EXISTS {module.Schema};"
                );
                script.Append(CreateHistoryTableSql(module.Schema)).AppendLine(";");
            }

            foreach (var migration in module.Migrations)
            {
                AppendMigration(script, module, migration);
            }
        }

        return script.ToString();
    }

    internal static string ComputeChecksum(PostgresMigration migration)
    {
        var text = string.Join("\n;\n", migration.Statements.Select(Normalize));
        return Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(text)));
    }

    // Migration SQL comes from C# raw string literals, whose line endings follow the checkout
    private static string Normalize(string statement) => statement.ReplaceLineEndings("\n").Trim();

    private static string QuoteLiteral(string value) =>
        $"'{value.Replace("'", "''", StringComparison.Ordinal)}'";

    private async Task<int> MigrateDatabaseAsync(
        string connectionString,
        IReadOnlyList<PostgresMigrationModule> modules,
        CancellationToken cancellationToken
    )
    {
        await using var connection = await _dataSources
            .GetDataSource(connectionString)
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);

        await AcquireLockAsync(connection, cancellationToken).ConfigureAwait(false);

        try
        {
            var applied = 0;
            foreach (var module in modules)
            {
                applied += await MigrateModuleAsync(connection, module, cancellationToken)
                    .ConfigureAwait(false);
            }

            return applied;
        }
        finally
        {
            await ReleaseLockAsync(connection).ConfigureAwait(false);
        }
    }

    private async Task<int> MigrateModuleAsync(
        NpgsqlConnection connection,
        PostgresMigrationModule module,
        CancellationToken cancellationToken
    )
    {
        await EnsureHistoryTableAsync(connection, module.Schema, cancellationToken)
            .ConfigureAwait(false);

        var history = await GetHistoryAsync(connection, module, cancellationToken)
            .ConfigureAwait(false);
        var applied = 0;

        foreach (var migration in module.Migrations)
        {
            var checksum = ComputeChecksum(migration);

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

            await ApplyAsync(connection, module, migration, checksum, cancellationToken)
                .ConfigureAwait(false);
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
        NpgsqlConnection connection,
        PostgresMigrationModule module,
        PostgresMigration migration,
        string checksum,
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
            await using var transaction = await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);

            foreach (var statement in migration.Statements)
            {
                await ExecuteAsync(connection, transaction, statement, cancellationToken)
                    .ConfigureAwait(false);
            }

            await using var record = CreateCommand(
                connection,
                transaction,
                $"""
                INSERT INTO {HistoryTable(module.Schema)} (module, version, description, checksum)
                VALUES (@module, @version, @description, @checksum)
                """
            );
            record.Parameters.AddWithValue("module", module.Name);
            record.Parameters.AddWithValue("version", migration.Version);
            record.Parameters.AddWithValue("description", migration.Description);
            record.Parameters.AddWithValue("checksum", checksum);
            await record.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
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
        NpgsqlConnection connection,
        string schema,
        CancellationToken cancellationToken
    )
    {
        // Create only what is missing, so an up-to-date database needs no DDL privileges
        var schemaExists = await QueryBooleanAsync(
                connection,
                "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = lower(@name))",
                schema,
                cancellationToken
            )
            .ConfigureAwait(false);

        if (!schemaExists)
        {
            await ExecuteAsync(
                    connection,
                    transaction: null,
                    $"CREATE SCHEMA IF NOT EXISTS {schema}",
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        var tableExists = await QueryBooleanAsync(
                connection,
                "SELECT to_regclass(@name) IS NOT NULL",
                HistoryTable(schema),
                cancellationToken
            )
            .ConfigureAwait(false);

        if (!tableExists)
        {
            await ExecuteAsync(
                    connection,
                    transaction: null,
                    CreateHistoryTableSql(schema),
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
    }

    private async Task<Dictionary<long, string>> GetHistoryAsync(
        NpgsqlConnection connection,
        PostgresMigrationModule module,
        CancellationToken cancellationToken
    )
    {
        await using var command = CreateCommand(
            connection,
            transaction: null,
            $"SELECT version, checksum FROM {HistoryTable(module.Schema)} WHERE module = @module"
        );
        command.Parameters.AddWithValue("module", module.Name);

        var history = new Dictionary<long, string>();
        await using var reader = await command
            .ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            history[reader.GetInt64(0)] = reader.GetString(1);
        }

        return history;
    }

    private async Task AcquireLockAsync(
        NpgsqlConnection connection,
        CancellationToken cancellationToken
    )
    {
        var waited = Stopwatch.StartNew();
        var loggedWait = false;

        while (true)
        {
            await using var command = CreateCommand(
                connection,
                transaction: null,
                "SELECT pg_try_advisory_lock(@key)"
            );
            command.Parameters.AddWithValue("key", LockKey);

            if (await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true)
            {
                return;
            }

            if (waited.Elapsed >= _options.LockTimeout)
            {
                throw new TimeoutException(
                    $"Timed out after {_options.LockTimeout} waiting for another process to finish PostgreSQL migrations."
                );
            }

            if (!loggedWait)
            {
                _logger.LogInformation(
                    "Waiting for another process to finish PostgreSQL migrations"
                );
                loggedWait = true;
            }

            await Task.Delay(LockPollInterval, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ReleaseLockAsync(NpgsqlConnection connection)
    {
        try
        {
            await using var command = CreateCommand(
                connection,
                transaction: null,
                "SELECT pg_advisory_unlock(@key)"
            );
            command.Parameters.AddWithValue("key", LockKey);
            await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // The session reset when the connection returns to the pool releases it as well
            _logger.LogWarning(ex, "Failed to release the PostgreSQL migration lock");
        }
    }

    private async Task<bool> QueryBooleanAsync(
        NpgsqlConnection connection,
        string sql,
        string name,
        CancellationToken cancellationToken
    )
    {
        await using var command = CreateCommand(connection, transaction: null, sql);
        command.Parameters.AddWithValue("name", name);
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
    }

    private async Task ExecuteAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string sql,
        CancellationToken cancellationToken
    )
    {
        await using var command = CreateCommand(connection, transaction, sql);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private NpgsqlCommand CreateCommand(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string sql
    ) =>
        new(sql, connection, transaction)
        {
            CommandTimeout = (int)_options.CommandTimeout.TotalSeconds,
        };

    private string HistoryTable(string schema) => $"{schema}.{_options.HistoryTableName}";

    private string CreateHistoryTableSql(string schema) =>
        $"""
            CREATE TABLE IF NOT EXISTS {HistoryTable(schema)} (
                module TEXT NOT NULL,
                version BIGINT NOT NULL,
                description TEXT NOT NULL,
                checksum TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (module, version)
            )
            """;

    private void AppendMigration(
        StringBuilder script,
        PostgresMigrationModule module,
        PostgresMigration migration
    )
    {
        var history = HistoryTable(module.Schema);
        var name = QuoteLiteral(module.Name);

        script.AppendLine(CultureInfo.InvariantCulture, $"DO {BlockQuote}");
        script.AppendLine("BEGIN");
        script.AppendLine(
            CultureInfo.InvariantCulture,
            $"    IF NOT EXISTS (SELECT 1 FROM {history} WHERE module = {name} AND version = {migration.Version}) THEN"
        );

        foreach (var statement in migration.Statements)
        {
            script.AppendLine(
                CultureInfo.InvariantCulture,
                $"        EXECUTE {StatementQuote}{Normalize(statement)}{StatementQuote};"
            );
        }

        script.AppendLine(
            CultureInfo.InvariantCulture,
            $"        INSERT INTO {history} (module, version, description, checksum) VALUES ({name}, {migration.Version}, {QuoteLiteral(migration.Description)}, {QuoteLiteral(ComputeChecksum(migration))});"
        );
        script.AppendLine("    END IF;");
        script.AppendLine("END");
        script.AppendLine(CultureInfo.InvariantCulture, $"{BlockQuote};");
    }
}
