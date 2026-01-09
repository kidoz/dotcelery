using Microsoft.Extensions.Logging;

namespace DotCelery.Core.Migrations;

/// <summary>
/// Runs database migrations in order, tracking applied versions.
/// Works with any database type through abstractions.
/// </summary>
public sealed class MigrationRunner
{
    private readonly IMigrationStore _store;
    private readonly IMigrationContext _context;
    private readonly ILogger<MigrationRunner> _logger;
    private readonly List<IMigration> _migrations = [];

    /// <summary>
    /// Initializes a new instance of the <see cref="MigrationRunner"/> class.
    /// </summary>
    /// <param name="store">The migration store for tracking applied migrations.</param>
    /// <param name="context">The migration context for executing migrations.</param>
    /// <param name="logger">The logger.</param>
    public MigrationRunner(
        IMigrationStore store,
        IMigrationContext context,
        ILogger<MigrationRunner> logger
    )
    {
        _store = store;
        _context = context;
        _logger = logger;
    }

    /// <summary>
    /// Adds a migration to the runner.
    /// </summary>
    /// <param name="migration">The migration to add.</param>
    /// <returns>The runner for chaining.</returns>
    public MigrationRunner AddMigration(IMigration migration)
    {
        _migrations.Add(migration);
        return this;
    }

    /// <summary>
    /// Adds all migrations from an assembly.
    /// </summary>
    /// <param name="assembly">The assembly to scan.</param>
    /// <returns>The runner for chaining.</returns>
    public MigrationRunner AddMigrationsFromAssembly(System.Reflection.Assembly assembly)
    {
        var migrationTypes = assembly
            .GetTypes()
            .Where(t => t.IsClass && !t.IsAbstract && typeof(IMigration).IsAssignableFrom(t));

        foreach (var type in migrationTypes)
        {
            if (Activator.CreateInstance(type) is IMigration migration)
            {
                _migrations.Add(migration);
            }
        }

        return this;
    }

    /// <summary>
    /// Runs all pending migrations up to the latest version.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of migrations applied.</returns>
    public async Task<int> MigrateToLatestAsync(CancellationToken cancellationToken = default)
    {
        await _store.InitializeAsync(cancellationToken).ConfigureAwait(false);

        var applied = await _store.GetAppliedVersionsAsync(cancellationToken).ConfigureAwait(false);
        var appliedSet = applied.ToHashSet();

        var pending = _migrations
            .Where(m => !appliedSet.Contains(m.Version))
            .OrderBy(m => m.Version)
            .ToList();

        if (pending.Count == 0)
        {
            _logger.LogInformation("Database is up to date. No migrations to apply");
            return 0;
        }

        _logger.LogInformation("Found {Count} pending migration(s)", pending.Count);

        foreach (var migration in pending)
        {
            await ApplyMigrationAsync(migration, cancellationToken).ConfigureAwait(false);
        }

        _logger.LogInformation("Successfully applied {Count} migration(s)", pending.Count);
        return pending.Count;
    }

    /// <summary>
    /// Migrates to a specific version (up or down).
    /// </summary>
    /// <param name="targetVersion">The target version.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of migrations applied or rolled back.</returns>
    public async Task<int> MigrateToAsync(
        long targetVersion,
        CancellationToken cancellationToken = default
    )
    {
        await _store.InitializeAsync(cancellationToken).ConfigureAwait(false);

        var applied = await _store.GetAppliedVersionsAsync(cancellationToken).ConfigureAwait(false);
        var currentVersion = applied.Count > 0 ? applied.Max() : 0;

        if (targetVersion == currentVersion)
        {
            _logger.LogInformation("Already at version {Version}", targetVersion);
            return 0;
        }

        int count;
        if (targetVersion > currentVersion)
        {
            // Migrate up
            var appliedSet = applied.ToHashSet();
            var toApply = _migrations
                .Where(m => m.Version > currentVersion && m.Version <= targetVersion)
                .Where(m => !appliedSet.Contains(m.Version))
                .OrderBy(m => m.Version)
                .ToList();

            foreach (var migration in toApply)
            {
                await ApplyMigrationAsync(migration, cancellationToken).ConfigureAwait(false);
            }

            count = toApply.Count;
        }
        else
        {
            // Migrate down
            var toRollback = _migrations
                .Where(m => m.Version > targetVersion && m.Version <= currentVersion)
                .Where(m => applied.Contains(m.Version))
                .OrderByDescending(m => m.Version)
                .ToList();

            foreach (var migration in toRollback)
            {
                await RollbackMigrationAsync(migration, cancellationToken).ConfigureAwait(false);
            }

            count = toRollback.Count;
        }

        return count;
    }

    /// <summary>
    /// Rolls back the last applied migration.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if a migration was rolled back.</returns>
    public async Task<bool> RollbackLastAsync(CancellationToken cancellationToken = default)
    {
        await _store.InitializeAsync(cancellationToken).ConfigureAwait(false);

        var applied = await _store.GetAppliedVersionsAsync(cancellationToken).ConfigureAwait(false);
        if (applied.Count == 0)
        {
            _logger.LogInformation("No migrations to rollback");
            return false;
        }

        var lastVersion = applied.Max();
        var migration = _migrations.FirstOrDefault(m => m.Version == lastVersion);

        if (migration is null)
        {
            _logger.LogWarning(
                "Migration {Version} not found in registered migrations",
                lastVersion
            );
            return false;
        }

        await RollbackMigrationAsync(migration, cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Gets the current database version.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The current version, or 0 if no migrations applied.</returns>
    public async Task<long> GetCurrentVersionAsync(CancellationToken cancellationToken = default)
    {
        await _store.InitializeAsync(cancellationToken).ConfigureAwait(false);
        var applied = await _store.GetAppliedVersionsAsync(cancellationToken).ConfigureAwait(false);
        return applied.Count > 0 ? applied.Max() : 0;
    }

    /// <summary>
    /// Gets all pending migrations.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of pending migrations.</returns>
    public async Task<IReadOnlyList<IMigration>> GetPendingMigrationsAsync(
        CancellationToken cancellationToken = default
    )
    {
        await _store.InitializeAsync(cancellationToken).ConfigureAwait(false);
        var applied = await _store.GetAppliedVersionsAsync(cancellationToken).ConfigureAwait(false);
        var appliedSet = applied.ToHashSet();

        return _migrations
            .Where(m => !appliedSet.Contains(m.Version))
            .OrderBy(m => m.Version)
            .ToList();
    }

    private async Task ApplyMigrationAsync(
        IMigration migration,
        CancellationToken cancellationToken
    )
    {
        _logger.LogInformation(
            "Applying migration {Version}: {Description}",
            migration.Version,
            migration.Description
        );

        try
        {
            await migration.UpAsync(_context, cancellationToken).ConfigureAwait(false);
            await _store
                .MarkAppliedAsync(migration.Version, migration.Description, cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation("Applied migration {Version}", migration.Version);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to apply migration {Version}", migration.Version);
            throw new MigrationException(migration.Version, migration.Description, "up", ex);
        }
    }

    private async Task RollbackMigrationAsync(
        IMigration migration,
        CancellationToken cancellationToken
    )
    {
        _logger.LogInformation(
            "Rolling back migration {Version}: {Description}",
            migration.Version,
            migration.Description
        );

        try
        {
            await migration.DownAsync(_context, cancellationToken).ConfigureAwait(false);
            await _store
                .MarkRolledBackAsync(migration.Version, cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation("Rolled back migration {Version}", migration.Version);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to rollback migration {Version}", migration.Version);
            throw new MigrationException(migration.Version, migration.Description, "down", ex);
        }
    }
}

/// <summary>
/// Exception thrown when a migration fails.
/// </summary>
public sealed class MigrationException : Exception
{
    /// <summary>
    /// Gets the migration version that failed.
    /// </summary>
    public long Version { get; }

    /// <summary>
    /// Gets the migration description.
    /// </summary>
    public string MigrationDescription { get; }

    /// <summary>
    /// Gets the direction (up or down) that failed.
    /// </summary>
    public string Direction { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="MigrationException"/> class.
    /// </summary>
    public MigrationException(
        long version,
        string description,
        string direction,
        Exception innerException
    )
        : base($"Migration {version} ({description}) failed during {direction}", innerException)
    {
        Version = version;
        MigrationDescription = description;
        Direction = direction;
    }
}
