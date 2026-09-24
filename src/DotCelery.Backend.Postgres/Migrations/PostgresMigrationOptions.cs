using DotCelery.Backend.Postgres.Validation;

namespace DotCelery.Backend.Postgres.Migrations;

/// <summary>
/// Options for applying PostgreSQL schema migrations.
/// </summary>
public sealed class PostgresMigrationOptions
{
    private string _historyTableName = "dotcelery_migrations";
    private TimeSpan _lockTimeout = TimeSpan.FromMinutes(5);
    private TimeSpan _commandTimeout = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Gets or sets whether pending migrations are applied when the host starts, before
    /// any hosted service starts. Disable this when a DBA applies the script from
    /// <see cref="PostgresMigrator.GenerateScript"/> instead. Default is true.
    /// </summary>
    public bool RunAtStartup { get; set; } = true;

    /// <summary>
    /// Gets or sets the table, created in each store's schema, that records applied migrations.
    /// </summary>
    public string HistoryTableName
    {
        get => _historyTableName;
        set
        {
            PostgresIdentifierValidator.ValidateIdentifier(value, nameof(HistoryTableName));
            _historyTableName = value;
        }
    }

    /// <summary>
    /// Gets or sets how long to wait while another process holds the migration lock.
    /// </summary>
    public TimeSpan LockTimeout
    {
        get => _lockTimeout;
        set
        {
            PostgresIdentifierValidator.ValidateTimeout(value, nameof(LockTimeout));
            _lockTimeout = value;
        }
    }

    /// <summary>
    /// Gets or sets the timeout for each migration statement.
    /// </summary>
    public TimeSpan CommandTimeout
    {
        get => _commandTimeout;
        set
        {
            PostgresIdentifierValidator.ValidateTimeout(value, nameof(CommandTimeout));
            _commandTimeout = value;
        }
    }
}
