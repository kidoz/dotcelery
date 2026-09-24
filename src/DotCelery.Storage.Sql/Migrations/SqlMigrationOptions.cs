using DotCelery.Storage.Sql.Schema;

namespace DotCelery.Storage.Sql.Migrations;

/// <summary>
/// Options for applying SQL schema migrations.
/// </summary>
public sealed class SqlMigrationOptions
{
    private static readonly TimeSpan MinTimeout = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan MaxTimeout = TimeSpan.FromHours(1);

    private string _historyTableName = "dotcelery_migrations";
    private TimeSpan _lockTimeout = TimeSpan.FromMinutes(5);
    private TimeSpan _commandTimeout = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Gets or sets whether pending migrations are applied when the host starts, before any
    /// hosted service starts. Disable this when a DBA applies the script from
    /// <see cref="SqlMigrator.GenerateScript"/> instead. Default is true.
    /// </summary>
    public bool RunAtStartup { get; set; } = true;

    /// <summary>
    /// Gets or sets the table, created in each module's schema, that records applied migrations.
    /// </summary>
    public string HistoryTableName
    {
        get => _historyTableName;
        set
        {
            SqlIdentifier.ThrowIfInvalid(value);
            _historyTableName = value;
        }
    }

    /// <summary>
    /// Gets or sets how long to wait while another process holds the migration lock.
    /// </summary>
    public TimeSpan LockTimeout
    {
        get => _lockTimeout;
        set => _lockTimeout = ValidateTimeout(value);
    }

    /// <summary>
    /// Gets or sets the timeout for each migration statement.
    /// </summary>
    public TimeSpan CommandTimeout
    {
        get => _commandTimeout;
        set => _commandTimeout = ValidateTimeout(value);
    }

    private static TimeSpan ValidateTimeout(TimeSpan value)
    {
        if (value < MinTimeout || value > MaxTimeout)
        {
            throw new ArgumentOutOfRangeException(
                nameof(value),
                value,
                $"Must be between {MinTimeout} and {MaxTimeout}."
            );
        }

        return value;
    }
}
