namespace DotCelery.Core.Migrations;

/// <summary>
/// Attribute to mark a class as a database migration with version and description.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class MigrationAttribute : Attribute
{
    /// <summary>
    /// Gets the migration version number.
    /// </summary>
    public long Version { get; }

    /// <summary>
    /// Gets the migration description.
    /// </summary>
    public string Description { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="MigrationAttribute"/> class.
    /// </summary>
    /// <param name="version">The version number (use timestamp format: YYYYMMDDNNN).</param>
    /// <param name="description">A description of what this migration does.</param>
    public MigrationAttribute(long version, string description)
    {
        Version = version;
        Description = description;
    }
}

/// <summary>
/// Base class for migrations that use the <see cref="MigrationAttribute"/>.
/// </summary>
public abstract class MigrationBase : IMigration
{
    private readonly Lazy<(long Version, string Description)> _metadata;

    /// <inheritdoc />
    public long Version => _metadata.Value.Version;

    /// <inheritdoc />
    public string Description => _metadata.Value.Description;

    /// <summary>
    /// Initializes a new instance of the <see cref="MigrationBase"/> class.
    /// </summary>
    protected MigrationBase()
    {
        _metadata = new Lazy<(long, string)>(() =>
        {
            var attr = GetType()
                .GetCustomAttributes(typeof(MigrationAttribute), false)
                .Cast<MigrationAttribute>()
                .FirstOrDefault();

            if (attr is null)
            {
                throw new InvalidOperationException(
                    $"Migration {GetType().Name} must have a [Migration] attribute"
                );
            }

            return (attr.Version, attr.Description);
        });
    }

    /// <inheritdoc />
    public abstract ValueTask UpAsync(
        IMigrationContext context,
        CancellationToken cancellationToken = default
    );

    /// <inheritdoc />
    public abstract ValueTask DownAsync(
        IMigrationContext context,
        CancellationToken cancellationToken = default
    );
}
