namespace DotCelery.Storage.Sql.Schema;

/// <summary>
/// The logical type of a column. Each dialect maps it to a database type.
/// </summary>
public enum SqlType
{
    /// <summary>Unicode text.</summary>
    Text,

    /// <summary>Binary data.</summary>
    Binary,

    /// <summary>A 32-bit integer.</summary>
    Integer32,

    /// <summary>A 64-bit integer.</summary>
    Integer64,

    /// <summary>A boolean.</summary>
    Boolean,

    /// <summary>A point in time with its UTC offset.</summary>
    Timestamp,

    /// <summary>A UUID.</summary>
    Uuid,
}

/// <summary>
/// A column definition.
/// </summary>
public sealed record SqlColumn
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SqlColumn"/> class.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="type">The column type.</param>
    /// <param name="nullable">Whether the column accepts nulls.</param>
    /// <param name="maxLength">The maximum text length, for dialects that need one.</param>
    public SqlColumn(string name, SqlType type, bool nullable = false, int? maxLength = null)
    {
        SqlIdentifier.ThrowIfInvalid(name);
        if (maxLength is < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(maxLength), "Must be at least 1.");
        }

        Name = name;
        Type = type;
        Nullable = nullable;
        MaxLength = maxLength;
    }

    /// <summary>Gets the column name.</summary>
    public string Name { get; }

    /// <summary>Gets the column type.</summary>
    public SqlType Type { get; }

    /// <summary>Gets whether the column accepts nulls.</summary>
    public bool Nullable { get; }

    /// <summary>Gets the maximum text length, for dialects that need one.</summary>
    public int? MaxLength { get; }

    /// <summary>Creates a text column.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="maxLength">The maximum length, for dialects that need one.</param>
    /// <param name="nullable">Whether the column accepts nulls.</param>
    /// <returns>The column.</returns>
    public static SqlColumn Text(string name, int? maxLength = null, bool nullable = false) =>
        new(name, SqlType.Text, nullable, maxLength);

    /// <summary>Creates a binary column.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="nullable">Whether the column accepts nulls.</param>
    /// <returns>The column.</returns>
    public static SqlColumn Binary(string name, bool nullable = false) =>
        new(name, SqlType.Binary, nullable);

    /// <summary>Creates a 32-bit integer column.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="nullable">Whether the column accepts nulls.</param>
    /// <returns>The column.</returns>
    public static SqlColumn Integer32(string name, bool nullable = false) =>
        new(name, SqlType.Integer32, nullable);

    /// <summary>Creates a 64-bit integer column.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="nullable">Whether the column accepts nulls.</param>
    /// <returns>The column.</returns>
    public static SqlColumn Integer64(string name, bool nullable = false) =>
        new(name, SqlType.Integer64, nullable);

    /// <summary>Creates a boolean column.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="nullable">Whether the column accepts nulls.</param>
    /// <returns>The column.</returns>
    public static SqlColumn Boolean(string name, bool nullable = false) =>
        new(name, SqlType.Boolean, nullable);

    /// <summary>Creates a timestamp column.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="nullable">Whether the column accepts nulls.</param>
    /// <returns>The column.</returns>
    public static SqlColumn Timestamp(string name, bool nullable = false) =>
        new(name, SqlType.Timestamp, nullable);

    /// <summary>Creates a UUID column.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="nullable">Whether the column accepts nulls.</param>
    /// <returns>The column.</returns>
    public static SqlColumn Uuid(string name, bool nullable = false) =>
        new(name, SqlType.Uuid, nullable);

    internal string Describe() =>
        $"{Name}:{Type}{(MaxLength is { } length ? $"({length})" : "")}{(Nullable ? "?" : "")}";
}

/// <summary>
/// A table definition, as created by <see cref="Migrations.SchemaOperation.CreateTable"/>.
/// </summary>
public sealed record SqlTable
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SqlTable"/> class.
    /// </summary>
    /// <param name="name">The table name.</param>
    /// <param name="columns">The columns.</param>
    /// <param name="primaryKey">The primary key columns, if any.</param>
    public SqlTable(
        string name,
        IReadOnlyList<SqlColumn> columns,
        IReadOnlyList<string>? primaryKey = null
    )
    {
        SqlIdentifier.ThrowIfInvalid(name);
        ArgumentNullException.ThrowIfNull(columns);
        if (columns.Count == 0)
        {
            throw new ArgumentException("A table needs at least one column.", nameof(columns));
        }

        if (
            columns.Select(c => c.Name).Distinct(StringComparer.OrdinalIgnoreCase).Count()
            != columns.Count
        )
        {
            throw new ArgumentException(
                $"Table '{name}' has duplicate column names.",
                nameof(columns)
            );
        }

        primaryKey ??= [];
        foreach (var column in primaryKey)
        {
            if (
                !columns.Any(c => string.Equals(c.Name, column, StringComparison.OrdinalIgnoreCase))
            )
            {
                throw new ArgumentException(
                    $"Primary key column '{column}' is not a column of table '{name}'.",
                    nameof(primaryKey)
                );
            }
        }

        Name = name;
        Columns = columns;
        PrimaryKey = primaryKey;
    }

    /// <summary>Gets the table name.</summary>
    public string Name { get; }

    /// <summary>Gets the columns.</summary>
    public IReadOnlyList<SqlColumn> Columns { get; }

    /// <summary>Gets the primary key columns.</summary>
    public IReadOnlyList<string> PrimaryKey { get; }
}

/// <summary>
/// An index definition.
/// </summary>
public sealed record SqlIndex
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SqlIndex"/> class.
    /// </summary>
    /// <param name="name">The index name, unique within the schema.</param>
    /// <param name="columns">The indexed columns, in order.</param>
    /// <param name="unique">Whether the index enforces uniqueness.</param>
    public SqlIndex(string name, IReadOnlyList<string> columns, bool unique = false)
    {
        SqlIdentifier.ThrowIfInvalid(name);
        ArgumentNullException.ThrowIfNull(columns);
        if (columns.Count == 0)
        {
            throw new ArgumentException("An index needs at least one column.", nameof(columns));
        }

        foreach (var column in columns)
        {
            SqlIdentifier.ThrowIfInvalid(column, nameof(columns));
        }

        Name = name;
        Columns = columns;
        Unique = unique;
    }

    /// <summary>Gets the index name.</summary>
    public string Name { get; }

    /// <summary>Gets the indexed columns, in order.</summary>
    public IReadOnlyList<string> Columns { get; }

    /// <summary>Gets whether the index enforces uniqueness.</summary>
    public bool Unique { get; }
}
