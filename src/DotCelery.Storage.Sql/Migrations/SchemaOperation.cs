using DotCelery.Storage.Sql.Schema;

namespace DotCelery.Storage.Sql.Migrations;

/// <summary>
/// A schema change within a migration. Each <see cref="SqlDialect"/> renders operations to SQL.
/// </summary>
/// <remarks>
/// Migration checksums are computed from the operations, not from the rendered SQL, so a
/// dialect can change how it renders SQL without invalidating migrations that were already
/// applied.
/// </remarks>
public abstract record SchemaOperation
{
    private SchemaOperation() { }

    internal abstract string Describe();

    /// <summary>
    /// Creates a sequence of 64-bit numbers.
    /// </summary>
    public sealed record CreateSequence : SchemaOperation
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CreateSequence"/> class.
        /// </summary>
        /// <param name="name">The sequence name.</param>
        public CreateSequence(string name)
        {
            SqlIdentifier.ThrowIfInvalid(name);
            Name = name;
        }

        /// <summary>Gets the sequence name.</summary>
        public string Name { get; }

        internal override string Describe() => $"create sequence {Name}";
    }

    /// <summary>
    /// Creates a table.
    /// </summary>
    public sealed record CreateTable : SchemaOperation
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CreateTable"/> class.
        /// </summary>
        /// <param name="table">The table definition.</param>
        public CreateTable(SqlTable table)
        {
            ArgumentNullException.ThrowIfNull(table);
            Table = table;
        }

        /// <summary>Gets the table definition.</summary>
        public SqlTable Table { get; }

        internal override string Describe() =>
            $"create table {Table.Name} ({string.Join(", ", Table.Columns.Select(c => c.Describe()))}) "
            + $"primary key ({string.Join(", ", Table.PrimaryKey)})";
    }

    /// <summary>
    /// Creates an index.
    /// </summary>
    public sealed record CreateIndex : SchemaOperation
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CreateIndex"/> class.
        /// </summary>
        /// <param name="table">The indexed table.</param>
        /// <param name="index">The index definition.</param>
        public CreateIndex(string table, SqlIndex index)
        {
            SqlIdentifier.ThrowIfInvalid(table);
            ArgumentNullException.ThrowIfNull(index);
            Table = table;
            Index = index;
        }

        /// <summary>Gets the indexed table.</summary>
        public string Table { get; }

        /// <summary>Gets the index definition.</summary>
        public SqlIndex Index { get; }

        internal override string Describe() =>
            $"create {(Index.Unique ? "unique " : "")}index {Index.Name} on {Table} "
            + $"({string.Join(", ", Index.Columns)})";
    }

    /// <summary>
    /// Adds a column to an existing table. The column must accept nulls, because existing
    /// rows have no value for it.
    /// </summary>
    public sealed record AddColumn : SchemaOperation
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AddColumn"/> class.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <param name="column">The new column; it must accept nulls.</param>
        public AddColumn(string table, SqlColumn column)
        {
            SqlIdentifier.ThrowIfInvalid(table);
            ArgumentNullException.ThrowIfNull(column);
            if (!column.Nullable)
            {
                throw new ArgumentException(
                    $"Column '{column.Name}' must accept nulls: existing rows have no value for it.",
                    nameof(column)
                );
            }

            Table = table;
            Column = column;
        }

        /// <summary>Gets the table.</summary>
        public string Table { get; }

        /// <summary>Gets the new column.</summary>
        public SqlColumn Column { get; }

        internal override string Describe() => $"add column {Table}.{Column.Describe()}";
    }

    /// <summary>
    /// Runs a SQL statement as written, for changes that the other operations cannot express,
    /// such as data changes. The statement is not portable between databases.
    /// </summary>
    public sealed record ExecuteSql : SchemaOperation
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ExecuteSql"/> class.
        /// </summary>
        /// <param name="statement">The SQL statement.</param>
        public ExecuteSql(string statement)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(statement);
            Statement = statement;
        }

        /// <summary>Gets the SQL statement.</summary>
        public string Statement { get; }

        // SQL written in C# raw string literals has the line endings of the checkout
        internal override string Describe() => $"sql {Statement.ReplaceLineEndings("\n").Trim()}";
    }
}
