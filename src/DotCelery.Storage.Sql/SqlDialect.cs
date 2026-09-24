using System.Text;
using DotCelery.Storage.Sql.Migrations;
using DotCelery.Storage.Sql.Schema;

namespace DotCelery.Storage.Sql;

/// <summary>
/// The SQL of one database: DDL for schema operations and the statements the migrator needs.
/// This is the only place where SQL text is written; everything else uses the schema model.
/// </summary>
/// <remarks>
/// Statements use named parameters written as <c>@name</c>.
/// </remarks>
public abstract class SqlDialect
{
    /// <summary>
    /// Gets the database name, such as <c>PostgreSQL</c>.
    /// </summary>
    public abstract string Name { get; }

    /// <summary>
    /// Gets a query that returns whether a schema exists. Parameter: <c>@schema</c>.
    /// </summary>
    public abstract string SchemaExistsQuery { get; }

    /// <summary>
    /// Gets a query that returns whether a table exists. Parameters: <c>@schema</c>, <c>@table</c>.
    /// </summary>
    public abstract string TableExistsQuery { get; }

    /// <summary>
    /// Gets a query that tries to take the migration lock without waiting and returns whether
    /// it succeeded. The lock is held by the connection. Parameter: <c>@key</c> (64-bit).
    /// </summary>
    public abstract string TryAcquireMigrationLockQuery { get; }

    /// <summary>
    /// Gets a statement that releases the migration lock. Parameter: <c>@key</c>.
    /// </summary>
    public abstract string ReleaseMigrationLockStatement { get; }

    /// <summary>
    /// Quotes a validated identifier.
    /// </summary>
    /// <param name="identifier">The identifier.</param>
    /// <returns>The quoted identifier.</returns>
    public abstract string Quote(string identifier);

    /// <summary>
    /// Quotes a schema-qualified name.
    /// </summary>
    /// <param name="schema">The schema.</param>
    /// <param name="name">The table or sequence name.</param>
    /// <returns>The quoted, qualified name.</returns>
    public string Qualify(string schema, string name) => $"{Quote(schema)}.{Quote(name)}";

    /// <summary>
    /// Renders a schema operation to SQL statements.
    /// </summary>
    /// <param name="schema">The target schema.</param>
    /// <param name="operation">The operation.</param>
    /// <returns>The statements, in order.</returns>
    public IReadOnlyList<string> Render(string schema, SchemaOperation operation)
    {
        SqlIdentifier.ThrowIfInvalid(schema);
        ArgumentNullException.ThrowIfNull(operation);

        return operation switch
        {
            SchemaOperation.CreateSequence op => [RenderCreateSequence(schema, op.Name)],
            SchemaOperation.CreateTable op =>
            [
                RenderCreateTable(schema, op.Table, ifNotExists: false),
            ],
            SchemaOperation.CreateIndex op => [RenderCreateIndex(schema, op.Table, op.Index)],
            SchemaOperation.AddColumn op => [RenderAddColumn(schema, op.Table, op.Column)],
            SchemaOperation.ExecuteSql op => [op.Statement],
            _ => throw new NotSupportedException(
                $"Unknown schema operation {operation.GetType().Name}."
            ),
        };
    }

    /// <summary>
    /// Renders a statement that creates a schema.
    /// </summary>
    /// <param name="schema">The schema.</param>
    /// <param name="ifNotExists">Whether an existing schema is left alone.</param>
    /// <returns>The statement.</returns>
    public virtual string RenderCreateSchema(string schema, bool ifNotExists) =>
        $"CREATE SCHEMA {(ifNotExists ? "IF NOT EXISTS " : "")}{Quote(schema)}";

    /// <summary>
    /// Renders a statement that creates a table.
    /// </summary>
    /// <param name="schema">The schema.</param>
    /// <param name="table">The table definition.</param>
    /// <param name="ifNotExists">Whether an existing table is left alone.</param>
    /// <returns>The statement.</returns>
    public virtual string RenderCreateTable(string schema, SqlTable table, bool ifNotExists)
    {
        ArgumentNullException.ThrowIfNull(table);

        var sql = new StringBuilder();
        sql.Append("CREATE TABLE ");
        if (ifNotExists)
        {
            sql.Append("IF NOT EXISTS ");
        }

        sql.Append(Qualify(schema, table.Name)).Append(" (");

        var definitions = table.Columns.Select(c => $"\n    {RenderColumn(c)}").ToList();
        if (table.PrimaryKey.Count > 0)
        {
            definitions.Add(
                $"\n    PRIMARY KEY ({string.Join(", ", table.PrimaryKey.Select(Quote))})"
            );
        }

        sql.Append(string.Join(",", definitions)).Append("\n)");
        return sql.ToString();
    }

    /// <summary>
    /// Renders the SQL script section that applies one migration unless the history already
    /// records it.
    /// </summary>
    /// <param name="schema">The schema of the module.</param>
    /// <param name="historyTable">The migration history table.</param>
    /// <param name="moduleName">The module name.</param>
    /// <param name="migration">The migration.</param>
    /// <returns>The script section.</returns>
    public abstract string RenderGuardedMigration(
        string schema,
        string historyTable,
        string moduleName,
        SqlMigration migration
    );

    /// <summary>
    /// Renders a query for the applied migrations of a module. Parameter: <c>@module</c>.
    /// Returns the columns <c>version</c> and <c>checksum</c>.
    /// </summary>
    /// <param name="schema">The schema.</param>
    /// <param name="historyTable">The migration history table.</param>
    /// <returns>The query.</returns>
    public virtual string RenderSelectMigrations(string schema, string historyTable) =>
        $"SELECT {Quote("version")}, {Quote("checksum")} FROM {Qualify(schema, historyTable)} "
        + $"WHERE {Quote("module")} = @module";

    /// <summary>
    /// Renders a statement that records an applied migration. Parameters: <c>@module</c>,
    /// <c>@version</c>, <c>@description</c>, <c>@checksum</c>, <c>@applied_at</c>.
    /// </summary>
    /// <param name="schema">The schema.</param>
    /// <param name="historyTable">The migration history table.</param>
    /// <returns>The statement.</returns>
    public virtual string RenderInsertMigration(string schema, string historyTable) =>
        $"INSERT INTO {Qualify(schema, historyTable)} "
        + $"({Quote("module")}, {Quote("version")}, {Quote("description")}, {Quote("checksum")}, {Quote("applied_at")}) "
        + "VALUES (@module, @version, @description, @checksum, @applied_at)";

    /// <summary>
    /// Gets the database type of a column.
    /// </summary>
    /// <param name="column">The column.</param>
    /// <returns>The database type.</returns>
    protected abstract string ColumnType(SqlColumn column);

    /// <summary>
    /// Renders a statement that creates a sequence.
    /// </summary>
    /// <param name="schema">The schema.</param>
    /// <param name="name">The sequence name.</param>
    /// <returns>The statement.</returns>
    protected virtual string RenderCreateSequence(string schema, string name) =>
        $"CREATE SEQUENCE {Qualify(schema, name)}";

    /// <summary>
    /// Renders a statement that creates an index.
    /// </summary>
    /// <param name="schema">The schema.</param>
    /// <param name="table">The indexed table.</param>
    /// <param name="index">The index definition.</param>
    /// <returns>The statement.</returns>
    protected virtual string RenderCreateIndex(string schema, string table, SqlIndex index)
    {
        ArgumentNullException.ThrowIfNull(index);

        return $"CREATE {(index.Unique ? "UNIQUE " : "")}INDEX {Quote(index.Name)} "
            + $"ON {Qualify(schema, table)} ({string.Join(", ", index.Columns.Select(Quote))})";
    }

    /// <summary>
    /// Renders a statement that adds a column.
    /// </summary>
    /// <param name="schema">The schema.</param>
    /// <param name="table">The table.</param>
    /// <param name="column">The new column.</param>
    /// <returns>The statement.</returns>
    protected virtual string RenderAddColumn(string schema, string table, SqlColumn column) =>
        $"ALTER TABLE {Qualify(schema, table)} ADD COLUMN {RenderColumn(column)}";

    /// <summary>
    /// Renders a column definition.
    /// </summary>
    /// <param name="column">The column.</param>
    /// <returns>The definition.</returns>
    protected virtual string RenderColumn(SqlColumn column)
    {
        ArgumentNullException.ThrowIfNull(column);
        return $"{Quote(column.Name)} {ColumnType(column)}{(column.Nullable ? "" : " NOT NULL")}";
    }
}
