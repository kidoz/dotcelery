using System.Globalization;
using System.Text;
using DotCelery.Storage.Sql;
using DotCelery.Storage.Sql.Migrations;
using DotCelery.Storage.Sql.Schema;

namespace DotCelery.Backend.SqlServer.Storage;

/// <summary>
/// The SQL Server dialect.
/// </summary>
/// <remarks>
/// Text columns use a binary collation, so keys are case-sensitive and sort in ordinal order
/// whatever the database's default collation. Primary keys are nonclustered, because the keys
/// of the storage tables can exceed the size limit of a clustered index.
/// </remarks>
public sealed class SqlServerDialect : SqlDialect
{
    /// <summary>
    /// The collation of text columns.
    /// </summary>
    public const string Collation = "Latin1_General_100_BIN2";

    // The migration lock is an application lock owned by the session
    private const string LockResource =
        "DECLARE @resource nvarchar(255) = CONCAT(N'dotcelery:migrations:', @key);";

    private SqlServerDialect() { }

    /// <summary>
    /// Gets the dialect instance.
    /// </summary>
    public static SqlServerDialect Instance { get; } = new();

    /// <inheritdoc />
    public override string Name => "SQL Server";

    /// <inheritdoc />
    public override string SchemaExistsQuery =>
        "SELECT CAST(CASE WHEN SCHEMA_ID(@schema) IS NULL THEN 0 ELSE 1 END AS bit)";

    /// <inheritdoc />
    public override string TableExistsQuery =>
        "SELECT CAST(CASE WHEN OBJECT_ID(QUOTENAME(@schema) + N'.' + QUOTENAME(@table), N'U') "
        + "IS NULL THEN 0 ELSE 1 END AS bit)";

    /// <inheritdoc />
    public override string TryAcquireMigrationLockQuery =>
        $"""
            {LockResource}
            DECLARE @result int;
            EXEC @result = sp_getapplock @Resource = @resource, @LockMode = 'Exclusive',
                @LockOwner = 'Session', @LockTimeout = 0;
            SELECT CAST(CASE WHEN @result >= 0 THEN 1 ELSE 0 END AS bit);
            """;

    /// <inheritdoc />
    public override string ReleaseMigrationLockStatement =>
        $"""
            {LockResource}
            EXEC sp_releaseapplock @Resource = @resource, @LockOwner = 'Session';
            """;

    /// <inheritdoc />
    public override string Quote(string identifier)
    {
        SqlIdentifier.ThrowIfInvalid(identifier);
        return $"[{identifier}]";
    }

    /// <inheritdoc />
    public override string RenderCreateSchema(string schema, bool ifNotExists)
    {
        // CREATE SCHEMA must be the only statement in its batch
        var create = $"EXEC(N'CREATE SCHEMA {Quote(schema)}')";
        return ifNotExists ? $"IF SCHEMA_ID({Literal(schema)}) IS NULL {create}" : create;
    }

    /// <inheritdoc />
    public override string RenderCreateTable(string schema, SqlTable table, bool ifNotExists)
    {
        ArgumentNullException.ThrowIfNull(table);

        var definitions = table.Columns.Select(c => $"\n    {RenderColumn(c)}").ToList();
        if (table.PrimaryKey.Count > 0)
        {
            definitions.Add(
                $"\n    CONSTRAINT [{PrimaryKeyName(table.Name)}] PRIMARY KEY NONCLUSTERED ({string.Join(", ", table.PrimaryKey.Select(Quote))})"
            );
        }

        var create =
            $"CREATE TABLE {Qualify(schema, table.Name)} ({string.Join(",", definitions)}\n)";
        return ifNotExists
            ? $"IF OBJECT_ID({Literal(Qualify(schema, table.Name))}, N'U') IS NULL\n{create}"
            : create;
    }

    /// <summary>
    /// Gets the name of a table's primary key, which statements can name in index hints.
    /// </summary>
    /// <param name="table">The table name.</param>
    /// <returns>The primary key name.</returns>
    public static string PrimaryKeyName(string table) => $"pk_{table}";

    /// <inheritdoc />
    public override string RenderGuardedMigration(
        string schema,
        string historyTable,
        string moduleName,
        SqlMigration migration
    )
    {
        ArgumentNullException.ThrowIfNull(migration);

        var history = Qualify(schema, historyTable);
        var module = Literal(moduleName);
        var script = new StringBuilder();
        script.AppendLine(
            CultureInfo.InvariantCulture,
            $"IF NOT EXISTS (SELECT 1 FROM {history} WHERE {Quote("module")} = {module} AND {Quote("version")} = {migration.Version})"
        );
        script.AppendLine("BEGIN");
        foreach (var statement in migration.Operations.SelectMany(o => Render(schema, o)))
        {
            script.AppendLine(
                CultureInfo.InvariantCulture,
                $"    EXEC({Literal(statement.ReplaceLineEndings("\n").Trim())});"
            );
        }

        script.AppendLine(
            CultureInfo.InvariantCulture,
            $"    INSERT INTO {history} ({Quote("module")}, {Quote("version")}, {Quote("description")}, {Quote("checksum")}, {Quote("applied_at")}) "
                + $"VALUES ({module}, {migration.Version}, {Literal(migration.Description)}, {Literal(migration.Checksum)}, SYSDATETIMEOFFSET());"
        );
        script.Append("END;");
        return script.ToString();
    }

    /// <inheritdoc />
    protected override string ColumnType(SqlColumn column)
    {
        ArgumentNullException.ThrowIfNull(column);

        return column.Type switch
        {
            SqlType.Text => column.MaxLength is { } length
                ? $"nvarchar({length}) COLLATE {Collation}"
                : $"nvarchar(max) COLLATE {Collation}",
            SqlType.Binary => "varbinary(max)",
            SqlType.Integer32 => "int",
            SqlType.Integer64 => "bigint",
            SqlType.Boolean => "bit",
            SqlType.Timestamp => "datetimeoffset(6)",
            SqlType.Uuid => "uniqueidentifier",
            _ => throw new NotSupportedException($"Unknown column type {column.Type}."),
        };
    }

    // The default start of a bigint sequence is its minimum value
    /// <inheritdoc />
    protected override string RenderCreateSequence(string schema, string name) =>
        $"CREATE SEQUENCE {Qualify(schema, name)} AS bigint START WITH 1";

    /// <inheritdoc />
    protected override string RenderAddColumn(string schema, string table, SqlColumn column) =>
        $"ALTER TABLE {Qualify(schema, table)} ADD {RenderColumn(column)}";

    private static string Literal(string value) =>
        $"N'{value.Replace("'", "''", StringComparison.Ordinal)}'";
}
