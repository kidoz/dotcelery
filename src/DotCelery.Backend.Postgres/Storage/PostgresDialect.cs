using System.Globalization;
using System.Text;
using DotCelery.Storage.Sql;
using DotCelery.Storage.Sql.Migrations;
using DotCelery.Storage.Sql.Schema;

namespace DotCelery.Backend.Postgres.Storage;

/// <summary>
/// The PostgreSQL <see cref="SqlDialect"/>.
/// </summary>
/// <remarks>
/// Identifiers are quoted in lower case, which matches how PostgreSQL treats unquoted names
/// while still allowing names that are reserved words.
/// </remarks>
public sealed class PostgresDialect : SqlDialect
{
    private const string BlockQuote = "$dotcelery$";
    private const string StatementQuote = "$dotcelery_migration$";

    private PostgresDialect() { }

    /// <summary>
    /// Gets the dialect.
    /// </summary>
    public static PostgresDialect Instance { get; } = new();

    /// <inheritdoc />
    public override string Name => "PostgreSQL";

    /// <inheritdoc />
    public override string SchemaExistsQuery =>
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = lower(@schema))";

    /// <inheritdoc />
    public override string TableExistsQuery =>
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_tables "
        + "WHERE schemaname = lower(@schema) AND tablename = lower(@table))";

    /// <inheritdoc />
    public override string TryAcquireMigrationLockQuery => "SELECT pg_try_advisory_lock(@key)";

    /// <inheritdoc />
    public override string ReleaseMigrationLockStatement => "SELECT pg_advisory_unlock(@key)";

    /// <inheritdoc />
    public override string Quote(string identifier)
    {
        SqlIdentifier.ThrowIfInvalid(identifier);
#pragma warning disable CA1308 // PostgreSQL folds unquoted identifiers to lower case
        return $"\"{identifier.ToLowerInvariant()}\"";
#pragma warning restore CA1308
    }

    /// <inheritdoc />
    public override string RenderGuardedMigration(
        string schema,
        string historyTable,
        string moduleName,
        SqlMigration migration
    )
    {
        ArgumentNullException.ThrowIfNull(migration);

        var statements = migration
            .Operations.SelectMany(o => Render(schema, o))
            .Select(s => s.ReplaceLineEndings("\n").Trim())
            .ToList();

        if (
            statements.Any(s =>
                s.Contains(BlockQuote, StringComparison.Ordinal)
                || s.Contains(StatementQuote, StringComparison.Ordinal)
            )
        )
        {
            throw new InvalidOperationException(
                $"Migration {migration.Version} of module '{moduleName}' contains a reserved "
                    + "dollar-quote tag and cannot be written to a script."
            );
        }

        var history = Qualify(schema, historyTable);
        var module = Literal(moduleName);
        var script = new StringBuilder();

        script.AppendLine(CultureInfo.InvariantCulture, $"DO {BlockQuote}");
        script.AppendLine("BEGIN");
        script.AppendLine(
            CultureInfo.InvariantCulture,
            $"    IF NOT EXISTS (SELECT 1 FROM {history} WHERE {Quote("module")} = {module} AND {Quote("version")} = {migration.Version}) THEN"
        );

        foreach (var statement in statements)
        {
            script.AppendLine(
                CultureInfo.InvariantCulture,
                $"        EXECUTE {StatementQuote}{statement}{StatementQuote};"
            );
        }

        script.AppendLine(
            CultureInfo.InvariantCulture,
            $"        INSERT INTO {history} ({Quote("module")}, {Quote("version")}, {Quote("description")}, {Quote("checksum")}, {Quote("applied_at")}) "
                + $"VALUES ({module}, {migration.Version}, {Literal(migration.Description)}, {Literal(migration.Checksum)}, now());"
        );
        script.AppendLine("    END IF;");
        script.AppendLine("END");
        script.Append(CultureInfo.InvariantCulture, $"{BlockQuote};");

        return script.ToString();
    }

    /// <inheritdoc />
    protected override string ColumnType(SqlColumn column)
    {
        ArgumentNullException.ThrowIfNull(column);

        return column.Type switch
        {
            SqlType.Text => "text",
            SqlType.Binary => "bytea",
            SqlType.Integer32 => "integer",
            SqlType.Integer64 => "bigint",
            SqlType.Boolean => "boolean",
            SqlType.Timestamp => "timestamptz",
            SqlType.Uuid => "uuid",
            _ => throw new NotSupportedException($"Unknown column type {column.Type}."),
        };
    }

    private static string Literal(string value) =>
        $"'{value.Replace("'", "''", StringComparison.Ordinal)}'";
}
