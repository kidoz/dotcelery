using System.Text.RegularExpressions;

namespace DotCelery.Build.SqlValidator;

/// <summary>
/// PostgreSQL SQL file validator for build-time validation.
/// Performs basic syntax validation on SQL files.
/// </summary>
public static partial class Program
{
    // Common PostgreSQL keywords for basic validation
    private static readonly HashSet<string> ValidKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "DROP",
        "ALTER",
        "TABLE",
        "INDEX",
        "SEQUENCE",
        "FROM",
        "WHERE",
        "AND",
        "OR",
        "NOT",
        "IN",
        "EXISTS",
        "LIKE",
        "BETWEEN",
        "IS",
        "NULL",
        "AS",
        "ON",
        "JOIN",
        "LEFT",
        "RIGHT",
        "INNER",
        "OUTER",
        "FULL",
        "CROSS",
        "GROUP",
        "BY",
        "HAVING",
        "ORDER",
        "ASC",
        "DESC",
        "LIMIT",
        "OFFSET",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "ALL",
        "DISTINCT",
        "INTO",
        "VALUES",
        "SET",
        "DEFAULT",
        "PRIMARY",
        "KEY",
        "FOREIGN",
        "REFERENCES",
        "UNIQUE",
        "CHECK",
        "CONSTRAINT",
        "IF",
        "ELSE",
        "THEN",
        "WHEN",
        "CASE",
        "END",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "TRANSACTION",
        "CASCADE",
        "RESTRICT",
        "TRIGGER",
        "FUNCTION",
        "PROCEDURE",
        "RETURN",
        "RETURNS",
        "LANGUAGE",
        "NOTIFY",
        "LISTEN",
        "UNLISTEN",
        "FOR",
        "SKIP",
        "LOCKED",
        "CONFLICT",
        "DO",
        "NOTHING",
        "EXCLUDED",
        "TIMESTAMPTZ",
        "TIMESTAMP",
        "WITH",
        "TIME",
        "ZONE",
        "VARCHAR",
        "TEXT",
        "INTEGER",
        "INT",
        "BIGINT",
        "SERIAL",
        "BOOLEAN",
        "JSONB",
        "JSON",
        "BYTEA",
        "DECIMAL",
        "NUMERIC",
        "FLOAT",
        "DOUBLE",
        "PRECISION",
        "NOW",
        "CURRENT_TIMESTAMP",
        "COUNT",
        "SUM",
        "AVG",
        "MIN",
        "MAX",
        "COALESCE",
        "NULLIF",
        "CAST",
        "NEXTVAL",
    };

    /// <summary>
    /// Entry point for the SQL validator.
    /// </summary>
    public static int Main(string[] args)
    {
        if (args.Length == 0)
        {
            Console.Error.WriteLine("Usage: DotCelery.Build.SqlValidator <sql-files...>");
            Console.Error.WriteLine("       DotCelery.Build.SqlValidator --directory <path>");
            return 1;
        }

        var files = GetFilesToValidate(args);
        if (files.Count == 0)
        {
            Console.WriteLine("No SQL files found to validate.");
            return 0;
        }

        Console.WriteLine($"Validating {files.Count} SQL file(s)...");

        var hasErrors = false;
        var validCount = 0;

        foreach (var file in files)
        {
            var result = ValidateFile(file);
            if (result.IsValid)
            {
                validCount++;
                Console.WriteLine($"  OK: {Path.GetFileName(file)}");
            }
            else
            {
                hasErrors = true;
                Console.Error.WriteLine($"  ERROR: {Path.GetFileName(file)}");
                foreach (var error in result.Errors)
                {
                    Console.Error.WriteLine($"    - {error}");
                }
            }
        }

        Console.WriteLine();
        if (hasErrors)
        {
            Console.Error.WriteLine(
                $"SQL validation FAILED: {files.Count - validCount} error(s) in {files.Count} file(s)"
            );
            return 1;
        }

        Console.WriteLine($"SQL validation PASSED: {validCount} file(s) validated successfully.");
        return 0;
    }

    private static List<string> GetFilesToValidate(string[] args)
    {
        var files = new List<string>();

        for (var i = 0; i < args.Length; i++)
        {
            if (args[i] == "--directory" && i + 1 < args.Length)
            {
                var directory = args[++i];
                if (Directory.Exists(directory))
                {
                    files.AddRange(
                        Directory.GetFiles(directory, "*.sql", SearchOption.AllDirectories)
                    );
                }
            }
            else if (File.Exists(args[i]))
            {
                files.Add(args[i]);
            }
        }

        return files;
    }

    private static ValidationResult ValidateFile(string filePath)
    {
        var errors = new List<string>();

        try
        {
            var sql = File.ReadAllText(filePath);
            var statements = SplitStatements(sql);

            foreach (var statement in statements)
            {
                var trimmed = statement.Trim();
                if (string.IsNullOrWhiteSpace(trimmed))
                {
                    continue;
                }

                // Skip comments
                if (trimmed.StartsWith("--"))
                {
                    continue;
                }

                var statementErrors = ValidateStatement(trimmed);
                errors.AddRange(statementErrors);
            }
        }
        catch (Exception ex)
        {
            errors.Add($"Failed to read file: {ex.Message}");
        }

        return new ValidationResult(errors.Count == 0, errors);
    }

    private static List<string> ValidateStatement(string statement)
    {
        var errors = new List<string>();

        // Remove template placeholders for validation
        var processedSql = statement.Replace("{schema}", "_schema_").Replace("{table}", "_table_");

        // Check for balanced parentheses
        var parenCount = 0;
        foreach (var c in processedSql)
        {
            if (c == '(')
                parenCount++;
            else if (c == ')')
                parenCount--;

            if (parenCount < 0)
            {
                errors.Add("Unbalanced parentheses: unexpected ')'");
                break;
            }
        }
        if (parenCount > 0)
        {
            errors.Add($"Unbalanced parentheses: {parenCount} unclosed '('");
        }

        // Check for balanced quotes
        if (!AreQuotesBalanced(processedSql, '\''))
        {
            errors.Add("Unbalanced single quotes");
        }
        if (!AreQuotesBalanced(processedSql, '"'))
        {
            errors.Add("Unbalanced double quotes");
        }

        // Check statement starts with valid keyword
        var firstWord = GetFirstWord(processedSql);
        if (!string.IsNullOrEmpty(firstWord) && !IsValidStatementStart(firstWord))
        {
            errors.Add($"Statement starts with unexpected keyword: '{firstWord}'");
        }

        // Check for common SQL injection patterns (should not exist in SQL files)
        if (ContainsSqlInjectionPattern(processedSql))
        {
            errors.Add("Potential SQL injection pattern detected");
        }

        return errors;
    }

    private static bool AreQuotesBalanced(string sql, char quote)
    {
        var inQuote = false;
        for (var i = 0; i < sql.Length; i++)
        {
            if (sql[i] == quote)
            {
                // Check for escaped quote
                if (inQuote && i + 1 < sql.Length && sql[i + 1] == quote)
                {
                    i++; // Skip escaped quote
                    continue;
                }
                inQuote = !inQuote;
            }
        }
        return !inQuote;
    }

    private static string GetFirstWord(string sql)
    {
        var match = FirstWordRegex().Match(sql);
        return match.Success ? match.Groups[1].Value : string.Empty;
    }

    private static bool IsValidStatementStart(string word)
    {
        var validStarts = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "SELECT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE",
            "DROP",
            "ALTER",
            "WITH",
            "TRUNCATE",
            "GRANT",
            "REVOKE",
            "EXPLAIN",
            "ANALYZE",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "SET",
            "NOTIFY",
            "LISTEN",
            "UNLISTEN",
            "VACUUM",
            "REINDEX",
            "CLUSTER",
            "COMMENT",
            "LOCK",
            "COPY",
            "DO",
        };
        return validStarts.Contains(word);
    }

    private static bool ContainsSqlInjectionPattern(string sql)
    {
        // Check for string concatenation that might indicate SQL injection
        // This shouldn't appear in parameterized SQL files
        return sql.Contains("' +", StringComparison.Ordinal)
            || sql.Contains("+ '", StringComparison.Ordinal)
            || sql.Contains("' ||", StringComparison.Ordinal)
            || sql.Contains("|| '", StringComparison.Ordinal);
    }

    private static List<string> SplitStatements(string sql)
    {
        var statements = new List<string>();
        var current = new System.Text.StringBuilder();
        var inString = false;
        var stringChar = '\0';
        var inLineComment = false;
        var inBlockComment = false;

        for (var i = 0; i < sql.Length; i++)
        {
            var c = sql[i];
            var next = i + 1 < sql.Length ? sql[i + 1] : '\0';

            // Handle line comments
            if (!inString && !inBlockComment && c == '-' && next == '-')
            {
                inLineComment = true;
                current.Append(c);
                continue;
            }

            if (inLineComment && (c == '\n' || c == '\r'))
            {
                inLineComment = false;
                current.Append(c);
                continue;
            }

            if (inLineComment)
            {
                current.Append(c);
                continue;
            }

            // Handle block comments
            if (!inString && !inBlockComment && c == '/' && next == '*')
            {
                inBlockComment = true;
                current.Append(c);
                continue;
            }

            if (inBlockComment && c == '*' && next == '/')
            {
                inBlockComment = false;
                current.Append(c);
                current.Append(next);
                i++;
                continue;
            }

            if (inBlockComment)
            {
                current.Append(c);
                continue;
            }

            // Handle strings
            if (!inString && (c == '\'' || c == '"'))
            {
                inString = true;
                stringChar = c;
                current.Append(c);
                continue;
            }

            if (inString && c == stringChar)
            {
                if (next == stringChar)
                {
                    current.Append(c);
                    current.Append(next);
                    i++;
                    continue;
                }
                inString = false;
                current.Append(c);
                continue;
            }

            // Handle statement separator
            if (!inString && c == ';')
            {
                current.Append(c);
                var stmt = current.ToString().Trim();
                if (!string.IsNullOrWhiteSpace(stmt))
                {
                    statements.Add(stmt);
                }
                current.Clear();
                continue;
            }

            current.Append(c);
        }

        var final = current.ToString().Trim();
        if (!string.IsNullOrWhiteSpace(final))
        {
            statements.Add(final);
        }

        return statements;
    }

    [GeneratedRegex(@"^\s*(\w+)", RegexOptions.Compiled)]
    private static partial Regex FirstWordRegex();

    private readonly record struct ValidationResult(bool IsValid, List<string> Errors);
}
