# DotCelery.Build.SqlValidator

**PostgreSQL SQL file validator for build-time validation**

> ⚠️ **Current Status**: This tool is built but **not currently used** in the DotCelery build process. All SQL is embedded in C# migration classes, not separate `.sql` files.

## Purpose

Validates PostgreSQL SQL files for common syntax errors before runtime:
- Balanced parentheses and quotes
- Valid SQL statement keywords
- SQL injection patterns
- Basic structural correctness

## Usage

### Command Line

```bash
# Validate specific files
dotnet run --project src/DotCelery.Build.SqlValidator -- file1.sql file2.sql

# Validate directory (recursive)
dotnet run --project src/DotCelery.Build.SqlValidator -- --directory path/to/sql

# Example output
Validating 3 SQL file(s)...
  OK: migration_001.sql
  OK: migration_002.sql
  ERROR: migration_003.sql
    - Unbalanced parentheses: 1 unclosed '('

SQL validation FAILED: 1 error(s) in 3 file(s)
```

### Exit Codes

- `0` - All SQL files valid
- `1` - Validation errors found or invalid arguments

## Validation Rules

### ✅ Checks Performed

1. **Balanced Parentheses**
   ```sql
   -- ❌ Error
   SELECT * FROM users WHERE (id = 1;

   -- ✅ OK
   SELECT * FROM users WHERE (id = 1);
   ```

2. **Balanced Quotes**
   ```sql
   -- ❌ Error
   SELECT * FROM users WHERE name = 'test;

   -- ✅ OK
   SELECT * FROM users WHERE name = 'test';
   ```

3. **Valid Statement Keywords**
   ```sql
   -- ❌ Error (invalid start)
   FOOBAR TABLE users;

   -- ✅ OK
   CREATE TABLE users (id INT);
   ```

4. **SQL Injection Patterns**
   ```sql
   -- ❌ Warning (string concatenation)
   WHERE name = 'foo' + @input

   -- ✅ OK (parameterized)
   WHERE name = @name
   ```

5. **Template Placeholder Support**
   ```sql
   -- ✅ OK (placeholders ignored during validation)
   CREATE TABLE {schema}.{table} (id INT);
   ```

### ❌ Not Checked

- PostgreSQL-specific syntax correctness
- Data type validation
- Constraint validity
- Semantic correctness
- Index optimization
- Performance issues

## Integration Examples

### MSBuild Target

Add to `*.csproj` to validate SQL during build:

```xml
<Target Name="ValidateSqlFiles" BeforeTargets="Build">
  <Exec Command="dotnet run --project $(MSBuildThisFileDirectory)../DotCelery.Build.SqlValidator/DotCelery.Build.SqlValidator.csproj -- --directory $(MSBuildThisFileDirectory)Migrations/Sql" />
</Target>
```

### Justfile Recipe

Add to `justfile` for manual validation:

```just
# Validate SQL migration files
validate-sql:
    dotnet run --project src/DotCelery.Build.SqlValidator -- --directory src/DotCelery.Backend.Postgres/Migrations/Sql
```

### GitHub Actions

Add to CI/CD pipeline:

```yaml
- name: Validate SQL Files
  run: |
    dotnet run --project src/DotCelery.Build.SqlValidator -- \
      --directory src/DotCelery.Backend.Postgres/Migrations/Sql
```

## Architecture

### Components

```
DotCelery.Build.SqlValidator/
├── Program.cs                    # Entry point and validation logic
│   ├── ValidateFile()           # File-level validation
│   ├── ValidateStatement()      # Statement-level checks
│   ├── SplitStatements()        # SQL parser (handles comments, strings)
│   └── Validation helpers       # Parentheses, quotes, keywords
└── DotCelery.Build.SqlValidator.csproj
```

### Key Features

- **Zero Dependencies**: Pure .NET implementation
- **Regex-based Parsing**: Uses C# 12 source-generated regex
- **Comment-aware**: Handles `--` line and `/* */` block comments
- **String-aware**: Properly handles quoted strings and escapes
- **Template-aware**: Supports `{schema}`, `{table}` placeholders

### Validation Algorithm

1. Read SQL file content
2. Split into individual statements (by `;`)
3. For each statement:
   - Skip comments and whitespace
   - Replace template placeholders
   - Check balanced parentheses
   - Check balanced quotes
   - Validate starting keyword
   - Check for SQL injection patterns
4. Aggregate errors and report

## Current State in DotCelery

### Why Not Used?

DotCelery uses **embedded SQL** in C# migration classes:

```csharp
// src/DotCelery.Backend.Postgres/Migrations/Results/M20250109001_CreateTaskResultsTable.cs
public override async ValueTask UpAsync(IMigrationContext context, ...)
{
    await context.ExecuteAsync(@"
        CREATE TABLE IF NOT EXISTS celery_task_results (
            task_id VARCHAR(255) PRIMARY KEY,
            state VARCHAR(20) NOT NULL,
            ...
        )", cancellationToken);
}
```

### Future Use Cases

This tool becomes useful if DotCelery migrates to separate `.sql` files:

```
src/DotCelery.Backend.Postgres/Migrations/Sql/
├── 001_CreateTaskResultsTable.sql
├── 002_CreateOutboxTable.sql
└── 003_CreateInboxTable.sql
```

## Limitations

1. **Basic Validation Only**: Does not use PostgreSQL parser
2. **No Semantic Checks**: Won't catch logical errors
3. **No Type Validation**: Column types not verified
4. **Limited Error Messages**: Generic error reporting

## Alternatives

For more comprehensive validation, consider:

- **Npgsql Parser**: Full PostgreSQL syntax validation
- **pgFormatter**: SQL formatting and validation
- **sqlfluff**: Linter with PostgreSQL support
- **Database Migration Tools**: Flyway, Liquibase with validation

## Contributing

To improve the validator:

1. Add unit tests in `tests/DotCelery.Build.SqlValidator.Tests/`
2. Enhance error messages with line/column numbers
3. Add PostgreSQL-specific syntax checks
4. Support configuration file (`.sqlvalidator.json`)
5. Generate structured reports (JSON, JUnit XML)

## License

Same as DotCelery - MIT License

---

**See Also:**
- [Complete Review](../../docs/SQL_VALIDATOR_REVIEW.md) - Detailed analysis and recommendations
- [Solution Review](../../docs/SOLUTION_REVIEW.md) - Project integration status
