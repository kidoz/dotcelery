using DotCelery.Backend.Postgres;
using DotCelery.Backend.Postgres.Storage;
using DotCelery.Storage.Sql.Migrations;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Integration.Postgres;

/// <summary>
/// Creates migrators with the PostgreSQL dialect for tests.
/// </summary>
internal static class PostgresTestMigrator
{
    public static SqlMigrator Create(
        IPostgresDataSourceProvider dataSources,
        IEnumerable<SqlMigrationModule> modules
    ) =>
        new(
            PostgresDialect.Instance,
            dataSources,
            modules,
            Options.Create(new SqlMigrationOptions()),
            NullLogger<SqlMigrator>.Instance
        );
}
