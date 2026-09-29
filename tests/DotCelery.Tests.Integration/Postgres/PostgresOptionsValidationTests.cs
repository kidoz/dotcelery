using DotCelery.Backend.Postgres.Storage;

namespace DotCelery.Tests.Integration.Postgres;

/// <summary>
/// Locks in validation of the PostgreSQL storage options so a configuration mistake (or hostile
/// config source) cannot smuggle an arbitrary fragment into the schema identifier used to build SQL.
/// </summary>
public sealed class PostgresOptionsValidationTests
{
    [Theory]
    [InlineData("schema; DROP TABLE x; --")]
    [InlineData("\"x")]
    [InlineData("9starts_with_digit")]
    [InlineData("name with spaces")]
    [InlineData("")]
    public void Schema_RejectsBadIdentifiers(string bad)
    {
        var options = new PostgresStorageOptions();

        Assert.Throws<ArgumentException>(() => options.Schema = bad);
    }

    [Fact]
    public void ConnectionStringAndCommandTimeout_RejectInvalidValues()
    {
        var options = new PostgresStorageOptions();

        Assert.Throws<ArgumentException>(() => options.ConnectionString = " ");
        Assert.Throws<ArgumentOutOfRangeException>(() => options.CommandTimeout = TimeSpan.Zero);
    }

    [Fact]
    public void Options_AcceptValidValues()
    {
        var options = new PostgresStorageOptions
        {
            ConnectionString = "Host=db;Database=jobs",
            Schema = "audit_2",
            CommandTimeout = TimeSpan.FromSeconds(5),
        };

        Assert.Equal("audit_2", options.Schema);
    }
}
