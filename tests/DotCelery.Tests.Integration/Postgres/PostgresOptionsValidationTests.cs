using DotCelery.Backend.Postgres.DeadLetter;
using DotCelery.Backend.Postgres.DelayedMessageStore;
using DotCelery.Backend.Postgres.Historical;
using DotCelery.Backend.Postgres.Metrics;
using DotCelery.Backend.Postgres.RateLimiting;
using DotCelery.Backend.Postgres.Sagas;

namespace DotCelery.Tests.Integration.Postgres;

/// <summary>
/// Locks in identifier validation across every Postgres options class so a
/// configuration mistake (or hostile config source) cannot smuggle an arbitrary
/// fragment into the schema/table identifiers used to build SQL.
/// </summary>
public sealed class PostgresOptionsValidationTests
{
    [Theory]
    [InlineData("schema; DROP TABLE x; --")]
    [InlineData("\"x")]
    [InlineData("9starts_with_digit")]
    [InlineData("")]
    public void DeadLetterStoreOptions_RejectsBadIdentifiers(string bad)
    {
        var options = new PostgresDeadLetterStoreOptions();
        Assert.Throws<ArgumentException>(() => options.Schema = bad);
        Assert.Throws<ArgumentException>(() => options.TableName = bad);
    }

    [Theory]
    [InlineData("schema; DROP TABLE x; --")]
    [InlineData("name with spaces")]
    public void DelayedMessageStoreOptions_RejectsBadIdentifiers(string bad)
    {
        var options = new PostgresDelayedMessageStoreOptions();
        Assert.Throws<ArgumentException>(() => options.Schema = bad);
        Assert.Throws<ArgumentException>(() => options.TableName = bad);
    }

    [Fact]
    public void SagaStoreOptions_RejectsBadIdentifiers_OnAllTableNames()
    {
        var options = new PostgresSagaStoreOptions();
        const string bad = "x\";DROP";

        Assert.Throws<ArgumentException>(() => options.Schema = bad);
        Assert.Throws<ArgumentException>(() => options.SagasTableName = bad);
        Assert.Throws<ArgumentException>(() => options.SagaStepsTableName = bad);
        Assert.Throws<ArgumentException>(() => options.TaskSagaTableName = bad);
    }

    [Fact]
    public void QueueMetricsOptions_RejectsBadIdentifiers()
    {
        var options = new PostgresQueueMetricsOptions();
        const string bad = "evil; --";

        Assert.Throws<ArgumentException>(() => options.Schema = bad);
        Assert.Throws<ArgumentException>(() => options.MetricsTableName = bad);
        Assert.Throws<ArgumentException>(() => options.RunningTasksTableName = bad);
    }

    [Fact]
    public void RateLimiterOptions_RejectsBadIdentifiers()
    {
        var options = new PostgresRateLimiterOptions();
        const string bad = "x'; DROP TABLE y; --";

        Assert.Throws<ArgumentException>(() => options.Schema = bad);
        Assert.Throws<ArgumentException>(() => options.TableName = bad);
    }

    [Fact]
    public void HistoricalDataStoreOptions_RejectsBadIdentifiers()
    {
        var options = new PostgresHistoricalDataStoreOptions();
        const string bad = "x; DROP";

        Assert.Throws<ArgumentException>(() => options.Schema = bad);
        Assert.Throws<ArgumentException>(() => options.SnapshotsTableName = bad);
    }

    [Fact]
    public void AllOptionsClasses_AcceptValidIdentifiers()
    {
        // Valid identifiers should not throw on any options class.
        var deadLetter = new PostgresDeadLetterStoreOptions { Schema = "audit", TableName = "dl" };
        var delayed = new PostgresDelayedMessageStoreOptions
        {
            Schema = "audit",
            TableName = "delayed",
        };
        var sagas = new PostgresSagaStoreOptions
        {
            Schema = "audit",
            SagasTableName = "s",
            SagaStepsTableName = "ss",
            TaskSagaTableName = "ts",
        };
        var metrics = new PostgresQueueMetricsOptions
        {
            Schema = "audit",
            MetricsTableName = "qm",
            RunningTasksTableName = "rt",
        };
        var rateLimit = new PostgresRateLimiterOptions { Schema = "audit", TableName = "rl" };
        var historical = new PostgresHistoricalDataStoreOptions
        {
            Schema = "audit",
            SnapshotsTableName = "ms",
        };

        Assert.Equal("audit", deadLetter.Schema);
        Assert.Equal("delayed", delayed.TableName);
        Assert.Equal("s", sagas.SagasTableName);
        Assert.Equal("rt", metrics.RunningTasksTableName);
        Assert.Equal("rl", rateLimit.TableName);
        Assert.Equal("ms", historical.SnapshotsTableName);
    }
}
