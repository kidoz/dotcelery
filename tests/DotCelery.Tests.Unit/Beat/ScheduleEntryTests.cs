using DotCelery.Beat;
using DotCelery.Core.Canvas;

namespace DotCelery.Tests.Unit.Beat;

public class ScheduleEntryTests
{
    [Fact]
    public void ScheduleEntry_RequiredProperties_SetCorrectly()
    {
        var sig = new Signature { TaskName = "test.task" };

        var entry = new ScheduleEntry { Name = "test-entry", Task = sig };

        Assert.Equal("test-entry", entry.Name);
        Assert.Same(sig, entry.Task);
        Assert.True(entry.Enabled);
        Assert.Null(entry.LastRunTime);
    }

    [Fact]
    public void ScheduleEntry_CronExpression_SetCorrectly()
    {
        var entry = new ScheduleEntry
        {
            Name = "cron-entry",
            Task = new Signature { TaskName = "test.task" },
            Cron = "*/5 * * * *",
        };

        Assert.Equal("*/5 * * * *", entry.Cron);
        Assert.Null(entry.Interval);
    }

    [Fact]
    public void ScheduleEntry_Interval_SetCorrectly()
    {
        var entry = new ScheduleEntry
        {
            Name = "interval-entry",
            Task = new Signature { TaskName = "test.task" },
            Interval = TimeSpan.FromMinutes(5),
        };

        Assert.Equal(TimeSpan.FromMinutes(5), entry.Interval);
        Assert.Null(entry.Cron);
    }

    [Fact]
    public void ScheduleEntry_Options_SetCorrectly()
    {
        var options = new ScheduleOptions
        {
            Queue = "custom",
            Priority = 5,
            ExpiresIn = TimeSpan.FromHours(1),
        };

        var entry = new ScheduleEntry
        {
            Name = "test-entry",
            Task = new Signature { TaskName = "test.task" },
            Options = options,
        };

        Assert.NotNull(entry.Options);
        Assert.Equal("custom", entry.Options.Queue);
        Assert.Equal(5, entry.Options.Priority);
        Assert.Equal(TimeSpan.FromHours(1), entry.Options.ExpiresIn);
    }

    [Fact]
    public void GetNextRunTime_Interval_WithLastRunTime_ReturnsLastPlusInterval()
    {
        var lastRun = DateTimeOffset.UtcNow.AddMinutes(-10);
        var entry = new ScheduleEntry
        {
            Name = "test-entry",
            Task = new Signature { TaskName = "test.task" },
            Interval = TimeSpan.FromMinutes(5),
            LastRunTime = lastRun,
        };

        var nextRun = entry.GetNextRunTime(DateTimeOffset.UtcNow);

        Assert.NotNull(nextRun);
        Assert.Equal(lastRun + TimeSpan.FromMinutes(5), nextRun);
    }

    [Fact]
    public void GetNextRunTime_Interval_NoLastRunTime_ReturnsFromPlusInterval()
    {
        var from = DateTimeOffset.UtcNow;
        var entry = new ScheduleEntry
        {
            Name = "test-entry",
            Task = new Signature { TaskName = "test.task" },
            Interval = TimeSpan.FromMinutes(5),
        };

        var nextRun = entry.GetNextRunTime(from);

        Assert.NotNull(nextRun);
        Assert.Equal(from + TimeSpan.FromMinutes(5), nextRun);
    }

    [Fact]
    public void GetNextRunTime_Disabled_ReturnsNull()
    {
        var entry = new ScheduleEntry
        {
            Name = "test-entry",
            Task = new Signature { TaskName = "test.task" },
            Interval = TimeSpan.FromMinutes(5),
            Enabled = false,
        };

        var nextRun = entry.GetNextRunTime(DateTimeOffset.UtcNow);

        Assert.Null(nextRun);
    }

    [Fact]
    public void GetNextRunTime_NoSchedule_ReturnsNull()
    {
        var entry = new ScheduleEntry
        {
            Name = "test-entry",
            Task = new Signature { TaskName = "test.task" },
        };

        var nextRun = entry.GetNextRunTime(DateTimeOffset.UtcNow);

        Assert.Null(nextRun);
    }

    [Fact]
    public void GetNextRunTime_Cron_ReturnsNextOccurrence()
    {
        var from = new DateTimeOffset(2024, 1, 1, 12, 0, 0, TimeSpan.Zero);
        var entry = new ScheduleEntry
        {
            Name = "test-entry",
            Task = new Signature { TaskName = "test.task" },
            Cron = "0 * * * *", // Every hour at minute 0
        };

        var nextRun = entry.GetNextRunTime(from);

        Assert.NotNull(nextRun);
        Assert.Equal(13, nextRun.Value.Hour);
        Assert.Equal(0, nextRun.Value.Minute);
    }

    [Fact]
    public void ShouldRun_Disabled_ReturnsFalse()
    {
        var entry = new ScheduleEntry
        {
            Name = "test-entry",
            Task = new Signature { TaskName = "test.task" },
            Interval = TimeSpan.FromMinutes(5),
            Enabled = false,
        };

        Assert.False(entry.ShouldRun(DateTimeOffset.UtcNow));
    }

    [Fact]
    public void ShouldRun_Interval_PastDue_ReturnsTrue()
    {
        var entry = new ScheduleEntry
        {
            Name = "test-entry",
            Task = new Signature { TaskName = "test.task" },
            Interval = TimeSpan.FromMinutes(5),
            LastRunTime = DateTimeOffset.UtcNow.AddMinutes(-10),
        };

        Assert.True(entry.ShouldRun(DateTimeOffset.UtcNow));
    }

    [Fact]
    public void ShouldRun_Interval_NotYetDue_ReturnsFalse()
    {
        var entry = new ScheduleEntry
        {
            Name = "test-entry",
            Task = new Signature { TaskName = "test.task" },
            Interval = TimeSpan.FromMinutes(10),
            LastRunTime = DateTimeOffset.UtcNow.AddMinutes(-5),
        };

        Assert.False(entry.ShouldRun(DateTimeOffset.UtcNow));
    }

    [Fact]
    public void ShouldRun_NoLastRunTime_ReturnsTrue()
    {
        var entry = new ScheduleEntry
        {
            Name = "test-entry",
            Task = new Signature { TaskName = "test.task" },
            Interval = TimeSpan.FromMinutes(5),
        };

        Assert.True(entry.ShouldRun(DateTimeOffset.UtcNow));
    }
}
