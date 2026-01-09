using DotCelery.Beat;
using DotCelery.Core.Canvas;

namespace DotCelery.Tests.Unit.Beat;

public class ScheduleTests
{
    [Fact]
    public void Schedule_Empty_CountIsZero()
    {
        var schedule = new Schedule();

        Assert.Equal(0, schedule.Count);
    }

    [Fact]
    public void Schedule_Add_IncreasesCount()
    {
        var schedule = new Schedule();
        var entry = CreateEntry("entry1");

        schedule.Add(entry);

        Assert.Equal(1, schedule.Count);
    }

    [Fact]
    public void Schedule_Add_NullEntry_ThrowsArgumentNullException()
    {
        var schedule = new Schedule();

        Assert.Throws<ArgumentNullException>(() => schedule.Add(null!));
    }

    [Fact]
    public void Schedule_Add_DuplicateName_OverwritesExisting()
    {
        var schedule = new Schedule();
        var entry1 = new ScheduleEntry
        {
            Name = "entry",
            Task = new Signature { TaskName = "task1" },
        };
        var entry2 = new ScheduleEntry
        {
            Name = "entry",
            Task = new Signature { TaskName = "task2" },
        };

        schedule.Add(entry1);
        schedule.Add(entry2);

        Assert.Equal(1, schedule.Count);
        Assert.Equal("task2", schedule.Get("entry")?.Task.TaskName);
    }

    [Fact]
    public void Schedule_Remove_ExistingEntry_ReturnsTrue()
    {
        var schedule = new Schedule();
        schedule.Add(CreateEntry("entry1"));

        var result = schedule.Remove("entry1");

        Assert.True(result);
        Assert.Equal(0, schedule.Count);
    }

    [Fact]
    public void Schedule_Remove_NonExistentEntry_ReturnsFalse()
    {
        var schedule = new Schedule();

        var result = schedule.Remove("non-existent");

        Assert.False(result);
    }

    [Fact]
    public void Schedule_Get_ExistingEntry_ReturnsEntry()
    {
        var schedule = new Schedule();
        var entry = CreateEntry("entry1");
        schedule.Add(entry);

        var result = schedule.Get("entry1");

        Assert.NotNull(result);
        Assert.Same(entry, result);
    }

    [Fact]
    public void Schedule_Get_NonExistentEntry_ReturnsNull()
    {
        var schedule = new Schedule();

        var result = schedule.Get("non-existent");

        Assert.Null(result);
    }

    [Fact]
    public void Schedule_Contains_ExistingEntry_ReturnsTrue()
    {
        var schedule = new Schedule();
        schedule.Add(CreateEntry("entry1"));

        Assert.True(schedule.Contains("entry1"));
    }

    [Fact]
    public void Schedule_Contains_NonExistentEntry_ReturnsFalse()
    {
        var schedule = new Schedule();

        Assert.False(schedule.Contains("entry1"));
    }

    [Fact]
    public void Schedule_GetDueEntries_ReturnsDueEntries()
    {
        var schedule = new Schedule();
        var now = DateTimeOffset.UtcNow;

        // Due entry (last run was 10 min ago, interval is 5 min)
        var dueEntry = new ScheduleEntry
        {
            Name = "due",
            Task = new Signature { TaskName = "task" },
            Interval = TimeSpan.FromMinutes(5),
            LastRunTime = now.AddMinutes(-10),
        };

        // Not due entry (last run was 2 min ago, interval is 5 min)
        var notDueEntry = new ScheduleEntry
        {
            Name = "not-due",
            Task = new Signature { TaskName = "task" },
            Interval = TimeSpan.FromMinutes(5),
            LastRunTime = now.AddMinutes(-2),
        };

        schedule.Add(dueEntry);
        schedule.Add(notDueEntry);

        var dueEntries = schedule.GetDueEntries(now).ToList();

        Assert.Single(dueEntries);
        Assert.Equal("due", dueEntries[0].Name);
    }

    [Fact]
    public void Schedule_GetDueEntries_DisabledEntry_NotReturned()
    {
        var schedule = new Schedule();
        var entry = new ScheduleEntry
        {
            Name = "disabled",
            Task = new Signature { TaskName = "task" },
            Interval = TimeSpan.FromMinutes(5),
            LastRunTime = DateTimeOffset.UtcNow.AddMinutes(-10),
            Enabled = false,
        };

        schedule.Add(entry);

        var dueEntries = schedule.GetDueEntries(DateTimeOffset.UtcNow).ToList();

        Assert.Empty(dueEntries);
    }

    [Fact]
    public void Schedule_Enumerable_ReturnsAllEntries()
    {
        var schedule = new Schedule();
        schedule.Add(CreateEntry("entry1"));
        schedule.Add(CreateEntry("entry2"));
        schedule.Add(CreateEntry("entry3"));

        var entries = schedule.ToList();

        Assert.Equal(3, entries.Count);
    }

    [Fact]
    public void Schedule_Enumerable_NonGeneric_Works()
    {
        var schedule = new Schedule();
        schedule.Add(CreateEntry("entry1"));

        var count = 0;
        foreach (var _ in (System.Collections.IEnumerable)schedule)
        {
            count++;
        }

        Assert.Equal(1, count);
    }

    private static ScheduleEntry CreateEntry(string name)
    {
        return new ScheduleEntry
        {
            Name = name,
            Task = new Signature { TaskName = $"{name}.task" },
            Interval = TimeSpan.FromMinutes(5),
        };
    }
}
