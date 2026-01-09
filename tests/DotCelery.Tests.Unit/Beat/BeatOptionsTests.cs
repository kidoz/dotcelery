using DotCelery.Beat;

namespace DotCelery.Tests.Unit.Beat;

public class BeatOptionsTests
{
    [Fact]
    public void BeatOptions_DefaultValues_SetCorrectly()
    {
        var options = new BeatOptions();

        Assert.Equal(TimeSpan.FromSeconds(1), options.CheckInterval);
        Assert.Equal(TimeSpan.Zero, options.MaxJitter);
        Assert.False(options.PersistState);
        Assert.Null(options.StatePath);
        Assert.Equal("celery-beat", options.SchedulerName);
        Assert.False(options.RunMissedOnStartup);
    }

    [Fact]
    public void BeatOptions_CustomValues_SetCorrectly()
    {
        var options = new BeatOptions
        {
            CheckInterval = TimeSpan.FromSeconds(5),
            MaxJitter = TimeSpan.FromMilliseconds(500),
            PersistState = true,
            StatePath = "/var/celerybeat-state",
            SchedulerName = "my-scheduler",
            RunMissedOnStartup = true,
        };

        Assert.Equal(TimeSpan.FromSeconds(5), options.CheckInterval);
        Assert.Equal(TimeSpan.FromMilliseconds(500), options.MaxJitter);
        Assert.True(options.PersistState);
        Assert.Equal("/var/celerybeat-state", options.StatePath);
        Assert.Equal("my-scheduler", options.SchedulerName);
        Assert.True(options.RunMissedOnStartup);
    }
}
