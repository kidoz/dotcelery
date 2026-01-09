namespace DotCelery.Tests.Unit.Beat;

using DotCelery.Beat;
using DotCelery.Broker.InMemory;
using DotCelery.Core.Canvas;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

public class BeatSchedulerServiceTests : IAsyncDisposable
{
    private readonly InMemoryBroker _broker = new();
    private readonly JsonMessageSerializer _serializer = new();

    [Fact]
    public async Task ExecuteAsync_DueEntry_PublishesTask()
    {
        var schedule = new Schedule
        {
            new ScheduleEntry
            {
                Name = "test-entry",
                Task = new Signature { TaskName = "test.task" },
                Interval = TimeSpan.FromSeconds(1),
            },
        };

        var options = Options.Create(
            new BeatOptions
            {
                CheckInterval = TimeSpan.FromMilliseconds(50),
                MaxJitter = TimeSpan.Zero,
            }
        );

        var service = new BeatSchedulerService(
            schedule,
            _broker,
            _serializer,
            options,
            NullLogger<BeatSchedulerService>.Instance
        );

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        await service.StartAsync(cts.Token);
        await Task.Delay(150, CancellationToken.None);
        await service.StopAsync(CancellationToken.None);

        // Should have published at least one task
        Assert.True(_broker.GetQueueLength("celery") >= 1);
    }

    [Fact]
    public async Task ExecuteAsync_WithJitter_DelaysTask()
    {
        var schedule = new Schedule
        {
            new ScheduleEntry
            {
                Name = "jitter-entry",
                Task = new Signature { TaskName = "jitter.task" },
                Interval = TimeSpan.FromMinutes(10), // Won't fire naturally
            },
        };

        // Force the entry to be due
        schedule.First().LastRunTime = DateTimeOffset.UtcNow.AddMinutes(-15);

        var options = Options.Create(
            new BeatOptions
            {
                CheckInterval = TimeSpan.FromMilliseconds(50),
                MaxJitter = TimeSpan.FromMilliseconds(100), // Some jitter
            }
        );

        var service = new BeatSchedulerService(
            schedule,
            _broker,
            _serializer,
            options,
            NullLogger<BeatSchedulerService>.Instance
        );

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        await service.StartAsync(cts.Token);
        await Task.Delay(100, CancellationToken.None);
        await service.StopAsync(CancellationToken.None);

        // Task should have been published (jitter just adds delay via ETA)
        Assert.True(_broker.GetQueueLength("celery") >= 1);
    }

    [Fact]
    public async Task ExecuteAsync_ConcurrentScheduling_DoesNotThrow()
    {
        // This tests that Random.Shared is thread-safe
        var schedule = new Schedule();
        for (var i = 0; i < 10; i++)
        {
            schedule.Add(
                new ScheduleEntry
                {
                    Name = $"entry-{i}",
                    Task = new Signature { TaskName = $"task.{i}" },
                    Interval = TimeSpan.FromMilliseconds(10),
                }
            );
        }

        var options = Options.Create(
            new BeatOptions
            {
                CheckInterval = TimeSpan.FromMilliseconds(10),
                MaxJitter = TimeSpan.FromMilliseconds(5),
            }
        );

        var service = new BeatSchedulerService(
            schedule,
            _broker,
            _serializer,
            options,
            NullLogger<BeatSchedulerService>.Instance
        );

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        // Should not throw any thread-safety exceptions
        var exception = await Record.ExceptionAsync(async () =>
        {
            await service.StartAsync(cts.Token);
            await Task.Delay(150, CancellationToken.None);
            await service.StopAsync(CancellationToken.None);
        });

        Assert.Null(exception);
    }

    [Fact]
    public async Task ExecuteAsync_EmptySchedule_DoesNotThrow()
    {
        var schedule = new Schedule();
        var options = Options.Create(
            new BeatOptions { CheckInterval = TimeSpan.FromMilliseconds(50) }
        );

        var service = new BeatSchedulerService(
            schedule,
            _broker,
            _serializer,
            options,
            NullLogger<BeatSchedulerService>.Instance
        );

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        var exception = await Record.ExceptionAsync(async () =>
        {
            await service.StartAsync(cts.Token);
            await Task.Delay(50, CancellationToken.None);
            await service.StopAsync(CancellationToken.None);
        });

        Assert.Null(exception);
    }

    public async ValueTask DisposeAsync()
    {
        await _broker.DisposeAsync();
    }
}
