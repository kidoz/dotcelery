namespace DotCelery.Tests.Unit.Beat;

using DotCelery.Backend.InMemory.Storage;
using DotCelery.Beat;
using DotCelery.Broker.InMemory;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Canvas;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;

public class BeatSchedulerServiceTests : IAsyncDisposable
{
    private const string EntryName = "test-entry";

    private static readonly DateTimeOffset Start = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan CheckInterval = TimeSpan.FromMilliseconds(100);

    private readonly InMemoryBroker _broker = new();
    private readonly JsonMessageSerializer _serializer = new();
    private readonly FakeTimeProvider _time = new(Start);

    [Fact]
    public async Task ExecuteAsync_DueEntry_PublishesTask()
    {
        var schedule = new Schedule
        {
            new ScheduleEntry
            {
                Name = EntryName,
                Task = new Signature { TaskName = "test.task" },
                Interval = TimeSpan.FromMinutes(1),
                LastRunTime = Start - TimeSpan.FromMinutes(2),
            },
        };
        // The run missed while the scheduler was down is run once at startup
        var service = CreateService(
            schedule,
            new BeatOptions { CheckInterval = CheckInterval, RunMissedOnStartup = true }
        );

        await using var running = await StartAsync(service);

        await AdvanceUntilAsync(_time, () => QueueLength() >= 1);

        Assert.Equal(1, QueueLength());
    }

    [Fact]
    public async Task ExecuteAsync_NewIntervalEntry_RunsOneIntervalAfterStartup()
    {
        var service = CreateService(CreateIntervalSchedule(EntryName));

        await using var running = await StartAsync(service);

        // A new entry waits for its interval instead of running at startup
        await AdvanceAsync(_time, TimeSpan.FromSeconds(50));
        Assert.Equal(0, QueueLength());

        await AdvanceUntilAsync(_time, () => QueueLength() >= 1);
        Assert.Equal(1, QueueLength());
    }

    [Fact]
    public async Task ExecuteAsync_IntervalLongerThanADay_RunsAfterTheInterval()
    {
        var schedule = new Schedule
        {
            new ScheduleEntry
            {
                Name = EntryName,
                Task = new Signature { TaskName = "long.task" },
                Interval = TimeSpan.FromDays(2),
            },
        };
        var service = CreateService(schedule);

        await using var running = await StartAsync(service);

        // The baseline used to move with the clock, so a schedule longer than a day never ran
        await AdvanceAsync(_time, TimeSpan.FromHours(23), TimeSpan.FromHours(1));
        Assert.Equal(0, QueueLength());

        await AdvanceUntilAsync(_time, () => QueueLength() >= 1, TimeSpan.FromHours(1));
        Assert.Equal(1, QueueLength());
    }

    [Fact]
    public async Task ExecuteAsync_NewCronEntry_RunsAtItsNextOccurrence()
    {
        var schedule = new Schedule
        {
            new ScheduleEntry
            {
                Name = EntryName,
                Task = new Signature { TaskName = "cron.task" },
                Cron = "0 0 * * *",
            },
        };
        var service = CreateService(schedule);

        await using var running = await StartAsync(service);

        // A new cron entry used to be due at startup, because its baseline was a day back
        await AdvanceAsync(_time, TimeSpan.FromHours(1), TimeSpan.FromMinutes(30));
        Assert.Equal(0, QueueLength());

        await AdvanceUntilAsync(_time, () => QueueLength() >= 1, TimeSpan.FromMinutes(30));
        Assert.Equal(1, QueueLength());
        Assert.Equal(new DateTimeOffset(2026, 1, 2, 0, 0, 0, TimeSpan.Zero), _time.GetUtcNow());
    }

    [Fact]
    public async Task ExecuteAsync_WithJitter_DelaysTask()
    {
        var schedule = new Schedule
        {
            new ScheduleEntry
            {
                Name = EntryName,
                Task = new Signature { TaskName = "jitter.task" },
                Interval = TimeSpan.FromMinutes(1),
                LastRunTime = Start - TimeSpan.FromMinutes(2),
            },
        };
        var service = CreateService(
            schedule,
            new BeatOptions
            {
                CheckInterval = CheckInterval,
                MaxJitter = TimeSpan.FromMilliseconds(100),
                RunMissedOnStartup = true,
            }
        );

        await using var running = await StartAsync(service);

        await AdvanceUntilAsync(_time, () => QueueLength() >= 1);

        Assert.Equal(1, QueueLength());
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
                    Interval = TimeSpan.FromMinutes(1),
                    LastRunTime = Start - TimeSpan.FromMinutes(2),
                }
            );
        }

        var service = CreateService(
            schedule,
            new BeatOptions
            {
                CheckInterval = CheckInterval,
                MaxJitter = TimeSpan.FromMilliseconds(5),
                RunMissedOnStartup = true,
            }
        );

        var exception = await Record.ExceptionAsync(async () =>
        {
            await using var running = await StartAsync(service);
            await AdvanceAsync(_time, TimeSpan.FromSeconds(1));
        });

        Assert.Null(exception);
        Assert.Equal(10, QueueLength());
    }

    [Fact]
    public async Task ExecuteAsync_EmptySchedule_DoesNotThrow()
    {
        var service = CreateService(new Schedule());

        var exception = await Record.ExceptionAsync(async () =>
        {
            await using var running = await StartAsync(service);
            await AdvanceAsync(_time, TimeSpan.FromSeconds(1));
        });

        Assert.Null(exception);
    }

    [Fact]
    public async Task ExecuteAsync_SchedulersSharingStorage_OnlyTheLeaderRunsTheSchedule()
    {
        var storage = new InMemoryStorageProvider();
        await using var otherBroker = new InMemoryBroker();
        var step = TimeSpan.FromSeconds(1);

        await using var first = await StartAsync(
            CreateService(CreateIntervalSchedule(EntryName), LeaderOptions(), storage: storage)
        );
        await using var second = await StartAsync(
            CreateService(
                CreateIntervalSchedule(EntryName),
                LeaderOptions(),
                broker: otherBroker,
                storage: storage
            )
        );

        await AdvanceUntilAsync(
            _time,
            () => QueueLength() + otherBroker.GetQueueLength("celery") >= 1,
            step
        );

        // One scheduler holds the lease and runs the schedule; the other stays a standby
        await AdvanceAsync(_time, TimeSpan.FromMinutes(3), step);
        Assert.NotEqual(QueueLength() >= 1, otherBroker.GetQueueLength("celery") >= 1);
    }

    [Fact]
    public async Task ExecuteAsync_WhenTheLeaderStops_AnotherSchedulerTakesOver()
    {
        var storage = new InMemoryStorageProvider();
        await using var otherBroker = new InMemoryBroker();
        var step = TimeSpan.FromSeconds(1);

        await using var first = await StartAsync(
            CreateService(CreateIntervalSchedule(EntryName), LeaderOptions(), storage: storage)
        );
        await using var second = await StartAsync(
            CreateService(
                CreateIntervalSchedule(EntryName),
                LeaderOptions(),
                broker: otherBroker,
                storage: storage
            )
        );

        await AdvanceUntilAsync(
            _time,
            () => QueueLength() + otherBroker.GetQueueLength("celery") >= 1,
            step
        );

        // Whichever scheduler took the lease is the one that runs the schedule
        var firstLeads = QueueLength() >= 1;
        Assert.NotEqual(firstLeads, otherBroker.GetQueueLength("celery") >= 1);

        if (firstLeads)
        {
            await first.DisposeAsync();
        }
        else
        {
            await second.DisposeAsync();
        }

        await AdvanceUntilAsync(
            _time,
            () => (firstLeads ? otherBroker.GetQueueLength("celery") : QueueLength()) >= 1,
            step
        );
    }

    [Fact]
    public async Task ExecuteAsync_WithoutStorage_EverySchedulerRunsTheSchedule()
    {
        await using var otherBroker = new InMemoryBroker();

        await using var first = await StartAsync(CreateService(CreateIntervalSchedule(EntryName)));
        await using var second = await StartAsync(
            CreateService(CreateIntervalSchedule(EntryName), broker: otherBroker)
        );

        await AdvanceUntilAsync(_time, () => otherBroker.GetQueueLength("celery") >= 1);

        Assert.Equal(1, QueueLength());
        Assert.Equal(1, otherBroker.GetQueueLength("celery"));
    }

    [Fact]
    public async Task ExecuteAsync_RunMissedOnStartup_RunsAMissedEntryOnce()
    {
        var statePath = NewStatePath();
        await using var restartedBroker = new InMemoryBroker();

        try
        {
            await RunEntryOnceAsync(statePath, Start);

            // The scheduler restarts five hours later and catches the missed run up once
            var restart = new FakeTimeProvider(Start.AddHours(5));
            await using var restarted = await StartAsync(
                CreateService(
                    CreateIntervalSchedule(EntryName),
                    new BeatOptions
                    {
                        CheckInterval = CheckInterval,
                        PersistState = true,
                        StatePath = statePath,
                        RunMissedOnStartup = true,
                    },
                    broker: restartedBroker,
                    time: restart
                )
            );

            await AdvanceUntilAsync(restart, () => restartedBroker.GetQueueLength("celery") >= 1);

            Assert.Equal(1, restartedBroker.GetQueueLength("celery"));
        }
        finally
        {
            DeleteStatePath(statePath);
        }
    }

    [Fact]
    public async Task ExecuteAsync_WhenMissedRunsAreSkipped_ResumesOnSchedule()
    {
        var statePath = NewStatePath();
        await using var restartedBroker = new InMemoryBroker();

        try
        {
            await RunEntryOnceAsync(statePath, Start);

            // The scheduler restarts five hours later and skips what it missed
            var restart = new FakeTimeProvider(Start.AddHours(5));
            await using var restarted = await StartAsync(
                CreateService(
                    CreateIntervalSchedule(EntryName),
                    new BeatOptions
                    {
                        CheckInterval = CheckInterval,
                        PersistState = true,
                        StatePath = statePath,
                        RunMissedOnStartup = false,
                    },
                    broker: restartedBroker,
                    time: restart
                )
            );

            await AdvanceAsync(restart, TimeSpan.FromSeconds(50));
            Assert.Equal(0, restartedBroker.GetQueueLength("celery"));

            await AdvanceUntilAsync(restart, () => restartedBroker.GetQueueLength("celery") >= 1);
            Assert.Equal(1, restartedBroker.GetQueueLength("celery"));
        }
        finally
        {
            DeleteStatePath(statePath);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _broker.DisposeAsync();
    }

    // The lease lasts three checks, so a scheduler that keeps checking keeps the schedule
    private static BeatOptions LeaderOptions() => new() { CheckInterval = TimeSpan.FromSeconds(1) };

    private static Schedule CreateIntervalSchedule(string name) =>
        new()
        {
            new ScheduleEntry
            {
                Name = name,
                Task = new Signature { TaskName = "interval.task" },
                Interval = TimeSpan.FromMinutes(1),
            },
        };

    private static string NewStatePath() =>
        Path.Combine(Path.GetTempPath(), $"beat-{Guid.NewGuid():N}.state.json");

    private static void DeleteStatePath(string path)
    {
        File.Delete(path);
        File.Delete(path + ".tmp");
    }

    // Runs the entry once, which leaves its last run time in the state file
    private async Task RunEntryOnceAsync(string statePath, DateTimeOffset start)
    {
        var time = new FakeTimeProvider(start);
        await using var broker = new InMemoryBroker();
        await using var service = await StartAsync(
            CreateService(
                CreateIntervalSchedule(EntryName),
                new BeatOptions
                {
                    CheckInterval = CheckInterval,
                    PersistState = true,
                    StatePath = statePath,
                },
                broker: broker,
                time: time
            )
        );

        await AdvanceUntilAsync(time, () => broker.GetQueueLength("celery") >= 1);

        Assert.True(File.Exists(statePath));
    }

    private BeatSchedulerService CreateService(
        Schedule schedule,
        BeatOptions? options = null,
        IStorageProvider? storage = null,
        IMessageBroker? broker = null,
        TimeProvider? time = null
    ) =>
        new(
            schedule,
            broker ?? _broker,
            _serializer,
            Options.Create(options ?? new BeatOptions { CheckInterval = CheckInterval }),
            NullLogger<BeatSchedulerService>.Instance,
            time ?? _time,
            storage
        );

    private static async Task<RunningService> StartAsync(BeatSchedulerService service)
    {
        await service.StartAsync(CancellationToken.None);
        return new RunningService(service);
    }

    private long QueueLength() => _broker.GetQueueLength("celery");

    private static async Task AdvanceUntilAsync(
        FakeTimeProvider time,
        Func<bool> condition,
        TimeSpan? step = null
    )
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        while (!condition())
        {
            timeout.Token.ThrowIfCancellationRequested();
            time.Advance(step ?? TimeSpan.FromSeconds(10));
            await Task.Delay(20, CancellationToken.None);
        }
    }

    private static async Task AdvanceAsync(
        FakeTimeProvider time,
        TimeSpan amount,
        TimeSpan? step = null
    )
    {
        var stepSize = step ?? TimeSpan.FromSeconds(10);

        for (var elapsed = TimeSpan.Zero; elapsed < amount; elapsed += stepSize)
        {
            time.Advance(stepSize);
            await Task.Delay(20, CancellationToken.None);
        }
    }

    private sealed class RunningService(BeatSchedulerService service) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await service.StopAsync(CancellationToken.None);
            service.Dispose();
        }
    }
}
