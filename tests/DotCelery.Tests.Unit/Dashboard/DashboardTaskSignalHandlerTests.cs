using System.Text.Json;
using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Models;
using DotCelery.Core.MultiTenancy;
using DotCelery.Core.Signals;
using DotCelery.Dashboard;
using DotCelery.Dashboard.Hubs;
using DotCelery.Dashboard.Services;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;

namespace DotCelery.Tests.Unit.Dashboard;

/// <summary>
/// Runs task lifecycle signals through the dashboard's handler, so the records it keeps and the
/// notifications it pushes are proven together.
/// </summary>
public sealed class DashboardTaskSignalHandlerTests
{
    private readonly FakeTimeProvider _time = new(
        new DateTimeOffset(2026, 10, 8, 12, 0, 0, TimeSpan.Zero)
    );
    private readonly InMemoryStorageProvider _storage = new();
    private readonly TaskRecordStore _records;
    private readonly IClientProxy _taskProxy = Substitute.For<IClientProxy>();
    private readonly IClientProxy _updatesProxy = Substitute.For<IClientProxy>();
    private readonly IClientProxy _overviewProxy = Substitute.For<IClientProxy>();
    private readonly DashboardTaskSignalHandler _handler;

    public DashboardTaskSignalHandlerTests()
    {
        _records = new TaskRecordStore(Options.Create(new DashboardOptions()), _storage);

        var clients = Substitute.For<IHubClients>();
        clients.Group("task:t1").Returns(_taskProxy);
        clients.Group("task-updates").Returns(_updatesProxy);
        clients.Group("dashboard").Returns(_overviewProxy);
        var hubContext = Substitute.For<IHubContext<DashboardHub>>();
        hubContext.Clients.Returns(clients);

        _handler = new DashboardTaskSignalHandler(
            _records,
            Options.Create(new DashboardOptions()),
            NullLogger<DashboardTaskSignalHandler>.Instance,
            new DashboardNotificationService(hubContext)
        );
    }

    [Fact]
    public async Task TenantNotifications_UseDisjointGroupsForTheSameTaskId()
    {
        var clients = Substitute.For<IHubClients>();
        var tenantA = Substitute.For<IClientProxy>();
        var tenantB = Substitute.For<IClientProxy>();
        clients.Group("tenant:1:a:task:t1").Returns(tenantA);
        clients.Group("tenant:1:a:task-updates").Returns(tenantA);
        clients.Group("tenant:1:b:task:t1").Returns(tenantB);
        clients.Group("tenant:1:b:task-updates").Returns(tenantB);
        var hub = Substitute.For<IHubContext<DashboardHub>>();
        hub.Clients.Returns(clients);
        var notifications = new DashboardNotificationService(hub);
        using (TenantContext.SetTenant("a"))
            await notifications.NotifyTaskStateChangedAsync("t1", "Success");
        await tenantA
            .Received(2)
            .SendCoreAsync("TaskStateChanged", Arg.Any<object?[]>(), Arg.Any<CancellationToken>());
        await tenantB.DidNotReceiveWithAnyArgs().SendCoreAsync(default!, default!, default);
        using (TenantContext.SetTenant("b"))
            await notifications.NotifyTaskStateChangedAsync("t1", "Failure");
        await tenantB
            .Received(2)
            .SendCoreAsync("TaskStateChanged", Arg.Any<object?[]>(), Arg.Any<CancellationToken>());
        _ = clients.DidNotReceive().All;
        clients.DidNotReceive().Group("task:t1");
    }

    [Fact]
    public async Task PublishSignal_RecordsQueueWithoutOverwritingAnAlreadyCompletedTask()
    {
        var sent = _time.GetUtcNow();
        await _handler.HandleAsync(
            new TaskPostRunSignal
            {
                TaskId = "t1",
                TaskName = "tests.alpha",
                Timestamp = sent.AddSeconds(1),
                State = TaskState.Success,
                Duration = TimeSpan.FromMilliseconds(250),
                Worker = "w1",
            }
        );
        await _handler.HandleAsync(
            new AfterTaskPublishSignal
            {
                TaskId = "t1",
                TaskName = "tests.alpha",
                Queue = "billing",
                Timestamp = sent,
            }
        );
        var task = await _records.GetAsync("t1");
        Assert.NotNull(task);
        Assert.Equal("Success", task.State);
        Assert.Equal("billing", task.Queue);
        Assert.Equal(sent, task.SentAt);
        Assert.Equal(sent.AddSeconds(1), task.CompletedAt);
    }

    [Fact]
    public async Task StartingARetry_ClearsThePreviousAttemptOutcome()
    {
        var sent = _time.GetUtcNow();
        await _records.RecordAsync(
            new DotCelery.Core.Dashboard.TaskSummary
            {
                TaskId = "t1",
                TaskName = "tests.alpha",
                State = "Failure",
                Queue = "billing",
                CompletedAt = sent,
                Duration = TimeSpan.FromSeconds(1),
                ExceptionMessage = "Previous failure",
            }
        );
        await _handler.HandleAsync(CreatePreRun("t1", sent.AddSeconds(2)));
        var task = await _records.GetAsync("t1");
        Assert.NotNull(task);
        Assert.Equal("Started", task.State);
        Assert.Equal("billing", task.Queue);
        Assert.Null(task.CompletedAt);
        Assert.Null(task.Duration);
        Assert.Null(task.ExceptionMessage);
    }

    [Fact]
    public async Task ATaskThatRunsAndSucceeds_IsRecordedWithItsTiming()
    {
        var started = _time.GetUtcNow();

        await _handler.HandleAsync(CreatePreRun("t1", started));

        var running = await _records.GetAsync("t1");
        Assert.NotNull(running);
        Assert.Equal(TaskState.Started.ToString(), running.State);
        Assert.Equal(started, running.StartedAt);
        Assert.Equal("worker-1", running.Worker);
        Assert.Null(running.CompletedAt);

        var completed = started.AddSeconds(2);
        await _handler.HandleAsync(
            new TaskPostRunSignal
            {
                TaskId = "t1",
                TaskName = "tests.alpha",
                Timestamp = completed,
                State = TaskState.Success,
                Duration = TimeSpan.FromSeconds(2),
                Worker = "worker-1",
            }
        );

        var succeeded = await _records.GetAsync("t1");
        Assert.NotNull(succeeded);
        Assert.Equal(TaskState.Success.ToString(), succeeded.State);
        Assert.Equal(started, succeeded.StartedAt);
        Assert.Equal(completed, succeeded.CompletedAt);
        Assert.Equal(TimeSpan.FromSeconds(2), succeeded.Duration);
    }

    [Fact]
    public async Task ATaskThatFails_IsRecordedWithASanitizedError()
    {
        await _handler.HandleAsync(CreatePreRun("t1", _time.GetUtcNow()));
        await _handler.HandleAsync(
            new TaskFailureSignal
            {
                TaskId = "t1",
                TaskName = "tests.alpha",
                Timestamp = _time.GetUtcNow(),
                Exception = new InvalidOperationException("connection string is secret"),
                Duration = TimeSpan.FromMilliseconds(5),
            }
        );

        var failed = await _records.GetAsync("t1");
        Assert.NotNull(failed);
        Assert.Equal("InvalidOperationException: Task execution failed", failed.ExceptionMessage);
        Assert.DoesNotContain("secret", failed.ExceptionMessage);
    }

    [Fact]
    public async Task ARejectedTask_IsRecordedWithItsReason()
    {
        await _handler.HandleAsync(
            new TaskRejectedSignal
            {
                TaskId = "t1",
                TaskName = "tests.alpha",
                Timestamp = _time.GetUtcNow(),
                Reason = "signature invalid",
            }
        );

        var rejected = await _records.GetAsync("t1");
        Assert.NotNull(rejected);
        Assert.Equal(TaskState.Rejected.ToString(), rejected.State);
        Assert.Equal("Rejected: signature invalid", rejected.ExceptionMessage);
    }

    [Fact]
    public async Task StateChanges_ArePushedToTheDashboardClients()
    {
        await _handler.HandleAsync(CreatePreRun("t1", _time.GetUtcNow()));

        // The task's watchers and the general subscribers both hear about it
        await _taskProxy
            .Received(1)
            .SendCoreAsync(
                "TaskStateChanged",
                Arg.Is<object?[]>(args =>
                    args.Length == 1
                    && JsonSerializer.SerializeToElement(args[0]).GetProperty("State").GetString()
                        == TaskState.Started.ToString()
                ),
                Arg.Any<CancellationToken>()
            );

        await _updatesProxy
            .Received(1)
            .SendCoreAsync("TaskStateChanged", Arg.Any<object?[]>(), Arg.Any<CancellationToken>());

        await _overviewProxy
            .Received()
            .SendCoreAsync("OverviewChanged", Arg.Any<object?[]>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task OnlyTerminalPostRunSignals_CountTowardTheTaskName()
    {
        var metrics = new TaskMetricsStore(_storage, timeProvider: _time);
        var handler = new DashboardTaskSignalHandler(
            _records,
            Options.Create(new DashboardOptions()),
            NullLogger<DashboardTaskSignalHandler>.Instance,
            taskMetrics: metrics
        );

        // Starting and retrying do not settle the task, so they are not counted
        await handler.HandleAsync(CreatePreRun("t1", _time.GetUtcNow()));
        await handler.HandleAsync(CreatePostRun("t1", TaskState.Retry, TimeSpan.FromSeconds(1)));
        Assert.Empty(await metrics.GetTotalsAsync());

        await handler.HandleAsync(CreatePostRun("t1", TaskState.Success, TimeSpan.FromSeconds(2)));
        await handler.HandleAsync(CreatePreRun("t2", _time.GetUtcNow()));
        await handler.HandleAsync(CreatePostRun("t2", TaskState.Failure, TimeSpan.FromSeconds(3)));

        var totals = (await metrics.GetTotalsAsync())["tests.alpha"];
        Assert.Equal(1, totals.Succeeded);
        Assert.Equal(1, totals.Failed);
        Assert.Equal(
            TimeSpan.FromSeconds(2).Ticks + TimeSpan.FromSeconds(3).Ticks,
            totals.DurationTicks
        );
    }

    private static TaskPreRunSignal CreatePreRun(string taskId, DateTimeOffset timestamp) =>
        new()
        {
            TaskId = taskId,
            TaskName = "tests.alpha",
            Timestamp = timestamp,
            Worker = "worker-1",
        };

    private static TaskPostRunSignal CreatePostRun(
        string taskId,
        TaskState state,
        TimeSpan duration
    ) =>
        new()
        {
            TaskId = taskId,
            TaskName = "tests.alpha",
            Timestamp = DateTimeOffset.UtcNow,
            State = state,
            Duration = duration,
            Worker = "worker-1",
        };
}
