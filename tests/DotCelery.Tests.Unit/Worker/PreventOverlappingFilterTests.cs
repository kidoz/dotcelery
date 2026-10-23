using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Attributes;
using DotCelery.Core.Execution;
using DotCelery.Core.Filters;
using DotCelery.Core.Models;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Filters;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;

namespace DotCelery.Tests.Unit.Worker;

/// <summary>
/// Exercises the prevent-overlapping filter directly: a task marked with the attribute runs
/// alone, and the next one runs once the first stops tracking.
/// </summary>
public sealed class PreventOverlappingFilterTests : IAsyncDisposable
{
    private readonly ServiceProvider _services;
    private readonly ResultBackend _backend;
    private readonly PreventOverlappingFilter _filter;

    public PreventOverlappingFilterTests()
    {
        _services = new ServiceCollection().BuildServiceProvider();
        _backend = new ResultBackend(new InMemoryStorageProvider());
        _filter = new PreventOverlappingFilter(
            new TaskExecutionTracker(new InMemoryStorageProvider()),
            NullLogger<PreventOverlappingFilter>.Instance
        );
    }

    [Fact]
    public async Task ASecondExecution_IsSkippedUntilTheFirstStops()
    {
        var first = CreateExecutingContext("t1", typeof(OverlappingTask));
        await _filter.OnExecutingAsync(first, CancellationToken.None);
        Assert.False(first.SkipExecution);

        var second = CreateExecutingContext("t2", typeof(OverlappingTask));
        await _filter.OnExecutingAsync(second, CancellationToken.None);
        Assert.True(second.SkipExecution);
        Assert.True(second.RequeueMessage);
        Assert.NotNull(second.RequeueDelay);

        // The first execution finishes and stops tracking
        await _filter.OnExecutedAsync(
            CreateExecutedContext("t1", typeof(OverlappingTask), first.Properties),
            CancellationToken.None
        );

        var third = CreateExecutingContext("t3", typeof(OverlappingTask));
        await _filter.OnExecutingAsync(third, CancellationToken.None);
        Assert.False(third.SkipExecution);
    }

    [Fact]
    public async Task ExecutionLease_IsRenewedUntilTheFilterScopeIsDisposed()
    {
        var time = new FakeTimeProvider();
        var tracker = Substitute.For<ITaskExecutionTracker>();
#pragma warning disable CA2012 // NSubstitute consumes the configured ValueTask.
        tracker
            .TryStartAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<string?>(),
                Arg.Any<TimeSpan?>(),
                Arg.Any<CancellationToken>()
            )
            .Returns(true);
        var renewed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        tracker
            .ExtendAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<string?>(),
                Arg.Any<TimeSpan?>(),
                Arg.Any<CancellationToken>()
            )
            .Returns(_ =>
            {
                renewed.TrySetResult();
                return ValueTask.FromResult(true);
            });
#pragma warning restore CA2012
        var filter = new PreventOverlappingFilter(
            tracker,
            NullLogger<PreventOverlappingFilter>.Instance,
            time
        );
        await filter.OnExecutingAsync(
            CreateExecutingContext("renewed", typeof(OverlappingTask)),
            CancellationToken.None
        );
        time.Advance(TimeSpan.FromMinutes(20));
        await renewed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await filter.DisposeAsync();
        tracker.ClearReceivedCalls();

        time.Advance(TimeSpan.FromHours(2));

        await tracker
            .DidNotReceive()
            .ExtendAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<string?>(),
                Arg.Any<TimeSpan?>(),
                Arg.Any<CancellationToken>()
            );
    }

    [Fact]
    public async Task LostExecutionLease_DoesNotReportSuccessfulCompletion()
    {
        var time = new FakeTimeProvider();
        var tracker = Substitute.For<ITaskExecutionTracker>();
        var lost = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
#pragma warning disable CA2012 // NSubstitute consumes the configured ValueTask.
        tracker
            .TryStartAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<string?>(),
                Arg.Any<TimeSpan?>(),
                Arg.Any<CancellationToken>()
            )
            .Returns(true);
        tracker
            .ExtendAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<string?>(),
                Arg.Any<TimeSpan?>(),
                Arg.Any<CancellationToken>()
            )
            .Returns(_ =>
            {
                lost.TrySetResult();
                return ValueTask.FromResult(false);
            });
#pragma warning restore CA2012
        await using var filter = new PreventOverlappingFilter(
            tracker,
            NullLogger<PreventOverlappingFilter>.Instance,
            time
        );
        var executing = CreateExecutingContext("lost", typeof(OverlappingTask));
        await filter.OnExecutingAsync(executing, CancellationToken.None);
        time.Advance(TimeSpan.FromMinutes(20));
        await lost.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var executed = CreateExecutedContext("lost", typeof(OverlappingTask), executing.Properties);

        await filter.OnExecutedAsync(executed, CancellationToken.None);

        Assert.IsType<InvalidOperationException>(executed.Exception);
        Assert.False(executed.Succeeded);
    }

    [Fact]
    public async Task ATaskWithoutTheAttribute_IsNotTracked()
    {
        var context = CreateExecutingContext("t1", typeof(PlainTask));
        await _filter.OnExecutingAsync(context, CancellationToken.None);

        Assert.False(context.SkipExecution);
        Assert.Empty(context.Properties);
    }

    public async ValueTask DisposeAsync()
    {
        await _filter.DisposeAsync();
        await _services.DisposeAsync();
        await _backend.DisposeAsync();
    }

    private TaskExecutingContext CreateExecutingContext(string taskId, Type taskType) =>
        new()
        {
            TaskId = taskId,
            TaskName = "tests.overlapping",
            Message = CreateMessage(taskId),
            TaskType = taskType,
            TaskContext = CreateTaskContext(taskId),
            ServiceProvider = _services,
        };

    private TaskExecutedContext CreateExecutedContext(
        string taskId,
        Type taskType,
        IDictionary<string, object?> properties
    ) =>
        new()
        {
            TaskId = taskId,
            TaskName = "tests.overlapping",
            Message = CreateMessage(taskId),
            TaskType = taskType,
            TaskContext = CreateTaskContext(taskId),
            ServiceProvider = _services,
            Duration = TimeSpan.Zero,
            Properties = properties,
        };

    private TaskExecutionContext CreateTaskContext(string taskId) =>
        new(CreateMessage(taskId), _services, _backend);

    private static TaskMessage CreateMessage(string taskId) =>
        new()
        {
            Id = taskId,
            Task = "tests.overlapping",
            Args = [],
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Queue = "celery",
        };

    [PreventOverlapping]
    private sealed class OverlappingTask;

    private sealed class PlainTask;
}
