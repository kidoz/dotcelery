using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Attributes;
using DotCelery.Core.Filters;
using DotCelery.Core.Models;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Filters;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

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
    public async Task ATaskWithoutTheAttribute_IsNotTracked()
    {
        var context = CreateExecutingContext("t1", typeof(PlainTask));
        await _filter.OnExecutingAsync(context, CancellationToken.None);

        Assert.False(context.SkipExecution);
        Assert.Empty(context.Properties);
    }

    public async ValueTask DisposeAsync()
    {
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
