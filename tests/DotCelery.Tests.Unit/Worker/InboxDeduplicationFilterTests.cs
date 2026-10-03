using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Filters;
using DotCelery.Core.Models;
using DotCelery.Core.Progress;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker.Filters;
using Microsoft.Extensions.Logging.Abstractions;

namespace DotCelery.Tests.Unit.Worker;

public sealed class InboxDeduplicationFilterTests
{
    private readonly InMemoryStorageProvider _storage = new();
    private readonly InboxStore _inbox;
    private readonly InboxDeduplicationFilter _filter;

    public InboxDeduplicationFilterTests()
    {
        _inbox = new InboxStore(_storage);
        _filter = new InboxDeduplicationFilter(
            NullLogger<InboxDeduplicationFilter>.Instance,
            _inbox
        );
    }

    [Fact]
    public async Task OnExecutingAsync_MessageNotProcessed_ContinuesExecution()
    {
        var context = Executing("a");

        await _filter.OnExecutingAsync(context, CancellationToken.None);

        Assert.False(context.SkipExecution);
        Assert.Null(context.SkipResult);
    }

    [Fact]
    public async Task OnExecutingAsync_MessageAlreadyProcessed_SkipsExecutionAsSuccess()
    {
        await _inbox.MarkProcessedAsync("a");

        var context = Executing("a");
        await _filter.OnExecutingAsync(context, CancellationToken.None);

        Assert.True(context.SkipExecution);
        Assert.NotNull(context.SkipResult);
        Assert.Equal(TaskState.Success, context.SkipResult.State);
        Assert.Equal(true, context.SkipResult.Metadata!["deduplicated"]);
    }

    // Marking is the executor's outcome recording, not the filter's
    [Fact]
    public async Task OnExecutedAsync_DoesNotMarkTheMessage()
    {
        await _filter.OnExecutedAsync(Executed("a"), CancellationToken.None);

        Assert.False(await _inbox.IsProcessedAsync("a"));
    }

    [Fact]
    public async Task WithoutAnInboxStore_DoesNothing()
    {
        var filter = new InboxDeduplicationFilter(
            NullLogger<InboxDeduplicationFilter>.Instance,
            inboxStore: null
        );

        var context = Executing("a");
        await filter.OnExecutingAsync(context, CancellationToken.None);
        await filter.OnExecutedAsync(Executed("a"), CancellationToken.None);

        Assert.False(context.SkipExecution);
    }

    private static TaskExecutingContext Executing(string taskId) =>
        new()
        {
            TaskId = taskId,
            TaskName = "tests.task",
            Message = CreateMessage(taskId),
            TaskType = typeof(InboxDeduplicationFilterTests),
            TaskContext = new SubstituteTaskContext(taskId),
            ServiceProvider = new EmptyServiceProvider(),
        };

    private static TaskExecutedContext Executed(string taskId) =>
        new()
        {
            TaskId = taskId,
            TaskName = "tests.task",
            Message = CreateMessage(taskId),
            TaskType = typeof(InboxDeduplicationFilterTests),
            TaskContext = new SubstituteTaskContext(taskId),
            ServiceProvider = new EmptyServiceProvider(),
            Duration = TimeSpan.FromMilliseconds(1),
        };

    private static TaskMessage CreateMessage(string taskId) =>
        new()
        {
            Id = taskId,
            Task = "tests.task",
            Args = [1, 2, 3],
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
        };

    private sealed class EmptyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }

    private sealed class SubstituteTaskContext(string taskId) : ITaskContext
    {
        public string TaskId { get; } = taskId;

        public string TaskName => "tests.task";

        public int RetryCount => 0;

        public int MaxRetries => 3;

        public string Queue => "tests";

        public DateTimeOffset SentAt => DateTimeOffset.UtcNow;

        public DateTimeOffset? Eta => null;

        public DateTimeOffset? Expires => null;

        public string? ParentId => null;

        public string? RootId => null;

        public string? CorrelationId => null;

        public string? TenantId => null;

        public string? PartitionKey => null;

        public IReadOnlyDictionary<string, string>? Headers => null;

        public IProgressReporter Progress => throw new NotSupportedException();

        public void Retry(TimeSpan? countdown = null, Exception? exception = null) =>
            throw new NotSupportedException();

        public Task UpdateStateAsync(TaskState state, object? metadata = null) =>
            Task.CompletedTask;

        public T GetRequiredService<T>()
            where T : notnull => throw new NotSupportedException();
    }
}
