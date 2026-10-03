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

    [Fact]
    public async Task OnExecutedAsync_AfterSuccessfulExecution_MarksTheMessageProcessed()
    {
        await _filter.OnExecutedAsync(Executed("a"), CancellationToken.None);

        // Another store instance stands for the next worker process
        Assert.True(await new InboxStore(_storage).IsProcessedAsync("a"));
    }

    [Fact]
    public async Task OnExecutedAsync_AfterAFailedExecution_LeavesTheMessageUnprocessed()
    {
        var context = Executed("a");
        context.Exception = new InvalidOperationException("boom");

        await _filter.OnExecutedAsync(context, CancellationToken.None);

        Assert.False(await _inbox.IsProcessedAsync("a"));
    }

    [Fact]
    public async Task OnExecutedAsync_AfterAFilterSetAFailedResult_LeavesTheMessageUnprocessed()
    {
        var context = Executed("a");
        context.TaskResult = new TaskResult
        {
            TaskId = "a",
            State = TaskState.Failure,
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromMilliseconds(1),
        };

        await _filter.OnExecutedAsync(context, CancellationToken.None);

        Assert.False(await _inbox.IsProcessedAsync("a"));
    }

    [Fact]
    public async Task OnExecutedAsync_WhenTheStoreFails_DoesNotThrow()
    {
        var filter = new InboxDeduplicationFilter(
            NullLogger<InboxDeduplicationFilter>.Instance,
            new FailingInboxStore()
        );

        await filter.OnExecutedAsync(Executed("a"), CancellationToken.None);
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

    private sealed class FailingInboxStore : IInboxStore
    {
        public ValueTask<bool> IsProcessedAsync(
            string messageId,
            CancellationToken cancellationToken = default
        ) => ValueTask.FromResult(false);

        public ValueTask MarkProcessedAsync(
            string messageId,
            object? transaction = null,
            CancellationToken cancellationToken = default
        ) => throw new InvalidOperationException("Storage unavailable");

        public ValueTask<long> GetCountAsync(CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(0L);

        public ValueTask<long> CleanupAsync(
            TimeSpan olderThan,
            CancellationToken cancellationToken = default
        ) => ValueTask.FromResult(0L);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

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
