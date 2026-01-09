namespace DotCelery.Tests.Unit.Worker;

using DotCelery.Backend.InMemory;
using DotCelery.Backend.InMemory.Partitioning;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Attributes;
using DotCelery.Core.Filters;
using DotCelery.Core.Models;
using DotCelery.Core.RateLimiting;
using DotCelery.Core.Serialization;
using DotCelery.Worker;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Filters;
using DotCelery.Worker.Registry;
using DotCelery.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

/// <summary>
/// Tests for requeue and rate-limiting behavior in the worker.
/// These tests verify:
/// - Requeue state persistence when filters request requeue
/// - RequeueDelay is properly propagated from filters
/// - Rate-limited tasks don't increment retry count
/// </summary>
public class RequeueAndRateLimitTests : IAsyncDisposable
{
    private readonly InMemoryResultBackend _backend = new();
    private readonly JsonMessageSerializer _serializer = new();

    [Fact]
    public async Task RequeueDelay_IsSetByPartitionedExecutionFilter_WhenPartitionIsLocked()
    {
        // Arrange
        var lockStore = new InMemoryPartitionLockStore();
        var options = new PartitionOptions { RequeueDelay = TimeSpan.FromSeconds(5) };

        var filter = new PartitionedExecutionFilter(
            lockStore,
            Options.Create(options),
            NullLogger<PartitionedExecutionFilter>.Instance
        );

        // First, lock the partition with another task
        await lockStore.TryAcquireAsync(
            "partition-1",
            "other-task",
            TimeSpan.FromMinutes(1),
            CancellationToken.None
        );

        var taskContext = Substitute.For<ITaskContext>();
        taskContext.PartitionKey.Returns("partition-1");

        var context = new TaskExecutingContext
        {
            TaskId = "my-task",
            TaskName = "test.task",
            TaskType = typeof(object),
            TaskContext = taskContext,
            ServiceProvider = new ServiceCollection().BuildServiceProvider(),
            Message = new TaskMessage
            {
                Id = "my-task",
                Task = "test.task",
                Args = [],
                ContentType = "application/json",
                Timestamp = DateTimeOffset.UtcNow,
                Queue = "celery",
            },
        };

        // Act
        await filter.OnExecutingAsync(context, CancellationToken.None);

        // Assert
        Assert.True(context.SkipExecution);
        Assert.True(context.RequeueMessage);
        Assert.Equal(TimeSpan.FromSeconds(5), context.RequeueDelay);
    }

    [Fact]
    public async Task RequeueDelay_IsNotSet_WhenPartitionIsAvailable()
    {
        // Arrange
        var lockStore = new InMemoryPartitionLockStore();
        var options = new PartitionOptions { RequeueDelay = TimeSpan.FromSeconds(5) };

        var filter = new PartitionedExecutionFilter(
            lockStore,
            Options.Create(options),
            NullLogger<PartitionedExecutionFilter>.Instance
        );

        var taskContext = Substitute.For<ITaskContext>();
        taskContext.PartitionKey.Returns("partition-1");

        var context = new TaskExecutingContext
        {
            TaskId = "my-task",
            TaskName = "test.task",
            TaskType = typeof(object),
            TaskContext = taskContext,
            ServiceProvider = new ServiceCollection().BuildServiceProvider(),
            Message = new TaskMessage
            {
                Id = "my-task",
                Task = "test.task",
                Args = [],
                ContentType = "application/json",
                Timestamp = DateTimeOffset.UtcNow,
                Queue = "celery",
            },
        };

        // Act
        await filter.OnExecutingAsync(context, CancellationToken.None);

        // Assert
        Assert.False(context.SkipExecution);
        Assert.False(context.RequeueMessage);
        Assert.Null(context.RequeueDelay);
    }

    [Fact]
    public async Task PartitionLock_IsReleased_OnExecuted()
    {
        // Arrange
        var lockStore = new InMemoryPartitionLockStore();
        var options = new PartitionOptions { RequeueDelay = TimeSpan.FromSeconds(5) };

        var filter = new PartitionedExecutionFilter(
            lockStore,
            Options.Create(options),
            NullLogger<PartitionedExecutionFilter>.Instance
        );

        var taskContext = Substitute.For<ITaskContext>();
        taskContext.PartitionKey.Returns("partition-1");

        var testMessage = new TaskMessage
        {
            Id = "my-task",
            Task = "test.task",
            Args = [],
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Queue = "celery",
        };

        var executingContext = new TaskExecutingContext
        {
            TaskId = "my-task",
            TaskName = "test.task",
            TaskType = typeof(object),
            TaskContext = taskContext,
            ServiceProvider = new ServiceCollection().BuildServiceProvider(),
            Message = testMessage,
        };

        // Acquire the lock
        await filter.OnExecutingAsync(executingContext, CancellationToken.None);
        Assert.True(executingContext.Properties.ContainsKey("PartitionLockAcquired"));

        var executedContext = new TaskExecutedContext
        {
            TaskId = "my-task",
            TaskName = "test.task",
            TaskType = typeof(object),
            TaskContext = taskContext,
            ServiceProvider = new ServiceCollection().BuildServiceProvider(),
            Duration = TimeSpan.FromMilliseconds(100),
            Properties = executingContext.Properties,
            Message = testMessage,
        };

        // Act
        await filter.OnExecutedAsync(executedContext, CancellationToken.None);

        // Assert - another task should be able to acquire the lock now
        var acquired = await lockStore.TryAcquireAsync(
            "partition-1",
            "other-task",
            TimeSpan.FromMinutes(1),
            CancellationToken.None
        );
        Assert.True(acquired);
    }

    [Fact]
    public async Task RateLimited_Task_SetsDoNotIncrementRetries()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddTransient<RateLimitedTestTask>();
        using var sp = services.BuildServiceProvider();

        var registry = new TaskRegistry();
        registry.Register(typeof(RateLimitedTestTask), RateLimitedTestTask.TaskName);

        var rateLimiter = new InMemoryRateLimiter();
        var workerOptions = Options.Create(
            new WorkerOptions { EnableRevocation = false, EnableRateLimiting = true }
        );

        var revocationManager = new RevocationManager(
            workerOptions,
            NullLogger<RevocationManager>.Instance
        );

        var filterPipeline = new TaskFilterPipeline(
            sp,
            Options.Create(new TaskFilterOptions()),
            NullLogger<TaskFilterPipeline>.Instance
        );

        var executor = new TaskExecutor(
            registry,
            sp,
            _serializer,
            _backend,
            revocationManager,
            filterPipeline,
            workerOptions,
            NullLogger<TaskExecutor>.Instance,
            rateLimiter: rateLimiter
        );

        // Exhaust the rate limit
        var policy = new RateLimitPolicy { Limit = 1, Window = TimeSpan.FromMinutes(5) };
        await rateLimiter.TryAcquireAsync(
            RateLimitedTestTask.TaskName,
            policy,
            CancellationToken.None
        );

        var message = CreateBrokerMessage(
            "task-1",
            RateLimitedTestTask.TaskName,
            new TestInput { Value = 42 }
        );

        // Act
        var result = await executor.ExecuteAsync(message, "worker-1", CancellationToken.None);

        // Assert
        Assert.Equal(TaskState.Retry, result.State);
        Assert.True(
            result.DoNotIncrementRetries,
            "Rate-limited tasks should not increment retry count"
        );
        Assert.NotNull(result.RetryAfter);
    }

    [Fact]
    public void TaskResult_DoNotIncrementRetries_DefaultsToFalse()
    {
        // Arrange & Act
        var result = new TaskResult
        {
            TaskId = "test",
            State = TaskState.Retry,
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = TimeSpan.Zero,
        };

        // Assert
        Assert.False(result.DoNotIncrementRetries);
    }

    [Fact]
    public void TaskResult_RequeueDelay_CanBeSet()
    {
        // Arrange & Act
        var result = new TaskResult
        {
            TaskId = "test",
            State = TaskState.Requeued,
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = TimeSpan.Zero,
            RequeueDelay = TimeSpan.FromSeconds(3),
        };

        // Assert
        Assert.Equal(TimeSpan.FromSeconds(3), result.RequeueDelay);
    }

    [Fact]
    public void TaskExecutingContext_RequeueDelay_CanBeSet()
    {
        // Arrange
        var taskContext = Substitute.For<ITaskContext>();

        // Act
        var context = new TaskExecutingContext
        {
            TaskId = "test",
            TaskName = "test.task",
            TaskType = typeof(object),
            TaskContext = taskContext,
            ServiceProvider = new ServiceCollection().BuildServiceProvider(),
            RequeueDelay = TimeSpan.FromSeconds(2),
            Message = new TaskMessage
            {
                Id = "test",
                Task = "test.task",
                Args = [],
                ContentType = "application/json",
                Timestamp = DateTimeOffset.UtcNow,
                Queue = "celery",
            },
        };

        // Assert
        Assert.Equal(TimeSpan.FromSeconds(2), context.RequeueDelay);
    }

    [Fact]
    public void TaskExecutingContext_RequeueDelay_DefaultsToNull()
    {
        // Arrange
        var taskContext = Substitute.For<ITaskContext>();

        // Act
        var context = new TaskExecutingContext
        {
            TaskId = "test",
            TaskName = "test.task",
            TaskType = typeof(object),
            TaskContext = taskContext,
            ServiceProvider = new ServiceCollection().BuildServiceProvider(),
            Message = new TaskMessage
            {
                Id = "test",
                Task = "test.task",
                Args = [],
                ContentType = "application/json",
                Timestamp = DateTimeOffset.UtcNow,
                Queue = "celery",
            },
        };

        // Assert
        Assert.Null(context.RequeueDelay);
    }

    private BrokerMessage CreateBrokerMessage(string taskId, string taskName, object? input)
    {
        return new BrokerMessage
        {
            Message = new TaskMessage
            {
                Id = taskId,
                Task = taskName,
                Args = input is not null ? _serializer.Serialize(input) : [],
                ContentType = _serializer.ContentType,
                Timestamp = DateTimeOffset.UtcNow,
                Queue = "celery",
            },
            DeliveryTag = 1UL,
            Queue = "celery",
            ReceivedAt = DateTimeOffset.UtcNow,
        };
    }

    public async ValueTask DisposeAsync()
    {
        await _backend.DisposeAsync();
    }

    [RateLimit(1, 5 * 60)] // 1 request per 5 minutes
    private sealed class RateLimitedTestTask : ITask<TestInput, TestOutput>
    {
        public static string TaskName => "ratelimited.task";

        public Task<TestOutput> ExecuteAsync(
            TestInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            return Task.FromResult(new TestOutput { Result = input.Value * 2 });
        }
    }

    private sealed class TestInput
    {
        public int Value { get; set; }
    }

    private sealed class TestOutput
    {
        public int Result { get; set; }
    }
}
