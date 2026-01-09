namespace DotCelery.Tests.Unit.Worker;

using DotCelery.Backend.InMemory;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using DotCelery.Worker;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Filters;
using DotCelery.Worker.Registry;
using DotCelery.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

public class TaskExecutorTests : IAsyncDisposable
{
    private readonly InMemoryResultBackend _backend = new();
    private readonly JsonMessageSerializer _serializer = new();
    private readonly ServiceProvider _serviceProvider;
    private readonly TaskRegistry _registry = new();
    private readonly RevocationManager _revocationManager;
    private readonly TaskFilterPipeline _filterPipeline;
    private readonly TaskExecutor _executor;

    public TaskExecutorTests()
    {
        var services = new ServiceCollection();
        services.AddTransient<TestTaskWithInput>();
        services.AddTransient<TestTaskNoInput>();
        services.AddTransient<TestTaskReturnsNull>();

        _serviceProvider = services.BuildServiceProvider();

        var workerOptions = Options.Create(
            new WorkerOptions { EnableRevocation = false, EnableRateLimiting = false }
        );

        _revocationManager = new RevocationManager(
            workerOptions,
            NullLogger<RevocationManager>.Instance
        );

        _filterPipeline = new TaskFilterPipeline(
            _serviceProvider,
            Options.Create(new TaskFilterOptions()),
            NullLogger<TaskFilterPipeline>.Instance
        );

        _executor = new TaskExecutor(
            _registry,
            _serviceProvider,
            _serializer,
            _backend,
            _revocationManager,
            _filterPipeline,
            workerOptions,
            NullLogger<TaskExecutor>.Instance
        );

        // Register tasks using correct API
        _registry.Register(typeof(TestTaskWithInput), TestTaskWithInput.TaskName);
        _registry.Register(typeof(TestTaskNoInput), TestTaskNoInput.TaskName);
        _registry.Register(typeof(TestTaskReturnsNull), TestTaskReturnsNull.TaskName);
    }

    [Fact]
    public async Task ExecuteAsync_ValidTask_ReturnsSuccess()
    {
        var message = CreateBrokerMessage(
            "task-1",
            TestTaskWithInput.TaskName,
            new TestInput { Value = 42 }
        );

        var result = await _executor.ExecuteAsync(message, "worker-1", CancellationToken.None);

        Assert.Equal(TaskState.Success, result.State);
        Assert.Equal("task-1", result.TaskId);
        Assert.NotNull(result.Result);
    }

    [Fact]
    public async Task ExecuteAsync_TaskWithNoInput_ExecutesSuccessfully()
    {
        var message = CreateBrokerMessage("task-2", TestTaskNoInput.TaskName, input: null);

        var result = await _executor.ExecuteAsync(message, "worker-1", CancellationToken.None);

        Assert.Equal(TaskState.Success, result.State);
    }

    [Fact]
    public async Task ExecuteAsync_TaskReturnsNull_StoresNullResult()
    {
        var message = CreateBrokerMessage(
            "task-3",
            TestTaskReturnsNull.TaskName,
            new TestInput { Value = 0 }
        );

        var result = await _executor.ExecuteAsync(message, "worker-1", CancellationToken.None);

        Assert.Equal(TaskState.Success, result.State);
        Assert.Null(result.Result);
    }

    [Fact]
    public async Task ExecuteAsync_UnknownTask_ThrowsUnknownTaskException()
    {
        var message = CreateBrokerMessage("task-4", "unknown.task", new TestInput { Value = 1 });

        await Assert.ThrowsAsync<DotCelery.Core.Exceptions.UnknownTaskException>(() =>
            _executor.ExecuteAsync(message, "worker-1", CancellationToken.None)
        );
    }

    [Fact]
    public async Task ExecuteAsync_TaskThrowsException_ReturnsFailure()
    {
        var services = new ServiceCollection();
        services.AddTransient<ThrowingTask>();
        using var sp = services.BuildServiceProvider();

        var registry = new TaskRegistry();
        registry.Register(typeof(ThrowingTask), ThrowingTask.TaskName);

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
            _revocationManager,
            filterPipeline,
            Options.Create(new WorkerOptions()),
            NullLogger<TaskExecutor>.Instance
        );

        var message = CreateBrokerMessage(
            "task-5",
            ThrowingTask.TaskName,
            new TestInput { Value = 1 }
        );

        var result = await executor.ExecuteAsync(message, "worker-1", CancellationToken.None);

        Assert.Equal(TaskState.Failure, result.State);
        Assert.NotNull(result.Exception);
        // With compiled delegates, exceptions propagate directly without TargetInvocationException wrapping
        Assert.Equal("System.InvalidOperationException", result.Exception.Type);
        Assert.Equal("Test exception", result.Exception.Message);
    }

    [Fact]
    public async Task ExecuteAsync_StoresResultInBackend()
    {
        var taskId = "stored-task";
        var message = CreateBrokerMessage(
            taskId,
            TestTaskWithInput.TaskName,
            new TestInput { Value = 10 }
        );

        await _executor.ExecuteAsync(message, "worker-1", CancellationToken.None);

        var storedResult = await _backend.GetResultAsync(taskId);
        Assert.NotNull(storedResult);
        Assert.Equal(TaskState.Success, storedResult.State);
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
        await _serviceProvider.DisposeAsync();
        _revocationManager.Dispose();
    }

    private sealed class TestTaskWithInput : ITask<TestInput, TestOutput>
    {
        public static string TaskName => "test.task.input";

        public Task<TestOutput> ExecuteAsync(
            TestInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            return Task.FromResult(new TestOutput { Result = input.Value * 2 });
        }
    }

    private sealed class TestTaskNoInput : ITask
    {
        public static string TaskName => "test.task.noinput";

#pragma warning disable CA1822 // Mark members as static - interface implementation
        public Task<TestOutput> ExecuteAsync(
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            return Task.FromResult(new TestOutput { Result = 100 });
        }
#pragma warning restore CA1822
    }

    private sealed class TestTaskReturnsNull : ITask<TestInput>
    {
        public static string TaskName => "test.task.returnsnull";

        public Task ExecuteAsync(
            TestInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            return Task.CompletedTask;
        }
    }

    private sealed class ThrowingTask : ITask<TestInput>
    {
        public static string TaskName => "throwing.task";

        public Task ExecuteAsync(
            TestInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            throw new InvalidOperationException("Test exception");
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
