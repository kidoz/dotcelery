using DotCelery.Backend.InMemory;
using DotCelery.Broker.InMemory;
using DotCelery.Client;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Unit.Client;

public class CeleryClientTests : IAsyncDisposable
{
    private readonly InMemoryBroker _broker = new();
    private readonly InMemoryResultBackend _backend = new();
    private readonly JsonMessageSerializer _serializer = new();
    private readonly CeleryClient _client;

    public CeleryClientTests()
    {
        var options = Options.Create(new CeleryClientOptions());
        _client = new CeleryClient(
            _broker,
            _backend,
            _serializer,
            options,
            NullLogger<CeleryClient>.Instance
        );
    }

    [Fact]
    public async Task SendAsync_ValidTask_PublishesMessage()
    {
        var input = new TestInput { Value = 42 };

        var result = await _client.SendAsync<TestTask, TestInput, TestOutput>(input);

        Assert.NotNull(result);
        Assert.NotEmpty(result.TaskId);
        Assert.Equal(1, _broker.GetQueueLength("celery"));
    }

    [Fact]
    public async Task SendAsync_WithOptions_AppliesOptions()
    {
        var input = new TestInput { Value = 42 };
        var eta = DateTimeOffset.UtcNow.AddMinutes(10);

        var options = new SendOptions
        {
            Queue = "custom-queue",
            Priority = 5,
            Eta = eta,
        };

        var result = await _client.SendAsync<TestTask, TestInput, TestOutput>(input, options);

        Assert.NotNull(result);
        Assert.Equal(1, _broker.GetQueueLength("custom-queue"));
    }

    [Fact]
    public async Task SendAsync_WithCountdown_CalculatesEta()
    {
        var input = new TestInput { Value = 42 };
        var options = new SendOptions { Countdown = TimeSpan.FromMinutes(5) };

        var result = await _client.SendAsync<TestTask, TestInput, TestOutput>(input, options);

        Assert.NotNull(result);
    }

    [Fact]
    public async Task SendAsync_WithCustomTaskId_UsesProvidedId()
    {
        var input = new TestInput { Value = 42 };
        var customId = "my-custom-task-id";
        var options = new SendOptions { TaskId = customId };

        var result = await _client.SendAsync<TestTask, TestInput, TestOutput>(input, options);

        Assert.Equal(customId, result.TaskId);
    }

    [Fact]
    public async Task SendAsync_NoReturnValue_PublishesMessage()
    {
        var input = new TestInput { Value = 42 };

        var result = await _client.SendAsync<TaskWithInputOnly, TestInput>(input);

        Assert.NotNull(result);
        Assert.Equal(1, _broker.GetQueueLength("celery"));
    }

    [Fact]
    public async Task GetResultAsync_NonExistentResult_ReturnsNull()
    {
        var result = await _client.GetResultAsync("non-existent");

        Assert.Null(result);
    }

    [Fact]
    public async Task GetResultAsync_ExistingResult_ReturnsResult()
    {
        var taskId = "test-task-id";
        var taskResult = new TaskResult
        {
            TaskId = taskId,
            State = TaskState.Success,
            Result = "{}"u8.ToArray(),
            ContentType = "application/json",
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromMilliseconds(100),
        };
        await _backend.StoreResultAsync(taskResult);

        var result = await _client.GetResultAsync(taskId);

        Assert.NotNull(result);
        Assert.Equal(taskId, result.TaskId);
        Assert.Equal(TaskState.Success, result.State);
    }

    [Fact]
    public async Task WaitForResultAsync_ExistingResult_ReturnsImmediately()
    {
        var taskId = "test-task-id";
        var taskResult = new TaskResult
        {
            TaskId = taskId,
            State = TaskState.Success,
            Result = "{}"u8.ToArray(),
            ContentType = "application/json",
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromMilliseconds(100),
        };
        await _backend.StoreResultAsync(taskResult);

        var result = await _client.WaitForResultAsync(taskId, TimeSpan.FromSeconds(1));

        Assert.NotNull(result);
        Assert.Equal(taskId, result.TaskId);
    }

    public async ValueTask DisposeAsync()
    {
        await _broker.DisposeAsync();
        await _backend.DisposeAsync();
    }

    private sealed class TestTask : ITask<TestInput, TestOutput>
    {
        public static string TaskName => "test.task";

        public Task<TestOutput> ExecuteAsync(
            TestInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            return Task.FromResult(new TestOutput { Result = input.Value * 2 });
        }
    }

    private sealed class TaskWithInputOnly : ITask<TestInput>
    {
        public static string TaskName => "task.input.only";

        public Task ExecuteAsync(
            TestInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            return Task.CompletedTask;
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
