namespace DotCelery.Tests.Unit.Client;

using DotCelery.Backend.InMemory;
using DotCelery.Broker.InMemory;
using DotCelery.Client;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

public class AsyncResultTests : IAsyncDisposable
{
    private readonly InMemoryBroker _broker = new();
    private readonly InMemoryResultBackend _backend = new();
    private readonly JsonMessageSerializer _serializer = new();
    private readonly CeleryClient _client;

    public AsyncResultTests()
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
    public void Constructor_NullTaskId_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new AsyncResult(null!, _client));
    }

    [Fact]
    public void Constructor_NullClient_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new AsyncResult("task-id", null!));
    }

    [Fact]
    public void Constructor_ValidParameters_CreatesInstance()
    {
        var result = new AsyncResult("task-id", _client);

        Assert.Equal("task-id", result.TaskId);
    }

    [Fact]
    public void GenericConstructor_NullSerializer_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new AsyncResult<TestOutput>("task-id", _client, null!)
        );
    }

    [Fact]
    public void GenericConstructor_ValidParameters_CreatesInstance()
    {
        var result = new AsyncResult<TestOutput>("task-id", _client, _serializer);

        Assert.Equal("task-id", result.TaskId);
    }

    [Fact]
    public async Task GetStateAsync_NonExistentTask_ReturnsNull()
    {
        var result = new AsyncResult("non-existent-task", _client);

        var state = await result.GetStateAsync();

        Assert.Null(state);
    }

    [Fact]
    public async Task GetStateAsync_ExistingTask_ReturnsState()
    {
        var taskId = "test-task";
        await _backend.StoreResultAsync(
            new TaskResult
            {
                TaskId = taskId,
                State = TaskState.Success,
                CompletedAt = DateTimeOffset.UtcNow,
                Duration = TimeSpan.Zero,
            }
        );

        var result = new AsyncResult(taskId, _client);
        var state = await result.GetStateAsync();

        Assert.Equal(TaskState.Success, state);
    }

    [Theory]
    [InlineData(TaskState.Success, true)]
    [InlineData(TaskState.Failure, true)]
    [InlineData(TaskState.Revoked, true)]
    [InlineData(TaskState.Rejected, true)]
    [InlineData(TaskState.Pending, false)]
    [InlineData(TaskState.Started, false)]
    [InlineData(TaskState.Retry, false)]
    public async Task IsCompleteAsync_ReturnsExpectedValue(TaskState state, bool expectedComplete)
    {
        var taskId = "test-task";
        await _backend.StoreResultAsync(
            new TaskResult
            {
                TaskId = taskId,
                State = state,
                CompletedAt = DateTimeOffset.UtcNow,
                Duration = TimeSpan.Zero,
            }
        );

        var result = new AsyncResult(taskId, _client);
        var isComplete = await result.IsCompleteAsync();

        Assert.Equal(expectedComplete, isComplete);
    }

    [Fact]
    public async Task WaitAsync_ExistingResult_ReturnsResult()
    {
        var taskId = "test-task";
        await _backend.StoreResultAsync(
            new TaskResult
            {
                TaskId = taskId,
                State = TaskState.Success,
                CompletedAt = DateTimeOffset.UtcNow,
                Duration = TimeSpan.Zero,
            }
        );

        var asyncResult = new AsyncResult(taskId, _client);
        var result = await asyncResult.WaitAsync(TimeSpan.FromSeconds(1));

        Assert.Equal(taskId, result.TaskId);
        Assert.Equal(TaskState.Success, result.State);
    }

    public async ValueTask DisposeAsync()
    {
        await _broker.DisposeAsync();
        await _backend.DisposeAsync();
    }

    private sealed class TestOutput
    {
        public string? Value { get; set; }
    }
}
