using DotCelery.Backend.Mongo;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.MongoDb;

namespace DotCelery.Tests.Integration.Mongo;

/// <summary>
/// Integration tests for MongoDB backend using Testcontainers.
/// These tests require Docker to be running.
/// </summary>
[Collection("Mongo")]
public class MongoBackendIntegrationTests : IAsyncLifetime
{
    private readonly MongoDbContainer _container;
    private MongoResultBackend? _backend;

    public MongoBackendIntegrationTests()
    {
        _container = new MongoDbBuilder("mongo:7").Build();
    }

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();

        var options = Options.Create(
            new MongoBackendOptions
            {
                ConnectionString = _container.GetConnectionString(),
                DatabaseName = "celery_test",
                UseChangeStreams = false,
                AutoCreateIndexes = true,
            }
        );

        var logger = LoggerFactory
            .Create(builder => builder.AddConsole())
            .CreateLogger<MongoResultBackend>();

        _backend = new MongoResultBackend(options, logger);
    }

    public async ValueTask DisposeAsync()
    {
        if (_backend is not null)
        {
            await _backend.DisposeAsync();
        }

        await _container.DisposeAsync();
    }

    [Fact]
    public async Task StoreAndGetResult_Works()
    {
        var result = CreateSuccessResult();

        await _backend!.StoreResultAsync(result);
        var retrieved = await _backend.GetResultAsync(result.TaskId);

        Assert.NotNull(retrieved);
        Assert.Equal(result.TaskId, retrieved.TaskId);
        Assert.Equal(result.State, retrieved.State);
    }

    [Fact]
    public async Task GetResultAsync_NonExistent_ReturnsNull()
    {
        var retrieved = await _backend!.GetResultAsync("non-existent-task-id");

        Assert.Null(retrieved);
    }

    [Fact]
    public async Task UpdateStateAsync_Works()
    {
        var taskId = Guid.NewGuid().ToString();

        await _backend!.UpdateStateAsync(taskId, TaskState.Started);
        var state = await _backend.GetStateAsync(taskId);

        Assert.Equal(TaskState.Started, state);
    }

    [Fact]
    public async Task GetStateAsync_NonExistent_ReturnsNull()
    {
        var state = await _backend!.GetStateAsync("non-existent-task-id");

        Assert.Null(state);
    }

    [Fact]
    public async Task WaitForResultAsync_ExistingResult_ReturnsImmediately()
    {
        var result = CreateSuccessResult();
        await _backend!.StoreResultAsync(result);

        var retrieved = await _backend.WaitForResultAsync(
            result.TaskId,
            timeout: TimeSpan.FromSeconds(1)
        );

        Assert.Equal(result.TaskId, retrieved.TaskId);
    }

    [Fact]
    public async Task WaitForResultAsync_ResultStoredLater_WaitsAndReturns()
    {
        var result = CreateSuccessResult();

        // Store result after a delay
        _ = Task.Run(async () =>
        {
            await Task.Delay(200);
            await _backend!.StoreResultAsync(result);
        });

        var retrieved = await _backend!.WaitForResultAsync(
            result.TaskId,
            timeout: TimeSpan.FromSeconds(5)
        );

        Assert.Equal(result.TaskId, retrieved.TaskId);
    }

    [Fact]
    public async Task WaitForResultAsync_Timeout_ThrowsTimeoutException()
    {
        await Assert.ThrowsAsync<TimeoutException>(() =>
            _backend!.WaitForResultAsync("non-existent", timeout: TimeSpan.FromMilliseconds(100))
        );
    }

    [Fact]
    public async Task StoreResultAsync_WithExpiry_ResultExpires()
    {
        var result = CreateSuccessResult();

        await _backend!.StoreResultAsync(result, expiry: TimeSpan.FromMilliseconds(100));

        // Should exist initially
        var retrieved = await _backend.GetResultAsync(result.TaskId);
        Assert.NotNull(retrieved);

        // Wait for expiry check (MongoDB TTL runs approximately every 60 seconds,
        // but our filter checks expires_at so it should be filtered immediately)
        await Task.Delay(200);

        // Should be filtered by query (expires_at check)
        retrieved = await _backend.GetResultAsync(result.TaskId);
        Assert.Null(retrieved);
    }

    [Fact]
    public async Task StoreResultAsync_OverwritesExisting()
    {
        var taskId = Guid.NewGuid().ToString();
        var result1 = CreateSuccessResult() with { TaskId = taskId };
        var result2 = CreateFailureResult() with { TaskId = taskId };

        await _backend!.StoreResultAsync(result1);
        await _backend.StoreResultAsync(result2);

        var retrieved = await _backend.GetResultAsync(taskId);

        Assert.NotNull(retrieved);
        Assert.Equal(TaskState.Failure, retrieved.State);
    }

    [Fact]
    public async Task ConcurrentStoreAndRetrieve_Works()
    {
        var tasks = Enumerable
            .Range(0, 10)
            .Select(async i =>
            {
                var result = CreateSuccessResult();
                await _backend!.StoreResultAsync(result);
                var retrieved = await _backend.GetResultAsync(result.TaskId);
                return retrieved?.TaskId == result.TaskId;
            });

        var results = await Task.WhenAll(tasks);

        Assert.All(results, Assert.True);
    }

    [Fact]
    public async Task StoreResultAsync_WithException_PreservesExceptionInfo()
    {
        var result = CreateFailureResult();

        await _backend!.StoreResultAsync(result);
        var retrieved = await _backend.GetResultAsync(result.TaskId);

        Assert.NotNull(retrieved);
        Assert.NotNull(retrieved.Exception);
        Assert.Equal("System.Exception", retrieved.Exception.Type);
        Assert.Equal("Test exception", retrieved.Exception.Message);
    }

    [Fact]
    public async Task StoreResultAsync_WithMetadata_PreservesMetadata()
    {
        var result = CreateSuccessResult() with
        {
            Metadata = new Dictionary<string, object> { ["key1"] = "value1", ["key2"] = 42 },
        };

        await _backend!.StoreResultAsync(result);
        var retrieved = await _backend.GetResultAsync(result.TaskId);

        Assert.NotNull(retrieved);
        Assert.NotNull(retrieved.Metadata);
        Assert.Equal("value1", retrieved.Metadata["key1"]?.ToString());
    }

    [Fact]
    public async Task StoreResultAsync_WithBinaryResult_PreservesData()
    {
        var binaryData = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 };
        var result = CreateSuccessResult() with
        {
            Result = binaryData,
            ContentType = "application/octet-stream",
        };

        await _backend!.StoreResultAsync(result);
        var retrieved = await _backend.GetResultAsync(result.TaskId);

        Assert.NotNull(retrieved);
        Assert.NotNull(retrieved.Result);
        Assert.Equal(binaryData, retrieved.Result);
        Assert.Equal("application/octet-stream", retrieved.ContentType);
    }

    private static TaskResult CreateSuccessResult() =>
        new()
        {
            TaskId = Guid.NewGuid().ToString(),
            State = TaskState.Success,
            Result = "{\"value\": 42}"u8.ToArray(),
            ContentType = "application/json",
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromMilliseconds(100),
        };

    private static TaskResult CreateFailureResult() =>
        new()
        {
            TaskId = Guid.NewGuid().ToString(),
            State = TaskState.Failure,
            Exception = new TaskExceptionInfo
            {
                Type = "System.Exception",
                Message = "Test exception",
            },
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromMilliseconds(100),
        };
}
