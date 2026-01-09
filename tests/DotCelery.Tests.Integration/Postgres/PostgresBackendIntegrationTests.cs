using DotCelery.Backend.Postgres;
using DotCelery.Backend.Postgres.Signals;
using DotCelery.Core.Models;
using DotCelery.Core.Signals;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.PostgreSql;

namespace DotCelery.Tests.Integration.Postgres;

/// <summary>
/// Integration tests for PostgreSQL backend using Testcontainers.
/// These tests require Docker to be running.
/// </summary>
[Collection("Postgres")]
public class PostgresBackendIntegrationTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container;
    private PostgresResultBackend? _backend;

    public PostgresBackendIntegrationTests()
    {
        _container = new PostgreSqlBuilder("postgres:16-alpine")
            .WithDatabase("celery")
            .WithUsername("celery")
            .WithPassword("celery")
            .Build();
    }

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();

        var options = Options.Create(
            new PostgresBackendOptions
            {
                ConnectionString = _container.GetConnectionString(),
                UseListenNotify = true,
                AutoCreateTables = true,
            }
        );

        var logger = LoggerFactory
            .Create(builder => builder.AddConsole())
            .CreateLogger<PostgresResultBackend>();

        _backend = new PostgresResultBackend(options, logger);
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

        // Wait for expiry
        await Task.Delay(200);

        // Should be gone (filtered by query)
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

/// <summary>
/// Integration tests for PostgreSQL signal store using Testcontainers.
/// </summary>
[Collection("Postgres")]
public class PostgresSignalStoreIntegrationTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container;
    private string _connectionString = string.Empty;
    private ILoggerFactory _loggerFactory = null!;

    public PostgresSignalStoreIntegrationTests()
    {
        _container = new PostgreSqlBuilder("postgres:16-alpine")
            .WithDatabase("celery_signals")
            .WithUsername("celery")
            .WithPassword("celery")
            .Build();
    }

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        _connectionString = _container.GetConnectionString();
        _loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
    }

    public async ValueTask DisposeAsync()
    {
        await _container.DisposeAsync();
        _loggerFactory.Dispose();
    }

    [Fact]
    public async Task EnqueueAndDequeue_Works()
    {
        await using var store = CreateSignalStore();

        var signal = new SignalMessage
        {
            Id = Guid.NewGuid().ToString(),
            SignalType = "TaskSuccess",
            TaskId = Guid.NewGuid().ToString(),
            TaskName = "test.task",
            Payload = "{}",
            CreatedAt = DateTimeOffset.UtcNow,
        };

        await store.EnqueueAsync(signal);
        var pending = await store.GetPendingCountAsync();

        Assert.Equal(1, pending);
    }

    [Fact]
    public async Task Dequeue_ClaimsMessage()
    {
        await using var store = CreateSignalStore();

        var signal = new SignalMessage
        {
            Id = Guid.NewGuid().ToString(),
            SignalType = "TaskSuccess",
            TaskId = Guid.NewGuid().ToString(),
            TaskName = "test.task",
            Payload = "{}",
            CreatedAt = DateTimeOffset.UtcNow,
        };

        await store.EnqueueAsync(signal);

        var dequeued = new List<SignalMessage>();
        await foreach (var msg in store.DequeueAsync(10))
        {
            dequeued.Add(msg);
        }

        Assert.Single(dequeued);
        Assert.Equal(signal.Id, dequeued[0].Id);

        // After dequeue, pending should be 0 (message is now processing)
        var pending = await store.GetPendingCountAsync();
        Assert.Equal(0, pending);
    }

    [Fact]
    public async Task Acknowledge_RemovesMessage()
    {
        await using var store = CreateSignalStore();

        var signal = new SignalMessage
        {
            Id = Guid.NewGuid().ToString(),
            SignalType = "TaskSuccess",
            TaskId = Guid.NewGuid().ToString(),
            TaskName = "test.task",
            Payload = "{}",
            CreatedAt = DateTimeOffset.UtcNow,
        };

        await store.EnqueueAsync(signal);

        await foreach (var msg in store.DequeueAsync(1))
        {
            await store.AcknowledgeAsync(msg.Id);
        }

        // Try to dequeue again - should get nothing
        var dequeued = new List<SignalMessage>();
        await foreach (var msg in store.DequeueAsync(10))
        {
            dequeued.Add(msg);
        }

        Assert.Empty(dequeued);
    }

    [Fact]
    public async Task Reject_RequeuesToQueue()
    {
        await using var store = CreateSignalStore();

        var signal = new SignalMessage
        {
            Id = Guid.NewGuid().ToString(),
            SignalType = "TaskSuccess",
            TaskId = Guid.NewGuid().ToString(),
            TaskName = "test.task",
            Payload = "{}",
            CreatedAt = DateTimeOffset.UtcNow,
        };

        await store.EnqueueAsync(signal);

        await foreach (var msg in store.DequeueAsync(1))
        {
            await store.RejectAsync(msg.Id, requeue: true);
        }

        var pending = await store.GetPendingCountAsync();
        Assert.Equal(1, pending);
    }

    [Fact]
    public async Task Reject_WithoutRequeue_DeletesMessage()
    {
        await using var store = CreateSignalStore();

        var signal = new SignalMessage
        {
            Id = Guid.NewGuid().ToString(),
            SignalType = "TaskSuccess",
            TaskId = Guid.NewGuid().ToString(),
            TaskName = "test.task",
            Payload = "{}",
            CreatedAt = DateTimeOffset.UtcNow,
        };

        await store.EnqueueAsync(signal);

        await foreach (var msg in store.DequeueAsync(1))
        {
            await store.RejectAsync(msg.Id, requeue: false);
        }

        var pending = await store.GetPendingCountAsync();
        Assert.Equal(0, pending);
    }

    [Fact]
    public async Task MultipleMessages_ProcessedInOrder()
    {
        await using var store = CreateSignalStore();

        for (var i = 0; i < 5; i++)
        {
            await store.EnqueueAsync(
                new SignalMessage
                {
                    Id = $"signal-{i}",
                    SignalType = "TaskSuccess",
                    TaskId = $"task-{i}",
                    TaskName = "test.task",
                    Payload = $"{{\"index\": {i}}}",
                    CreatedAt = DateTimeOffset.UtcNow.AddMilliseconds(i),
                }
            );
        }

        var dequeued = new List<SignalMessage>();
        await foreach (var msg in store.DequeueAsync(10))
        {
            dequeued.Add(msg);
        }

        Assert.Equal(5, dequeued.Count);
        Assert.Equal("signal-0", dequeued[0].Id);
    }

    private PostgresSignalStore CreateSignalStore()
    {
        return new PostgresSignalStore(
            Options.Create(
                new PostgresSignalStoreOptions
                {
                    ConnectionString = _connectionString,
                    AutoCreateTables = true,
                }
            ),
            _loggerFactory.CreateLogger<PostgresSignalStore>()
        );
    }
}
