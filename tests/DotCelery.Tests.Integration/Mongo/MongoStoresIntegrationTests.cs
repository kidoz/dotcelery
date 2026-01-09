using System.Text.Json;
using DotCelery.Backend.Mongo.Batches;
using DotCelery.Backend.Mongo.DeadLetter;
using DotCelery.Backend.Mongo.DelayedMessageStore;
using DotCelery.Backend.Mongo.Execution;
using DotCelery.Backend.Mongo.Historical;
using DotCelery.Backend.Mongo.Metrics;
using DotCelery.Backend.Mongo.Outbox;
using DotCelery.Backend.Mongo.Partitioning;
using DotCelery.Backend.Mongo.RateLimiting;
using DotCelery.Backend.Mongo.Revocation;
using DotCelery.Backend.Mongo.Sagas;
using DotCelery.Backend.Mongo.Signals;
using DotCelery.Core.Batches;
using DotCelery.Core.Canvas;
using DotCelery.Core.Dashboard;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Models;
using DotCelery.Core.Outbox;
using DotCelery.Core.RateLimiting;
using DotCelery.Core.Sagas;
using DotCelery.Core.Serialization;
using DotCelery.Core.Signals;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.MongoDb;

namespace DotCelery.Tests.Integration.Mongo;

/// <summary>
/// Integration tests for MongoDB stores using Testcontainers.
/// </summary>
[Collection("Mongo")]
public class MongoStoresIntegrationTests : IAsyncLifetime
{
    private readonly MongoDbContainer _container;
    private string _connectionString = string.Empty;
    private ILoggerFactory _loggerFactory = null!;

    public MongoStoresIntegrationTests()
    {
        _container = new MongoDbBuilder("mongo:7").Build();
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

    #region MongoBatchStore Tests

    [Fact]
    public async Task MongoBatchStore_CreateAndGet_Works()
    {
        await using var store = CreateBatchStore();

        var batch = new Batch
        {
            Id = Guid.NewGuid().ToString(),
            Name = "test-batch",
            State = BatchState.Pending,
            TaskIds = ["task1", "task2", "task3"],
            CompletedTaskIds = [],
            FailedTaskIds = [],
            CreatedAt = DateTimeOffset.UtcNow,
        };

        await store.CreateAsync(batch);
        var retrieved = await store.GetAsync(batch.Id);

        Assert.NotNull(retrieved);
        Assert.Equal(batch.Id, retrieved.Id);
        Assert.Equal(batch.Name, retrieved.Name);
        Assert.Equal(3, retrieved.TotalTasks);
    }

    [Fact]
    public async Task MongoBatchStore_MarkTaskCompleted_UpdatesBatch()
    {
        await using var store = CreateBatchStore();

        var batch = new Batch
        {
            Id = Guid.NewGuid().ToString(),
            State = BatchState.Processing,
            TaskIds = ["task1", "task2"],
            CompletedTaskIds = [],
            FailedTaskIds = [],
            CreatedAt = DateTimeOffset.UtcNow,
        };

        await store.CreateAsync(batch);
        var updated = await store.MarkTaskCompletedAsync(batch.Id, "task1");

        Assert.NotNull(updated);
        Assert.Contains("task1", updated.CompletedTaskIds);
        Assert.Equal(1, updated.CompletedCount);
    }

    private MongoBatchStore CreateBatchStore()
    {
        return new MongoBatchStore(
            Options.Create(
                new MongoBatchStoreOptions
                {
                    ConnectionString = _connectionString,
                    DatabaseName = "test_batches_" + Guid.NewGuid().ToString("N")[..8],
                }
            ),
            _loggerFactory.CreateLogger<MongoBatchStore>()
        );
    }

    #endregion

    #region MongoDeadLetterStore Tests

    [Fact]
    public async Task MongoDeadLetterStore_StoreAndGet_Works()
    {
        await using var store = CreateDeadLetterStore();

        var message = new DeadLetterMessage
        {
            Id = Guid.NewGuid().ToString(),
            TaskId = Guid.NewGuid().ToString(),
            TaskName = "test.task",
            Queue = "celery",
            Reason = DeadLetterReason.MaxRetriesExceeded,
            OriginalMessage = "test"u8.ToArray(),
            RetryCount = 3,
            Timestamp = DateTimeOffset.UtcNow,
        };

        await store.StoreAsync(message);
        var retrieved = await store.GetAsync(message.Id);

        Assert.NotNull(retrieved);
        Assert.Equal(message.TaskId, retrieved.TaskId);
        Assert.Equal(message.Reason, retrieved.Reason);
    }

    [Fact]
    public async Task MongoDeadLetterStore_GetCount_ReturnsCorrectCount()
    {
        await using var store = CreateDeadLetterStore();

        await store.StoreAsync(CreateDeadLetterMessage("dlq1"));
        await store.StoreAsync(CreateDeadLetterMessage("dlq2"));
        await store.StoreAsync(CreateDeadLetterMessage("dlq3"));

        var count = await store.GetCountAsync();

        Assert.Equal(3, count);
    }

    private MongoDeadLetterStore CreateDeadLetterStore()
    {
        return new MongoDeadLetterStore(
            Options.Create(
                new MongoDeadLetterStoreOptions
                {
                    ConnectionString = _connectionString,
                    DatabaseName = "test_deadletter_" + Guid.NewGuid().ToString("N")[..8],
                }
            ),
            _loggerFactory.CreateLogger<MongoDeadLetterStore>()
        );
    }

    private static DeadLetterMessage CreateDeadLetterMessage(string id) =>
        new()
        {
            Id = id,
            TaskId = Guid.NewGuid().ToString(),
            TaskName = "test.task",
            Queue = "celery",
            Reason = DeadLetterReason.Failed,
            OriginalMessage = "test"u8.ToArray(),
            RetryCount = 1,
            Timestamp = DateTimeOffset.UtcNow,
        };

    #endregion

    #region MongoDelayedMessageStore Tests

    [Fact]
    public async Task MongoDelayedMessageStore_AddAndGetDue_Works()
    {
        await using var store = CreateDelayedMessageStore();

        var message = new TaskMessage
        {
            Id = Guid.NewGuid().ToString(),
            Task = "test.task",
            Args = JsonSerializer.SerializeToUtf8Bytes(
                new { x = 1 },
                DotCeleryJsonContext.Default.Options
            ),
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
        };

        var deliveryTime = DateTimeOffset.UtcNow.AddMilliseconds(-100);
        await store.AddAsync(message, deliveryTime);

        var dueMessages = new List<TaskMessage>();
        await foreach (var m in store.GetDueMessagesAsync(DateTimeOffset.UtcNow))
        {
            dueMessages.Add(m);
        }

        Assert.Single(dueMessages);
        Assert.Equal(message.Id, dueMessages[0].Id);
    }

    [Fact]
    public async Task MongoDelayedMessageStore_Remove_Works()
    {
        await using var store = CreateDelayedMessageStore();

        var message = new TaskMessage
        {
            Id = Guid.NewGuid().ToString(),
            Task = "test.task",
            Args = "{}"u8.ToArray(),
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
        };

        await store.AddAsync(message, DateTimeOffset.UtcNow.AddHours(1));
        var removed = await store.RemoveAsync(message.Id);
        var count = await store.GetPendingCountAsync();

        Assert.True(removed);
        Assert.Equal(0, count);
    }

    private MongoDelayedMessageStore CreateDelayedMessageStore()
    {
        return new MongoDelayedMessageStore(
            Options.Create(
                new MongoDelayedMessageStoreOptions
                {
                    ConnectionString = _connectionString,
                    DatabaseName = "test_delayed_" + Guid.NewGuid().ToString("N")[..8],
                }
            ),
            _loggerFactory.CreateLogger<MongoDelayedMessageStore>()
        );
    }

    #endregion

    #region MongoRevocationStore Tests

    [Fact]
    public async Task MongoRevocationStore_RevokeAndCheck_Works()
    {
        await using var store = CreateRevocationStore();

        var taskId = Guid.NewGuid().ToString();
        await store.RevokeAsync(taskId, RevokeOptions.WithTermination);

        var isRevoked = await store.IsRevokedAsync(taskId);

        Assert.True(isRevoked);
    }

    [Fact]
    public async Task MongoRevocationStore_BulkRevoke_Works()
    {
        await using var store = CreateRevocationStore();

        var taskIds = new[] { "task1", "task2", "task3" };
        await store.RevokeAsync(taskIds, RevokeOptions.Default);

        foreach (var taskId in taskIds)
        {
            Assert.True(await store.IsRevokedAsync(taskId));
        }
    }

    private MongoRevocationStore CreateRevocationStore()
    {
        return new MongoRevocationStore(
            Options.Create(
                new MongoRevocationStoreOptions
                {
                    ConnectionString = _connectionString,
                    DatabaseName = "test_revocation_" + Guid.NewGuid().ToString("N")[..8],
                }
            ),
            _loggerFactory.CreateLogger<MongoRevocationStore>()
        );
    }

    #endregion

    #region MongoSagaStore Tests

    [Fact]
    public async Task MongoSagaStore_CreateAndGet_Works()
    {
        await using var store = CreateSagaStore();

        var saga = new Saga
        {
            Id = Guid.NewGuid().ToString(),
            Name = "test-saga",
            State = SagaState.Created,
            Steps =
            [
                new SagaStep
                {
                    Id = "step1",
                    Name = "Step 1",
                    Order = 0,
                    ExecuteTask = new Signature { TaskName = "step1.execute" },
                },
            ],
            CreatedAt = DateTimeOffset.UtcNow,
        };

        await store.CreateAsync(saga);
        var retrieved = await store.GetAsync(saga.Id);

        Assert.NotNull(retrieved);
        Assert.Equal(saga.Name, retrieved.Name);
        Assert.Single(retrieved.Steps);
    }

    [Fact]
    public async Task MongoSagaStore_UpdateState_Works()
    {
        await using var store = CreateSagaStore();

        var saga = CreateTestSaga();
        await store.CreateAsync(saga);

        var updated = await store.UpdateStateAsync(saga.Id, SagaState.Executing);

        Assert.NotNull(updated);
        Assert.Equal(SagaState.Executing, updated.State);
        Assert.NotNull(updated.StartedAt);
    }

    [Fact]
    public async Task MongoSagaStore_GetByState_Works()
    {
        await using var store = CreateSagaStore();

        await store.CreateAsync(CreateTestSaga() with { State = SagaState.Created });
        await store.CreateAsync(CreateTestSaga() with { State = SagaState.Executing });
        await store.CreateAsync(CreateTestSaga() with { State = SagaState.Created });

        var createdSagas = new List<Saga>();
        await foreach (var saga in store.GetByStateAsync(SagaState.Created))
        {
            createdSagas.Add(saga);
        }

        Assert.Equal(2, createdSagas.Count);
    }

    private MongoSagaStore CreateSagaStore()
    {
        return new MongoSagaStore(
            Options.Create(
                new MongoSagaStoreOptions
                {
                    ConnectionString = _connectionString,
                    DatabaseName = "test_saga_" + Guid.NewGuid().ToString("N")[..8],
                }
            ),
            _loggerFactory.CreateLogger<MongoSagaStore>()
        );
    }

    private static Saga CreateTestSaga() =>
        new()
        {
            Id = Guid.NewGuid().ToString(),
            Name = "test-saga",
            State = SagaState.Created,
            Steps =
            [
                new SagaStep
                {
                    Id = Guid.NewGuid().ToString(),
                    Name = "Step 1",
                    Order = 0,
                    ExecuteTask = new Signature { TaskName = "step1.execute" },
                },
            ],
            CreatedAt = DateTimeOffset.UtcNow,
        };

    #endregion

    #region MongoRateLimiter Tests

    [Fact]
    public async Task MongoRateLimiter_TryAcquire_UnderLimit_Succeeds()
    {
        await using var limiter = CreateRateLimiter();
        var policy = RateLimitPolicy.PerMinute(10);

        var lease = await limiter.TryAcquireAsync("resource1", policy);

        Assert.True(lease.IsAcquired);
    }

    [Fact]
    public async Task MongoRateLimiter_TryAcquire_OverLimit_Fails()
    {
        await using var limiter = CreateRateLimiter();
        var policy = new RateLimitPolicy { Limit = 2, Window = TimeSpan.FromMinutes(1) };

        await limiter.TryAcquireAsync("resource2", policy);
        await limiter.TryAcquireAsync("resource2", policy);
        var lease = await limiter.TryAcquireAsync("resource2", policy);

        Assert.False(lease.IsAcquired);
        Assert.NotNull(lease.RetryAfter);
    }

    private MongoRateLimiter CreateRateLimiter()
    {
        return new MongoRateLimiter(
            Options.Create(
                new MongoRateLimiterOptions
                {
                    ConnectionString = _connectionString,
                    DatabaseName = "test_ratelimit_" + Guid.NewGuid().ToString("N")[..8],
                }
            ),
            _loggerFactory.CreateLogger<MongoRateLimiter>()
        );
    }

    #endregion

    #region MongoPartitionLockStore Tests

    [Fact]
    public async Task MongoPartitionLockStore_TryAcquire_Works()
    {
        await using var store = CreatePartitionLockStore();

        var acquired = await store.TryAcquireAsync("partition1", "task1", TimeSpan.FromMinutes(5));

        Assert.True(acquired);
        Assert.True(await store.IsLockedAsync("partition1"));
    }

    [Fact]
    public async Task MongoPartitionLockStore_TryAcquire_AlreadyLocked_Fails()
    {
        await using var store = CreatePartitionLockStore();

        await store.TryAcquireAsync("partition2", "task1", TimeSpan.FromMinutes(5));
        var acquired = await store.TryAcquireAsync("partition2", "task2", TimeSpan.FromMinutes(5));

        Assert.False(acquired);
    }

    [Fact]
    public async Task MongoPartitionLockStore_Release_Works()
    {
        await using var store = CreatePartitionLockStore();

        await store.TryAcquireAsync("partition3", "task1", TimeSpan.FromMinutes(5));
        var released = await store.ReleaseAsync("partition3", "task1");

        Assert.True(released);
        Assert.False(await store.IsLockedAsync("partition3"));
    }

    private MongoPartitionLockStore CreatePartitionLockStore()
    {
        return new MongoPartitionLockStore(
            Options.Create(
                new MongoPartitionLockStoreOptions
                {
                    ConnectionString = _connectionString,
                    DatabaseName = "test_partlock_" + Guid.NewGuid().ToString("N")[..8],
                }
            ),
            _loggerFactory.CreateLogger<MongoPartitionLockStore>()
        );
    }

    #endregion

    #region MongoTaskExecutionTracker Tests

    [Fact]
    public async Task MongoTaskExecutionTracker_TryStart_Works()
    {
        await using var tracker = CreateTaskExecutionTracker();

        var started = await tracker.TryStartAsync(
            "task.name",
            "task-id-1",
            timeout: TimeSpan.FromMinutes(5)
        );

        Assert.True(started);
        Assert.True(await tracker.IsExecutingAsync("task.name"));
    }

    [Fact]
    public async Task MongoTaskExecutionTracker_TryStart_AlreadyRunning_Fails()
    {
        await using var tracker = CreateTaskExecutionTracker();

        await tracker.TryStartAsync("task.name2", "task-id-1", timeout: TimeSpan.FromMinutes(5));
        var started = await tracker.TryStartAsync(
            "task.name2",
            "task-id-2",
            timeout: TimeSpan.FromMinutes(5)
        );

        Assert.False(started);
    }

    [Fact]
    public async Task MongoTaskExecutionTracker_Stop_Works()
    {
        await using var tracker = CreateTaskExecutionTracker();

        await tracker.TryStartAsync("task.name3", "task-id-1", timeout: TimeSpan.FromMinutes(5));
        await tracker.StopAsync("task.name3", "task-id-1");

        Assert.False(await tracker.IsExecutingAsync("task.name3"));
    }

    private MongoTaskExecutionTracker CreateTaskExecutionTracker()
    {
        return new MongoTaskExecutionTracker(
            Options.Create(
                new MongoTaskExecutionTrackerOptions
                {
                    ConnectionString = _connectionString,
                    DatabaseName = "test_execution_" + Guid.NewGuid().ToString("N")[..8],
                }
            ),
            _loggerFactory.CreateLogger<MongoTaskExecutionTracker>()
        );
    }

    #endregion

    #region MongoQueueMetrics Tests

    [Fact]
    public async Task MongoQueueMetrics_RecordEnqueued_IncrementsWaiting()
    {
        await using var metrics = CreateQueueMetrics();

        await metrics.RecordEnqueuedAsync("test-queue");
        await metrics.RecordEnqueuedAsync("test-queue");
        var waiting = await metrics.GetWaitingCountAsync("test-queue");

        Assert.Equal(2, waiting);
    }

    [Fact]
    public async Task MongoQueueMetrics_RecordStartedAndCompleted_Works()
    {
        await using var metrics = CreateQueueMetrics();

        await metrics.RecordEnqueuedAsync("test-queue2");
        await metrics.RecordStartedAsync("test-queue2", "task1");
        await metrics.RecordCompletedAsync(
            "test-queue2",
            "task1",
            success: true,
            TimeSpan.FromMilliseconds(100)
        );

        var data = await metrics.GetMetricsAsync("test-queue2");

        Assert.Equal(1, data.ProcessedCount);
        Assert.Equal(1, data.SuccessCount);
    }

    private MongoQueueMetrics CreateQueueMetrics()
    {
        return new MongoQueueMetrics(
            Options.Create(
                new MongoQueueMetricsOptions
                {
                    ConnectionString = _connectionString,
                    DatabaseName = "test_metrics_" + Guid.NewGuid().ToString("N")[..8],
                }
            ),
            _loggerFactory.CreateLogger<MongoQueueMetrics>()
        );
    }

    #endregion

    #region MongoHistoricalDataStore Tests

    [Fact]
    public async Task MongoHistoricalDataStore_RecordAndGet_Works()
    {
        await using var store = CreateHistoricalDataStore();

        var snapshot = new MetricsSnapshot
        {
            Timestamp = DateTimeOffset.UtcNow,
            TaskName = "test.task",
            SuccessCount = 10,
            FailureCount = 2,
        };

        await store.RecordMetricsAsync(snapshot);
        var count = await store.GetSnapshotCountAsync();

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task MongoHistoricalDataStore_GetMetrics_Works()
    {
        await using var store = CreateHistoricalDataStore();
        var now = DateTimeOffset.UtcNow;

        await store.RecordMetricsAsync(
            new MetricsSnapshot
            {
                Timestamp = now.AddMinutes(-30),
                SuccessCount = 10,
                FailureCount = 2,
            }
        );
        await store.RecordMetricsAsync(
            new MetricsSnapshot
            {
                Timestamp = now.AddMinutes(-15),
                SuccessCount = 20,
                FailureCount = 3,
            }
        );

        var metrics = await store.GetMetricsAsync(now.AddHours(-1), now);

        Assert.Equal(30, metrics.SuccessCount);
        Assert.Equal(5, metrics.FailureCount);
    }

    private MongoHistoricalDataStore CreateHistoricalDataStore()
    {
        return new MongoHistoricalDataStore(
            Options.Create(
                new MongoHistoricalDataStoreOptions
                {
                    ConnectionString = _connectionString,
                    DatabaseName = "test_historical_" + Guid.NewGuid().ToString("N")[..8],
                }
            ),
            _loggerFactory.CreateLogger<MongoHistoricalDataStore>()
        );
    }

    #endregion

    #region MongoOutboxStore Tests

    [Fact]
    public async Task MongoOutboxStore_StoreAndGetPending_Works()
    {
        await using var store = CreateOutboxStore();

        var taskMessage = new TaskMessage
        {
            Id = Guid.NewGuid().ToString(),
            Task = "test.task",
            Args = "{}"u8.ToArray(),
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
        };

        var outboxMessage = new OutboxMessage
        {
            Id = Guid.NewGuid().ToString(),
            TaskMessage = taskMessage,
            CreatedAt = DateTimeOffset.UtcNow,
            Status = OutboxMessageStatus.Pending,
        };

        await store.StoreAsync(outboxMessage);
        var pendingCount = await store.GetPendingCountAsync();

        Assert.Equal(1, pendingCount);
    }

    [Fact]
    public async Task MongoOutboxStore_MarkDispatched_Works()
    {
        await using var store = CreateOutboxStore();

        var taskMessage = new TaskMessage
        {
            Id = Guid.NewGuid().ToString(),
            Task = "test.task",
            Args = "{}"u8.ToArray(),
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
        };

        var outboxMessage = new OutboxMessage
        {
            Id = Guid.NewGuid().ToString(),
            TaskMessage = taskMessage,
            CreatedAt = DateTimeOffset.UtcNow,
            Status = OutboxMessageStatus.Pending,
        };

        await store.StoreAsync(outboxMessage);
        await store.MarkDispatchedAsync(outboxMessage.Id);
        var pendingCount = await store.GetPendingCountAsync();

        Assert.Equal(0, pendingCount);
    }

    [Fact]
    public async Task MongoOutboxStore_GetPending_ReturnsInOrder()
    {
        await using var store = CreateOutboxStore();

        for (var i = 0; i < 5; i++)
        {
            var taskMessage = new TaskMessage
            {
                Id = $"task-{i}",
                Task = "test.task",
                Args = "{}"u8.ToArray(),
                ContentType = "application/json",
                Timestamp = DateTimeOffset.UtcNow,
            };

            await store.StoreAsync(
                new OutboxMessage
                {
                    Id = $"msg-{i}",
                    TaskMessage = taskMessage,
                    CreatedAt = DateTimeOffset.UtcNow,
                    Status = OutboxMessageStatus.Pending,
                }
            );
        }

        var pending = new List<OutboxMessage>();
        await foreach (var msg in store.GetPendingAsync(10))
        {
            pending.Add(msg);
        }

        Assert.Equal(5, pending.Count);
        Assert.Equal("msg-0", pending[0].Id);
    }

    private MongoOutboxStore CreateOutboxStore()
    {
        return new MongoOutboxStore(
            Options.Create(
                new MongoOutboxStoreOptions
                {
                    ConnectionString = _connectionString,
                    DatabaseName = "test_outbox_" + Guid.NewGuid().ToString("N")[..8],
                }
            ),
            _loggerFactory.CreateLogger<MongoOutboxStore>()
        );
    }

    #endregion

    #region MongoInboxStore Tests

    [Fact]
    public async Task MongoInboxStore_MarkProcessed_Works()
    {
        await using var store = CreateInboxStore();

        var messageId = Guid.NewGuid().ToString();
        await store.MarkProcessedAsync(messageId);

        var isProcessed = await store.IsProcessedAsync(messageId);

        Assert.True(isProcessed);
    }

    [Fact]
    public async Task MongoInboxStore_IsProcessed_ReturnsFalseForNew()
    {
        await using var store = CreateInboxStore();

        var isProcessed = await store.IsProcessedAsync("unknown-message");

        Assert.False(isProcessed);
    }

    [Fact]
    public async Task MongoInboxStore_GetCount_Works()
    {
        await using var store = CreateInboxStore();

        await store.MarkProcessedAsync("msg1");
        await store.MarkProcessedAsync("msg2");
        await store.MarkProcessedAsync("msg3");

        var count = await store.GetCountAsync();

        Assert.Equal(3, count);
    }

    [Fact]
    public async Task MongoInboxStore_Cleanup_RemovesOldRecords()
    {
        await using var store = CreateInboxStore();

        await store.MarkProcessedAsync("msg1");
        await store.MarkProcessedAsync("msg2");

        // Cleanup with 0 duration should remove all records
        var removed = await store.CleanupAsync(TimeSpan.Zero);

        Assert.Equal(2, removed);
        Assert.Equal(0, await store.GetCountAsync());
    }

    private MongoInboxStore CreateInboxStore()
    {
        return new MongoInboxStore(
            Options.Create(
                new MongoInboxStoreOptions
                {
                    ConnectionString = _connectionString,
                    DatabaseName = "test_inbox_" + Guid.NewGuid().ToString("N")[..8],
                }
            ),
            _loggerFactory.CreateLogger<MongoInboxStore>()
        );
    }

    #endregion

    #region MongoSignalStore Tests

    [Fact]
    public async Task MongoSignalStore_EnqueueAndDequeue_Works()
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
    public async Task MongoSignalStore_Dequeue_ClaimsMessage()
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

        // After dequeue, pending should still be 0 (message is now processing)
        var pending = await store.GetPendingCountAsync();
        Assert.Equal(0, pending);
    }

    [Fact]
    public async Task MongoSignalStore_Acknowledge_RemovesMessage()
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
    public async Task MongoSignalStore_Reject_RequeuesToQueue()
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

    private MongoSignalStore CreateSignalStore()
    {
        return new MongoSignalStore(
            Options.Create(
                new MongoSignalStoreOptions
                {
                    ConnectionString = _connectionString,
                    DatabaseName = "test_signals_" + Guid.NewGuid().ToString("N")[..8],
                }
            ),
            _loggerFactory.CreateLogger<MongoSignalStore>()
        );
    }

    #endregion
}
