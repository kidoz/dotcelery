namespace DotCelery.Tests.Unit.Worker;

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using DotCelery.Backend.InMemory;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Exceptions;
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

/// <summary>
/// Tests for how the worker settles broker messages after failures and during shutdown.
/// </summary>
public sealed class CeleryWorkerServiceTests : IAsyncDisposable
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(10);

    private readonly RecordingBroker _broker = new();
    private readonly FaultyResultBackend _backend = new();
    private readonly TaskGate _gate = new();
    private readonly ServiceProvider _serviceProvider;
    private CeleryWorkerService? _worker;

    public CeleryWorkerServiceTests()
    {
        var services = new ServiceCollection();
        services.AddSingleton(_gate);
        services.AddTransient<EchoTask>();
        services.AddTransient<BlockingTask>();
        services.AddTransient<RetryingTask>();
        _serviceProvider = services.BuildServiceProvider();
    }

    [Fact]
    public async Task BackendFailureBeforeExecution_ReturnsMessageToBroker()
    {
        _backend.FailStateUpdates = true;
        var worker = CreateWorker();
        _broker.Enqueue(CreateMessage("t1", EchoTask.TaskName));

        await worker.StartAsync(CancellationToken.None);
        await WaitForAsync(() => _broker.HasEvent("requeue:t1"));
        await worker.StopAsync(CancellationToken.None);

        Assert.False(_broker.HasEvent("reject:t1"));
        Assert.False(_broker.HasEvent("ack:t1"));
    }

    [Fact]
    public async Task ResultStoreFailureAfterSuccess_ReturnsMessageWithoutRecordingFailure()
    {
        _backend.FailResultStores = true;
        var worker = CreateWorker();
        _broker.Enqueue(CreateMessage("t1", EchoTask.TaskName));

        await worker.StartAsync(CancellationToken.None);
        await WaitForAsync(() => _broker.HasEvent("requeue:t1"));
        await worker.StopAsync(CancellationToken.None);

        Assert.False(_broker.HasEvent("ack:t1"));
        Assert.DoesNotContain(_backend.AttemptedStores, r => r.State == TaskState.Failure);
    }

    [Fact]
    public async Task UnknownTask_IsRejectedWithoutRequeue()
    {
        var worker = CreateWorker();
        _broker.Enqueue(CreateMessage("t1", "not.registered"));

        await worker.StartAsync(CancellationToken.None);
        await WaitForAsync(() => _broker.HasEvent("reject:t1"));
        await worker.StopAsync(CancellationToken.None);

        Assert.False(_broker.HasEvent("requeue:t1"));
    }

    [Fact]
    public async Task RetryPublishFailure_ReturnsOriginalMessageToBroker()
    {
        _broker.FailPublish = true;
        var worker = CreateWorker();
        _broker.Enqueue(CreateMessage("t1", RetryingTask.TaskName));

        await worker.StartAsync(CancellationToken.None);
        await WaitForAsync(() => _broker.HasEvent("requeue:t1"));
        await worker.StopAsync(CancellationToken.None);

        Assert.False(_broker.HasEvent("ack:t1"));
    }

    [Fact]
    public async Task AckFailure_DoesNotStopProcessing()
    {
        _broker.FailAckFor = "t1";
        var worker = CreateWorker();
        _broker.Enqueue(CreateMessage("t1", EchoTask.TaskName));
        _broker.Enqueue(CreateMessage("t2", EchoTask.TaskName));

        await worker.StartAsync(CancellationToken.None);
        await WaitForAsync(() => _broker.HasEvent("ack:t2"));
        await worker.StopAsync(CancellationToken.None);

        Assert.True(_broker.HasEvent("ack-failed:t1"));
    }

    [Fact]
    public async Task GracefulShutdown_FinishesRunningTaskBeforeClosingConsumer()
    {
        var worker = CreateWorker();
        _broker.Enqueue(CreateMessage("t1", BlockingTask.TaskName));

        await worker.StartAsync(CancellationToken.None);
        await _gate.Started.Task.WaitAsync(WaitTimeout);

        // t2 is prefetched while t1 runs, so shutdown must hand it back unprocessed
        _broker.Enqueue(CreateMessage("t2", EchoTask.TaskName));
        await WaitForAsync(() => _broker.HasEvent("deliver:t2"));

        var stopping = worker.StopAsync(CancellationToken.None);
        _gate.Release.SetResult();
        await stopping.WaitAsync(WaitTimeout);

        Assert.True(_broker.HasEvent("ack:t1"));
        Assert.True(_broker.HasEvent("requeue:t2"));
        Assert.True(_broker.IndexOf("ack:t1") < _broker.IndexOf("close"));
        Assert.Empty(_broker.UnsettledWhenClosed);
        Assert.Equal(TaskState.Success, (await _backend.GetResultAsync("t1"))?.State);
        Assert.Null(await _backend.GetResultAsync("t2"));
    }

    [Fact]
    public async Task ForcedShutdown_ReturnsInterruptedTaskOnlyAfterItStops()
    {
        var worker = CreateWorker(o => o.ShutdownTimeout = TimeSpan.FromMilliseconds(100));
        _broker.Enqueue(CreateMessage("t1", BlockingTask.TaskName));

        await worker.StartAsync(CancellationToken.None);
        await _gate.Started.Task.WaitAsync(WaitTimeout);
        await worker.StopAsync(CancellationToken.None).WaitAsync(WaitTimeout);

        Assert.True(_gate.Cancelled);
        Assert.True(_broker.HasEvent("requeue:t1"));
        Assert.True(_broker.IndexOf("requeue:t1") < _broker.IndexOf("close"));
        Assert.Empty(_broker.UnsettledWhenClosed);
        Assert.Null(await _backend.GetResultAsync("t1"));
    }

    [Fact]
    public async Task ShutdownWithoutGracePeriod_ReturnsInterruptedTaskWithoutRecordingFailure()
    {
        var worker = CreateWorker(o => o.EnableGracefulShutdown = false);
        _broker.Enqueue(CreateMessage("t1", BlockingTask.TaskName));

        await worker.StartAsync(CancellationToken.None);
        await _gate.Started.Task.WaitAsync(WaitTimeout);
        await worker.StopAsync(CancellationToken.None).WaitAsync(WaitTimeout);

        Assert.True(_broker.HasEvent("requeue:t1"));
        Assert.False(_broker.HasEvent("ack:t1"));
        Assert.DoesNotContain(_backend.AttemptedStores, r => r.State == TaskState.Failure);
    }

    [Fact]
    public async Task BrokerEndsStreamWhileRunning_FailsTheService()
    {
        var worker = CreateWorker();
        _broker.EndStream();

        await worker.StartAsync(CancellationToken.None);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            worker.ExecuteTask!.WaitAsync(WaitTimeout)
        );
    }

    public async ValueTask DisposeAsync()
    {
        _gate.Release.TrySetResult();
        _worker?.Dispose();
        await _serviceProvider.DisposeAsync();
    }

    private CeleryWorkerService CreateWorker(Action<WorkerOptions>? configure = null)
    {
        var options = new WorkerOptions
        {
            Concurrency = 1,
            PrefetchCount = 1,
            EnableRevocation = false,
            EnableRateLimiting = false,
            InfrastructureFailureRequeueDelay = TimeSpan.Zero,
            ShutdownTimeout = WaitTimeout,
        };
        configure?.Invoke(options);
        var workerOptions = Options.Create(options);

        var registry = new TaskRegistry();
        registry.Register(typeof(EchoTask), EchoTask.TaskName);
        registry.Register(typeof(BlockingTask), BlockingTask.TaskName);
        registry.Register(typeof(RetryingTask), RetryingTask.TaskName);

        var executor = new TaskExecutor(
            registry,
            _serviceProvider,
            new JsonMessageSerializer(),
            _backend,
            new RevocationManager(workerOptions, NullLogger<RevocationManager>.Instance),
            new TaskFilterPipeline(
                _serviceProvider,
                Options.Create(new TaskFilterOptions()),
                NullLogger<TaskFilterPipeline>.Instance
            ),
            workerOptions,
            NullLogger<TaskExecutor>.Instance
        );

        _worker = new CeleryWorkerService(
            _broker,
            executor,
            workerOptions,
            NullLogger<CeleryWorkerService>.Instance,
            shutdownHandler: new GracefulShutdownHandler(
                NullLogger<GracefulShutdownHandler>.Instance
            )
        );
        return _worker;
    }

    private static TaskMessage CreateMessage(string taskId, string taskName) =>
        new()
        {
            Id = taskId,
            Task = taskName,
            Args = new JsonMessageSerializer().Serialize(new TestInput { Value = 1 }),
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Queue = "celery",
        };

    private static async Task WaitForAsync(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(WaitTimeout);
        while (!condition())
        {
            await Task.Delay(10, timeout.Token);
        }
    }

    /// <summary>
    /// Broker that records settlements and, like RabbitMQ, returns every unsettled message
    /// when the consumer closes.
    /// </summary>
    private sealed class RecordingBroker : IMessageBroker
    {
        private readonly Channel<BrokerMessage> _pending = Channel.CreateUnbounded<BrokerMessage>();
        private readonly ConcurrentDictionary<string, BrokerMessage> _unsettled = new();
        private readonly ConcurrentQueue<string> _events = new();

        public bool FailPublish { get; set; }

        public string? FailAckFor { get; set; }

        public IReadOnlyCollection<string> UnsettledWhenClosed { get; private set; } = [];

        public void Enqueue(TaskMessage message) =>
            _pending.Writer.TryWrite(
                new BrokerMessage
                {
                    Message = message,
                    DeliveryTag = Guid.NewGuid(),
                    Queue = message.Queue,
                    ReceivedAt = DateTimeOffset.UtcNow,
                }
            );

        public void EndStream() => _pending.Writer.Complete();

        public bool HasEvent(string name) => _events.Contains(name);

        public int IndexOf(string name) => _events.ToList().IndexOf(name);

        public ValueTask PublishAsync(
            TaskMessage message,
            CancellationToken cancellationToken = default
        )
        {
            if (FailPublish)
            {
                return ValueTask.FromException(new InvalidOperationException("Broker unavailable"));
            }

            _events.Enqueue($"publish:{message.Id}");
            return ValueTask.CompletedTask;
        }

        public async IAsyncEnumerable<BrokerMessage> ConsumeAsync(
            IReadOnlyList<string> queues,
            [EnumeratorCancellation] CancellationToken cancellationToken = default
        )
        {
            try
            {
                while (await _pending.Reader.WaitToReadAsync(cancellationToken))
                {
                    while (_pending.Reader.TryRead(out var message))
                    {
                        _unsettled[message.Message.Id] = message;
                        _events.Enqueue($"deliver:{message.Message.Id}");
                        yield return message;
                    }
                }
            }
            finally
            {
                UnsettledWhenClosed = _unsettled.Keys.ToList();
                _events.Enqueue("close");
            }
        }

        public ValueTask AckAsync(
            BrokerMessage message,
            CancellationToken cancellationToken = default
        )
        {
            if (FailAckFor == message.Message.Id)
            {
                _events.Enqueue($"ack-failed:{message.Message.Id}");
                return ValueTask.FromException(new InvalidOperationException("Ack failed"));
            }

            return Settle(message, "ack");
        }

        public ValueTask RejectAsync(
            BrokerMessage message,
            bool requeue = false,
            CancellationToken cancellationToken = default
        ) => Settle(message, requeue ? "requeue" : "reject");

        public ValueTask<bool> IsHealthyAsync(CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(true);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private ValueTask Settle(BrokerMessage message, string action)
        {
            _unsettled.TryRemove(message.Message.Id, out _);
            _events.Enqueue($"{action}:{message.Message.Id}");
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// In-memory result backend whose writes can be made to fail.
    /// </summary>
    private sealed class FaultyResultBackend : IResultBackend
    {
        private readonly InMemoryResultBackend _inner = new();

        public bool FailStateUpdates { get; set; }

        public bool FailResultStores { get; set; }

        public ConcurrentQueue<TaskResult> AttemptedStores { get; } = new();

        public ValueTask StoreResultAsync(
            TaskResult result,
            TimeSpan? expiry = null,
            CancellationToken cancellationToken = default
        )
        {
            AttemptedStores.Enqueue(result);

            return FailResultStores
                ? ValueTask.FromException(new InvalidOperationException("Backend unavailable"))
                : _inner.StoreResultAsync(result, expiry, cancellationToken);
        }

        public ValueTask<TaskResult?> GetResultAsync(
            string taskId,
            CancellationToken cancellationToken = default
        ) => _inner.GetResultAsync(taskId, cancellationToken);

        public Task<TaskResult> WaitForResultAsync(
            string taskId,
            TimeSpan? timeout = null,
            CancellationToken cancellationToken = default
        ) => _inner.WaitForResultAsync(taskId, timeout, cancellationToken);

        public ValueTask UpdateStateAsync(
            string taskId,
            TaskState state,
            object? metadata = null,
            CancellationToken cancellationToken = default
        ) =>
            FailStateUpdates
                ? ValueTask.FromException(new InvalidOperationException("Backend unavailable"))
                : _inner.UpdateStateAsync(taskId, state, metadata, cancellationToken);

        public ValueTask<TaskState?> GetStateAsync(
            string taskId,
            CancellationToken cancellationToken = default
        ) => _inner.GetStateAsync(taskId, cancellationToken);

        public ValueTask DisposeAsync() => _inner.DisposeAsync();
    }

    private sealed class TaskGate
    {
        public TaskCompletionSource Started { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool Cancelled { get; set; }
    }

    private sealed class EchoTask : ITask<TestInput, TestInput>
    {
        public static string TaskName => "test.echo";

        public Task<TestInput> ExecuteAsync(
            TestInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        ) => Task.FromResult(input);
    }

    private sealed class BlockingTask(TaskGate gate) : ITask<TestInput>
    {
        public static string TaskName => "test.blocking";

        public async Task ExecuteAsync(
            TestInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            gate.Started.TrySetResult();

            try
            {
                await gate.Release.Task.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                gate.Cancelled = true;
                throw;
            }
        }
    }

    private sealed class RetryingTask : ITask<TestInput>
    {
        public static string TaskName => "test.retrying";

        public Task ExecuteAsync(
            TestInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        ) => throw new RetryException(countdown: null, new InvalidOperationException("Transient"));
    }

    private sealed class TestInput
    {
        public int Value { get; set; }
    }
}
