using System.Runtime.CompilerServices;
using DotCelery.Backend.InMemory.Storage;
using DotCelery.Broker.InMemory;
using DotCelery.Core.Abstractions;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using DotCelery.Core.Signals;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Filters;
using DotCelery.Worker.Registry;
using DotCelery.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Unit.Chaos;

/// <summary>
/// Fault-injection tests: a dependency is taken away while messages are flowing, and the worker's
/// guarantees are asserted — a message is not lost, a task runs at least once, a duplicate is
/// suppressed by the inbox, a poison message does not stop the worker, and a message that no
/// worker can ever run is rejected instead of circulating forever.
/// </summary>
public sealed class WorkerChaosTests : IAsyncDisposable
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(15);

    private readonly InMemoryBroker _inMemoryBroker = new();
    private readonly RecordingBroker _broker;
    private readonly InMemoryStorageProvider _storage = new();
    private readonly JsonMessageSerializer _serializer = new();
    private readonly ServiceProvider _services;
    private readonly FaultyResultBackend _backend;
    private readonly InboxStore _inbox;
    private readonly TaskRegistry _registry = new();
    private readonly ExecutionCounter _counter = new();
    private readonly RecordingDeadLetterHandler _deadLetters = new();
    private readonly WorkerOptions _options;
    private CeleryWorkerService? _worker;

    public WorkerChaosTests()
    {
        _broker = new RecordingBroker(_inMemoryBroker);
        _backend = new FaultyResultBackend(new ResultBackend(_storage));
        _inbox = new InboxStore(_storage);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_counter);
        services.AddSingleton<IInboxStore>(_inbox);
        services.AddTransient<CountingTask>();
        services.AddTransient<InboxDeduplicationFilter>();
        _services = services.BuildServiceProvider();

        _options = new WorkerOptions
        {
            Concurrency = 1,
            PrefetchCount = 1,
            EnableRevocation = false,
            EnableRateLimiting = false,
            InfrastructureFailureRequeueDelay = TimeSpan.FromMilliseconds(25),
            ShutdownTimeout = WaitTimeout,
        };

        _registry.Register(typeof(CountingTask), CountingTask.TaskName);
    }

    [Fact]
    public async Task ResultBackendOutage_ReturnsTheMessageAndCompletesAfterRecovery()
    {
        _backend.FailWrites = true;
        var message = CreateMessage(CountingTask.TaskName);
        await _broker.PublishAsync(message);
        await StartWorkerAsync();

        await WaitUntilAsync(
            () => _counter.Runs >= 1 && _broker.Requeues >= 1,
            "the task should run, and its unrecorded outcome should return the message to the broker"
        );

        _backend.FailWrites = false;

        var result = await WaitForResultAsync(message.Id);

        Assert.Equal(TaskState.Success, result.State);
        Assert.True(
            _counter.Runs >= 2,
            $"the task runs again after an infrastructure failure; runs={_counter.Runs}"
        );
    }

    [Fact]
    public async Task DuplicateDelivery_WithTheInbox_RunsTheTaskOnce()
    {
        var message = CreateMessage(CountingTask.TaskName);

        // The same task ID is delivered twice, as an at-least-once broker may do
        await _broker.PublishAsync(message);
        await _broker.PublishAsync(message);
        await StartWorkerAsync();

        var result = await WaitForResultAsync(message.Id);
        await WaitUntilAsync(() => _broker.Acks >= 2, "both deliveries should be settled");

        Assert.Equal(TaskState.Success, result.State);
        Assert.Equal(1, _counter.Runs);
    }

    [Fact]
    public async Task UnknownTask_IsDeadLetteredAndTheWorkerKeepsProcessing()
    {
        var poison = CreateMessage("chaos.unknown");
        var valid = CreateMessage(CountingTask.TaskName);
        await _broker.PublishAsync(poison);
        await _broker.PublishAsync(valid);
        await StartWorkerAsync();

        var result = await WaitForResultAsync(valid.Id);

        Assert.Equal(TaskState.Success, result.State);
        Assert.Equal(1, _counter.Runs);
        Assert.Contains(
            _deadLetters.Handled,
            handled =>
                handled.Message.Id == poison.Id && handled.Reason == DeadLetterReason.UnknownTask
        );
        Assert.Equal(0, _broker.Requeues);
    }

    [Fact]
    public async Task MalformedPayload_IsRejectedInsteadOfRetried()
    {
        var malformed = CreateMessage(CountingTask.TaskName) with
        {
            Args = "{ not json"u8.ToArray(),
        };
        await _broker.PublishAsync(malformed);
        await StartWorkerAsync();

        var result = await WaitForResultAsync(malformed.Id);

        Assert.Equal(TaskState.Rejected, result.State);
        Assert.Equal(0, _counter.Runs);
        Assert.Equal(0, _broker.Requeues);
        Assert.True(_broker.Acks >= 1, "the rejected message is settled");
        Assert.Contains(
            _deadLetters.Handled,
            handled =>
                handled.Message.Id == malformed.Id && handled.Reason == DeadLetterReason.Rejected
        );
    }

    public async ValueTask DisposeAsync()
    {
        if (_worker is not null)
        {
            await _worker.StopAsync(CancellationToken.None);
            _worker.Dispose();
        }

        await _services.DisposeAsync();
        await _broker.DisposeAsync();
    }

    private async Task StartWorkerAsync()
    {
        var options = Options.Create(_options);
        var filterOptions = new TaskFilterOptions();
        filterOptions.GlobalFilterTypes.Add(typeof(InboxDeduplicationFilter));

        var executor = new TaskExecutor(
            _registry,
            _services,
            _serializer,
            _backend,
            new RevocationManager(options, NullLogger<RevocationManager>.Instance),
            new TaskFilterPipeline(
                _services,
                Options.Create(filterOptions),
                NullLogger<TaskFilterPipeline>.Instance
            ),
            options,
            NullLogger<TaskExecutor>.Instance,
            signalDispatcher: new TaskSignalDispatcher(
                _services,
                NullLogger<TaskSignalDispatcher>.Instance
            ),
            inboxStore: _inbox
        );

        _worker = new CeleryWorkerService(
            _broker,
            executor,
            options,
            NullLogger<CeleryWorkerService>.Instance,
            deadLetterHandler: _deadLetters
        );

        await _worker.StartAsync(CancellationToken.None);
    }

    private TaskMessage CreateMessage(string taskName) =>
        new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Task = taskName,
            Args = _serializer.Serialize(new CountingInput()),
            ContentType = _serializer.ContentType,
            Timestamp = DateTimeOffset.UtcNow,
            Queue = "celery",
        };

    private async Task<TaskResult> WaitForResultAsync(string taskId)
    {
        var deadline = DateTime.UtcNow + WaitTimeout;
        while (DateTime.UtcNow < deadline)
        {
            var result = await _backend.GetResultAsync(taskId, CancellationToken.None);
            if (result is not null)
            {
                return result;
            }

            await Task.Delay(25);
        }

        Assert.Fail($"No result was stored for task {taskId} within {WaitTimeout}.");
        return null!;
    }

    private static async Task WaitUntilAsync(Func<bool> condition, string description)
    {
        var deadline = DateTime.UtcNow + WaitTimeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(25);
        }

        Assert.Fail($"Timed out after {WaitTimeout}: {description}.");
    }

    private sealed record CountingInput
    {
        public int Value { get; init; }
    }

    private sealed class CountingTask(ExecutionCounter counter) : ITask<CountingInput>
    {
        public static string TaskName => "chaos.count";

        public Task ExecuteAsync(
            CountingInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            counter.Increment();
            return Task.CompletedTask;
        }
    }

    private sealed class ExecutionCounter
    {
        private int _runs;

        public int Runs => Volatile.Read(ref _runs);

        public void Increment() => Interlocked.Increment(ref _runs);
    }

    private sealed class FaultyResultBackend(IResultBackend inner) : IResultBackend
    {
        private volatile bool _failWrites;

        public bool FailWrites
        {
            get => _failWrites;
            set => _failWrites = value;
        }

        public ValueTask StoreResultAsync(
            TaskResult result,
            TimeSpan? expiry = null,
            CancellationToken cancellationToken = default
        ) =>
            _failWrites
                ? ValueTask.FromException(
                    new InvalidOperationException("The result backend is unavailable.")
                )
                : inner.StoreResultAsync(result, expiry, cancellationToken);

        public ValueTask<TaskResult?> GetResultAsync(
            string taskId,
            CancellationToken cancellationToken = default
        ) => inner.GetResultAsync(taskId, cancellationToken);

        public Task<TaskResult> WaitForResultAsync(
            string taskId,
            TimeSpan? timeout = null,
            CancellationToken cancellationToken = default
        ) => inner.WaitForResultAsync(taskId, timeout, cancellationToken);

        public ValueTask UpdateStateAsync(
            string taskId,
            TaskState state,
            object? metadata = null,
            CancellationToken cancellationToken = default
        ) => inner.UpdateStateAsync(taskId, state, metadata, cancellationToken);

        public ValueTask<TaskState?> GetStateAsync(
            string taskId,
            CancellationToken cancellationToken = default
        ) => inner.GetStateAsync(taskId, cancellationToken);

        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    // Counts what the worker settled, so the test can tell a requeue from a rejection
    private sealed class RecordingBroker(IMessageBroker inner) : IMessageBroker
    {
        private int _acks;
        private int _requeues;
        private int _rejections;

        public int Acks => Volatile.Read(ref _acks);

        public int Requeues => Volatile.Read(ref _requeues);

        public int Rejections => Volatile.Read(ref _rejections);

        public ValueTask PublishAsync(
            TaskMessage message,
            CancellationToken cancellationToken = default
        ) => inner.PublishAsync(message, cancellationToken);

        public async IAsyncEnumerable<BrokerMessage> ConsumeAsync(
            IReadOnlyList<string> queues,
            [EnumeratorCancellation] CancellationToken cancellationToken = default
        )
        {
            await foreach (
                var message in inner
                    .ConsumeAsync(queues, cancellationToken)
                    .ConfigureAwait(false)
            )
            {
                yield return message;
            }
        }

        public ValueTask AckAsync(
            BrokerMessage message,
            CancellationToken cancellationToken = default
        )
        {
            Interlocked.Increment(ref _acks);
            return inner.AckAsync(message, cancellationToken);
        }

        public ValueTask RejectAsync(
            BrokerMessage message,
            bool requeue = false,
            CancellationToken cancellationToken = default
        )
        {
            if (requeue)
            {
                Interlocked.Increment(ref _requeues);
            }
            else
            {
                Interlocked.Increment(ref _rejections);
            }

            return inner.RejectAsync(message, requeue, cancellationToken);
        }

        public ValueTask<bool> IsHealthyAsync(CancellationToken cancellationToken = default) =>
            inner.IsHealthyAsync(cancellationToken);

        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    private sealed class RecordingDeadLetterHandler : IDeadLetterHandler
    {
        private readonly List<(TaskMessage Message, DeadLetterReason Reason)> _handled = [];

        public IReadOnlyList<(TaskMessage Message, DeadLetterReason Reason)> Handled
        {
            get
            {
                lock (_handled)
                {
                    return [.. _handled];
                }
            }
        }

        public ValueTask HandleAsync(
            TaskMessage message,
            DeadLetterReason reason,
            Exception? exception = null,
            string? worker = null,
            CancellationToken cancellationToken = default
        )
        {
            lock (_handled)
            {
                _handled.Add((message, reason));
            }

            return ValueTask.CompletedTask;
        }
    }
}
