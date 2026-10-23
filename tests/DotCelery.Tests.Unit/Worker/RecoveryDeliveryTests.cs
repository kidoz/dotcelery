using DotCelery.Backend.InMemory.Storage;
using DotCelery.Broker.InMemory;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Canvas;
using DotCelery.Core.Models;
using DotCelery.Core.Sagas;
using DotCelery.Core.Serialization;
using DotCelery.Core.Signals;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Filters;
using DotCelery.Worker.Registry;
using DotCelery.Worker.Sagas;
using DotCelery.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;

namespace DotCelery.Tests.Unit.Worker;

public sealed class RecoveryDeliveryTests : IAsyncDisposable
{
    private readonly FakeTimeProvider _time = new();
    private readonly ResultBackend _results;
    private readonly InboxStore _inbox;
    private readonly SagaStore _sagas;
    private readonly InMemoryBroker _broker = new();
    private readonly SagaOrchestrator _orchestrator;
    private readonly ServiceProvider _services;
    private readonly Counter _counter = new();
    private readonly CriticalHandler _handler = new();
    private readonly TaskExecutor _executor;
    private readonly RevocationManager _revocations;

    public RecoveryDeliveryTests()
    {
        var storage = new InMemoryStorageProvider(_time);
        _results = new ResultBackend(storage, timeProvider: _time);
        _inbox = new InboxStore(storage, timeProvider: _time);
        _sagas = new SagaStore(storage, timeProvider: _time);
        var sagaOptions = Options.Create(new SagaOptions { DispatchSignals = false });
        _orchestrator = new SagaOrchestrator(
            _sagas,
            _broker,
            new JsonMessageSerializer(),
            _results,
            sagaOptions,
            NullLogger<SagaOrchestrator>.Instance,
            timeProvider: _time
        );
        var sagaHandler = new SagaCompletionHandler(
            _sagas,
            _orchestrator,
            sagaOptions,
            NullLogger<SagaCompletionHandler>.Instance,
            timeProvider: _time
        );
        _services = new ServiceCollection()
            .AddLogging()
            .AddSingleton(_counter)
            .AddSingleton<IInboxStore>(_inbox)
            .AddTransient<InboxDeduplicationFilter>()
            .AddSingleton<ITaskSignalHandler<TaskSuccessSignal>>(_handler)
            .AddSingleton<ITaskSignalHandler<TaskSuccessSignal>>(sagaHandler)
            .AddSingleton<ITaskSignalHandler<TaskPostRunSignal>>(_handler)
            .AddTransient<EchoTask>()
            .BuildServiceProvider();
        var options = Options.Create(
            new WorkerOptions { EnableRevocation = false, EnableRateLimiting = false }
        );
        _revocations = new RevocationManager(options, NullLogger<RevocationManager>.Instance);
        var registry = new TaskRegistry();
        registry.Register(typeof(EchoTask), EchoTask.TaskName);
        var filters = new TaskFilterOptions();
        filters.GlobalFilterTypes.Add(typeof(InboxDeduplicationFilter));
        _executor = new TaskExecutor(
            registry,
            _services,
            new JsonMessageSerializer(),
            _results,
            _revocations,
            new TaskFilterPipeline(
                _services,
                Options.Create(filters),
                NullLogger<TaskFilterPipeline>.Instance
            ),
            options,
            NullLogger<TaskExecutor>.Instance,
            signalDispatcher: new TaskSignalDispatcher(
                _services,
                NullLogger<TaskSignalDispatcher>.Instance
            ),
            inboxStore: _inbox,
            timeProvider: _time
        );
    }

    [Fact]
    public async Task CriticalSignalFailure_ReplaysTheSignalWithoutRepeatingTheBody()
    {
        _handler.Fail = true;
        var delivery = Delivery("signal");
        await Assert.ThrowsAsync<IOException>(() =>
            _executor.ExecuteAsync(delivery, "worker", CancellationToken.None)
        );
        var recorded = await _results.GetResultAsync("signal");
        Assert.Equal(TaskState.Success, recorded!.State);
        Assert.False(recorded.SignalsPublished);
        Assert.True(await _inbox.IsProcessedAsync("signal"));
        _handler.Fail = false;

        var recovered = await _executor.ExecuteAsync(delivery, "recovery", CancellationToken.None);

        Assert.True(recovered.IsDuplicate);
        Assert.True(recovered.SignalsPublished);
        Assert.Equal(recorded.Result, recovered.Result);
        Assert.Equal(recorded.CompletedAt, recovered.CompletedAt);
        Assert.Equal(1, _counter.Calls);
        Assert.Equal(2, _handler.Calls);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PostRunFailure_ReplaysSuccessWithoutAdvancingAnUnrelatedSagaStep(
        bool nextStepCompleted
    )
    {
        await _sagas.CreateAsync(
            new Saga
            {
                Id = "saga",
                Name = "replay",
                State = SagaState.Executing,
                CreatedAt = _time.GetUtcNow(),
                Steps =
                [
                    new SagaStep
                    {
                        Id = "step-0",
                        Name = "first",
                        Order = 0,
                        State = SagaStepState.Executing,
                        ExecuteTaskId = "saga-first",
                        ExecuteTask = new Signature { TaskName = EchoTask.TaskName },
                    },
                    new SagaStep
                    {
                        Id = "step-1",
                        Name = "second",
                        Order = 1,
                        ExecuteTask = new Signature { TaskName = EchoTask.TaskName },
                    },
                ],
            }
        );
        _handler.PostRun = async () =>
        {
            Assert.Equal(1, (await _sagas.GetAsync("saga"))!.CurrentStepIndex);
            if (nextStepCompleted)
                await _sagas.UpdateStepStateAsync("saga", "step-1", SagaStepState.Completed);
            throw new IOException("Stopped after success dispatch, before its checkpoint");
        };
        var delivery = Delivery("saga-first");
        await Assert.ThrowsAsync<IOException>(() =>
            _executor.ExecuteAsync(delivery, "worker", CancellationToken.None)
        );
        var recorded = (await _results.GetResultAsync("saga-first"))!;
        Assert.False(recorded.SignalsPublished);
        Assert.True(await _inbox.IsProcessedAsync("saga-first"));
        _handler.PostRun = null;

        var replayed = await _executor.ExecuteAsync(delivery, "recovery", CancellationToken.None);

        Assert.True(replayed.IsDuplicate);
        Assert.True(replayed.SignalsPublished);
        Assert.Equal(recorded.Result, replayed.Result);
        Assert.Equal(1, _counter.Calls);
        var saga = (await _sagas.GetAsync("saga"))!;
        Assert.Equal(1, saga.CurrentStepIndex);
        Assert.Equal(SagaState.Executing, saga.State);
        Assert.Equal(1, _broker.GetQueueLength("celery"));
        if (!nextStepCompleted)
            await _sagas.UpdateStepStateAsync("saga", "step-1", SagaStepState.Completed);
        await _orchestrator.ContinueStepAsync("saga", "step-1");
        Assert.Equal(SagaState.Completed, (await _sagas.GetAsync("saga"))!.State);
    }

    [Theory]
    [InlineData("plain")]
    [InlineData("chain")]
    [InlineData("link")]
    public async Task ExpiredResult_UsesTheRetainedInboxOutcomeWithoutRerunningTheBody(
        string continuation
    )
    {
        var delivery = Delivery("retained", continuation);
        var original = await _executor.ExecuteAsync(delivery, "worker", CancellationToken.None);
        _time.Advance(TimeSpan.FromDays(2));
        Assert.Null(await _results.GetResultAsync("retained"));

        var duplicate = await _executor.ExecuteAsync(delivery, "recovery", CancellationToken.None);

        Assert.True(duplicate.IsDuplicate);
        Assert.Equal(original.Result, duplicate.Result);
        Assert.Equal(original.CompletedAt, duplicate.CompletedAt);
        Assert.Equal(original.FollowUpsPublished, duplicate.FollowUpsPublished);
        Assert.True(duplicate.SignalsPublished);
        Assert.Equal(1, _counter.Calls);
        Assert.Equal(1, _handler.Calls);
    }

    [Fact]
    public async Task LegacyMarkerWithoutRecoveryData_IsRejectedRatherThanRetriedAsInfrastructureFailure()
    {
        await _inbox.MarkProcessedAsync("legacy");
        var result = await _executor.ExecuteAsync(
            Delivery("legacy", "chain"),
            "recovery",
            CancellationToken.None
        );
        Assert.Equal(TaskState.Rejected, result.State);
        Assert.Equal(TaskState.Rejected, (await _results.GetResultAsync("legacy"))!.State);
        Assert.Equal(0, _counter.Calls);
    }

    private BrokerMessage Delivery(string id, string continuation = "plain") =>
        new()
        {
            DeliveryTag = id,
            Queue = "celery",
            ReceivedAt = _time.GetUtcNow(),
            Message = new TaskMessage
            {
                Id = id,
                Task = EchoTask.TaskName,
                Args = "{}"u8.ToArray(),
                ContentType = "application/json",
                Timestamp = _time.GetUtcNow(),
                Chain =
                    continuation == "chain"
                        ?
                        [
                            new ChainStep
                            {
                                TaskId = "next",
                                Signature = new Signature { TaskName = EchoTask.TaskName },
                            },
                        ]
                        : null,
                Link =
                    continuation == "link" ? new Signature { TaskName = EchoTask.TaskName } : null,
            },
        };

    public async ValueTask DisposeAsync()
    {
        _revocations.Dispose();
        await _services.DisposeAsync();
        await _results.DisposeAsync();
        await _inbox.DisposeAsync();
        await _sagas.DisposeAsync();
        await _broker.DisposeAsync();
    }

    private sealed class Counter
    {
        public int Calls { get; set; }
    }

    public sealed class Input;

    private sealed class EchoTask(Counter counter) : ITask<Input, Input>
    {
        public static string TaskName => "recovery.echo";

        public Task<Input> ExecuteAsync(
            Input input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            counter.Calls++;
            return Task.FromResult(input);
        }
    }

    private sealed class CriticalHandler
        : ITaskSignalHandler<TaskSuccessSignal>,
            ITaskSignalHandler<TaskPostRunSignal>,
            ICriticalTaskSignalHandler
    {
        public bool Fail { get; set; }
        public Func<Task>? PostRun { get; set; }

        public async ValueTask HandleAsync(
            TaskPostRunSignal signal,
            CancellationToken cancellationToken = default
        )
        {
            if (PostRun is not null)
                await PostRun();
        }

        public int Calls { get; private set; }

        public ValueTask HandleAsync(
            TaskSuccessSignal signal,
            CancellationToken cancellationToken = default
        )
        {
            Calls++;
            if (Fail)
                throw new IOException("Callback publication failed");
            return ValueTask.CompletedTask;
        }
    }
}
