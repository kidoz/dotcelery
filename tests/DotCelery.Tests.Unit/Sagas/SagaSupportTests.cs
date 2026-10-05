using DotCelery.Backend.InMemory.Storage;
using DotCelery.Broker.InMemory;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Canvas;
using DotCelery.Core.Extensions;
using DotCelery.Core.Models;
using DotCelery.Core.Sagas;
using DotCelery.Core.Serialization;
using DotCelery.Core.Signals;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Extensions;
using DotCelery.Worker.Filters;
using DotCelery.Worker.Registry;
using DotCelery.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Unit.Sagas;

/// <summary>
/// Runs sagas through a worker after registering them with <c>AddSagaSupport()</c>, so the
/// registration and the transitions are proven together and not only per component.
/// </summary>
public sealed class SagaSupportTests : IAsyncDisposable
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(15);

    private readonly InMemoryBroker _broker = new();
    private readonly InMemoryStorageProvider _storage = new();
    private readonly JsonMessageSerializer _serializer = new();
    private readonly ServiceProvider _services;
    private readonly SagaStore _sagaStore;
    private readonly ResultBackend _backend;
    private readonly CeleryWorkerService _worker;

    public SagaSupportTests()
    {
        _sagaStore = new SagaStore(_storage);
        _backend = new ResultBackend(_storage);

        var broker = _broker;
        var serializer = _serializer;
        var backend = _backend;
        var services = new ServiceCollection();

        // What a host provides, which the extensions rely on
        services.AddLogging();
        services.AddOptions();

        services.AddSingleton<ISagaStore>(_sagaStore);
        services.AddSingleton<IMessageBroker>(broker);
        services.AddSingleton<IMessageSerializer>(serializer);
        services.AddSingleton<IResultBackend>(backend);
        services.AddTransient<ReserveStockTask>();
        services.AddTransient<ChargeCardTask>();
        services.AddTransient<ReleaseStockTask>();

        var builder = new DotCeleryBuilder(services);
        builder.AddSagaSupport();

        services.AddSingleton<ITaskSignalDispatcher>(provider => new TaskSignalDispatcher(
            provider,
            NullLogger<TaskSignalDispatcher>.Instance
        ));

        _services = services.BuildServiceProvider();

        var workerOptions = Options.Create(
            new WorkerOptions
            {
                Concurrency = 1,
                PrefetchCount = 1,
                EnableRevocation = false,
                EnableRateLimiting = false,
                InfrastructureFailureRequeueDelay = TimeSpan.Zero,
                ShutdownTimeout = WaitTimeout,
            }
        );
        var registry = new TaskRegistry();
        registry.Register(typeof(ReserveStockTask), ReserveStockTask.TaskName);
        registry.Register(typeof(ChargeCardTask), ChargeCardTask.TaskName);
        registry.Register(typeof(ReleaseStockTask), ReleaseStockTask.TaskName);

        var executor = new TaskExecutor(
            registry,
            _services,
            serializer,
            backend,
            new RevocationManager(workerOptions, NullLogger<RevocationManager>.Instance),
            new TaskFilterPipeline(
                _services,
                Options.Create(new TaskFilterOptions()),
                NullLogger<TaskFilterPipeline>.Instance
            ),
            workerOptions,
            NullLogger<TaskExecutor>.Instance,
            signalDispatcher: _services.GetRequiredService<ITaskSignalDispatcher>()
        );

        _worker = new CeleryWorkerService(
            broker,
            executor,
            workerOptions,
            NullLogger<CeleryWorkerService>.Instance,
            timeProvider: null,
            deadLetterHandler: null
        );
    }

    [Fact]
    public async Task AddSagaSupport_RegistersTheOrchestrator()
    {
        Assert.NotNull(_services.GetRequiredService<ISagaOrchestrator>());
    }

    [Fact]
    public async Task Saga_RunsItsStepsInOrderWhenTheySucceed()
    {
        var orchestrator = _services.GetRequiredService<ISagaOrchestrator>();
        var saga = CreateSaga(sagaId: "saga-completes");

        await orchestrator.StartAsync(saga);
        await _worker.StartAsync(CancellationToken.None);

        var finished = await WaitForSagaAsync(orchestrator, "saga-completes");

        Assert.Equal(SagaState.Completed, finished.State);
        Assert.Equal(2, finished.Steps.Count(step => step.State == SagaStepState.Completed));
    }

    [Fact]
    public async Task Saga_CompensatesCompletedStepsWhenOneFails()
    {
        var orchestrator = _services.GetRequiredService<ISagaOrchestrator>();
        var saga = CreateSaga(sagaId: "saga-compensates", failSecondStep: true);

        await orchestrator.StartAsync(saga);
        await _worker.StartAsync(CancellationToken.None);

        var finished = await WaitForSagaAsync(orchestrator, "saga-compensates");

        Assert.Equal(SagaState.Compensated, finished.State);
        Assert.Equal(SagaStepState.Compensated, finished.Steps[0].State);
        Assert.Equal(SagaStepState.Failed, finished.Steps[1].State);
    }

    public async ValueTask DisposeAsync()
    {
        await _worker.StopAsync(CancellationToken.None);
        _worker.Dispose();
        await _services.DisposeAsync();
        await _backend.DisposeAsync();
        await _sagaStore.DisposeAsync();
        await _broker.DisposeAsync();
    }

    private static Saga CreateSaga(string sagaId, bool failSecondStep = false)
    {
        var serializer = new JsonMessageSerializer();

        return new Saga
        {
            Id = sagaId,
            Name = "place-order",
            State = SagaState.Created,
            CreatedAt = DateTimeOffset.UtcNow,
            Steps =
            [
                new SagaStep
                {
                    Id = "reserve",
                    Name = "Reserve stock",
                    Order = 1,
                    ExecuteTask = new Signature
                    {
                        TaskName = ReserveStockTask.TaskName,
                        Args = serializer.Serialize(new OrderInput()),
                    },
                    CompensateTask = new Signature
                    {
                        TaskName = ReleaseStockTask.TaskName,
                        Args = serializer.Serialize(new OrderInput()),
                    },
                },
                new SagaStep
                {
                    Id = "charge",
                    Name = "Charge card",
                    Order = 2,
                    ExecuteTask = new Signature
                    {
                        TaskName = ChargeCardTask.TaskName,
                        Args = serializer.Serialize(new OrderInput { Fail = failSecondStep }),
                    },
                },
            ],
        };
    }

    private static async Task<Saga> WaitForSagaAsync(ISagaOrchestrator orchestrator, string sagaId)
    {
        using var timeout = new CancellationTokenSource(WaitTimeout);

        while (true)
        {
            timeout.Token.ThrowIfCancellationRequested();

            var saga = await orchestrator.GetAsync(sagaId, CancellationToken.None);
            if (saga is { State: SagaState.Completed or SagaState.Compensated or SagaState.Failed })
            {
                return saga;
            }

            await Task.Delay(20, CancellationToken.None);
        }
    }

    public sealed class OrderInput
    {
        public bool Fail { get; init; }
    }

    public sealed class ReserveStockTask : ITask<OrderInput>
    {
        public static string TaskName => "tests.reserve";

        public Task ExecuteAsync(
            OrderInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        ) => Task.CompletedTask;
    }

    public sealed class ChargeCardTask : ITask<OrderInput>
    {
        public static string TaskName => "tests.charge";

        public Task ExecuteAsync(
            OrderInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        ) =>
            input.Fail
                ? Task.FromException(new InvalidOperationException("Card declined"))
                : Task.CompletedTask;
    }

    public sealed class ReleaseStockTask : ITask<OrderInput>
    {
        public static string TaskName => "tests.release";

        public Task ExecuteAsync(
            OrderInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        ) => Task.CompletedTask;
    }
}
