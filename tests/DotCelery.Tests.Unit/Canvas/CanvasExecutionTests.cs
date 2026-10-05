using DotCelery.Backend.InMemory.Storage;
using DotCelery.Broker.InMemory;
using DotCelery.Client;
using DotCelery.Client.Canvas;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Batches;
using DotCelery.Core.Canvas;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using DotCelery.Core.Signals;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker;
using DotCelery.Worker.Batches;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Filters;
using DotCelery.Worker.Registry;
using DotCelery.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Unit.Canvas;

/// <summary>
/// Runs a submitted canvas through a worker, so the chain is proven by its results and not only
/// by the messages the client published.
/// </summary>
public sealed class CanvasExecutionTests : IAsyncDisposable
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(15);

    private readonly InMemoryBroker _broker = new();
    private readonly InMemoryStorageProvider _storage = new();
    private readonly JsonMessageSerializer _serializer = new();
    private readonly ServiceProvider _services;
    private readonly ResultBackend _backend;
    private readonly BatchStore _batchStore;
    private readonly CeleryWorkerService _worker;

    public CanvasExecutionTests()
    {
        _backend = new ResultBackend(_storage);
        _batchStore = new BatchStore(_storage);

        var services = new ServiceCollection();
        services.AddTransient<IncrementTask>();
        services.AddTransient<DoubleTask>();
        services.AddTransient<CollectTask>();
        var batchHandler = new BatchCompletionHandler(
            _batchStore,
            _broker,
            _serializer,
            NullLogger<BatchCompletionHandler>.Instance
        );
        services.AddSingleton<ITaskSignalHandler<TaskSuccessSignal>>(batchHandler);
        services.AddSingleton<ITaskSignalHandler<TaskFailureSignal>>(batchHandler);
        _services = services.BuildServiceProvider();

        var options = Options.Create(
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
        registry.Register(typeof(IncrementTask), IncrementTask.TaskName);
        registry.Register(typeof(DoubleTask), DoubleTask.TaskName);
        registry.Register(typeof(CollectTask), CollectTask.TaskName);

        var executor = new TaskExecutor(
            registry,
            _services,
            _serializer,
            _backend,
            new RevocationManager(options, NullLogger<RevocationManager>.Instance),
            new TaskFilterPipeline(
                _services,
                Options.Create(new TaskFilterOptions()),
                NullLogger<TaskFilterPipeline>.Instance
            ),
            options,
            NullLogger<TaskExecutor>.Instance,
            signalDispatcher: new TaskSignalDispatcher(
                _services,
                NullLogger<TaskSignalDispatcher>.Instance
            )
        );

        _worker = new CeleryWorkerService(
            _broker,
            executor,
            options,
            NullLogger<CeleryWorkerService>.Instance,
            shutdownHandler: new GracefulShutdownHandler(
                NullLogger<GracefulShutdownHandler>.Instance
            )
        );
    }

    [Fact]
    public async Task Chain_RunsEveryStepWithThePreviousResult()
    {
        var client = CreateClient();
        var chain = new Chain(
            new Signature<IncrementTask, NumberInput, NumberInput>
            {
                Input = new NumberInput { Value = 1 },
            },
            new Signature<DoubleTask, NumberInput, NumberInput>(),
            new Signature<IncrementTask, NumberInput, NumberInput>()
        );

        var result = await client.SendChainAsync(chain);
        await _worker.StartAsync(CancellationToken.None);

        // 1 -> increment 2 -> double 4 -> increment 5
        var last = await WaitForResultAsync(result.LastTaskId);

        Assert.Equal(TaskState.Success, last.State);
        Assert.Equal(5, _serializer.Deserialize<NumberInput>(last.Result!)!.Value);
    }

    [Fact]
    public async Task Chord_RunsItsCallbackWhenEveryHeaderTaskFinished()
    {
        var client = CreateClient();
        var chord = new Group(
            new Signature<IncrementTask, NumberInput, NumberInput>
            {
                Input = new NumberInput { Value = 1 },
            },
            new Signature<IncrementTask, NumberInput, NumberInput>
            {
                Input = new NumberInput { Value = 2 },
            }
        ).WithCallback(
            new Signature<CollectTask, NumberInput> { Input = new NumberInput { Value = 9 } }
        );

        var result = await client.SendChordAsync(chord);
        await _worker.StartAsync(CancellationToken.None);

        var callbackResult = await WaitForResultAsync(result.CallbackTaskId);

        Assert.Equal(TaskState.Success, callbackResult.State);

        var batch = await _batchStore.GetAsync(result.Id);
        Assert.NotNull(batch);
        Assert.True(batch.IsFinished);
        Assert.Equal(2, batch.CompletedCount);
        Assert.NotNull(batch.CallbackDispatchedAt);
    }

    public async ValueTask DisposeAsync()
    {
        await _worker.StopAsync(CancellationToken.None);
        _worker.Dispose();
        await _services.DisposeAsync();
        await _backend.DisposeAsync();
        await _batchStore.DisposeAsync();
        await _broker.DisposeAsync();
    }

    private CanvasClient CreateClient() =>
        new(
            _broker,
            _serializer,
            Options.Create(new CeleryClientOptions()),
            NullLogger<CanvasClient>.Instance,
            _batchStore
        );

    private async Task<TaskResult> WaitForResultAsync(string taskId)
    {
        using var timeout = new CancellationTokenSource(WaitTimeout);
        return await _backend.WaitForResultAsync(taskId, WaitTimeout, timeout.Token);
    }

    public sealed class NumberInput
    {
        public int Value { get; init; }
    }

    public sealed class IncrementTask : ITask<NumberInput, NumberInput>
    {
        public static string TaskName => "tests.increment";

        public Task<NumberInput> ExecuteAsync(
            NumberInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        ) => Task.FromResult(new NumberInput { Value = input.Value + 1 });
    }

    public sealed class DoubleTask : ITask<NumberInput, NumberInput>
    {
        public static string TaskName => "tests.double";

        public Task<NumberInput> ExecuteAsync(
            NumberInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        ) => Task.FromResult(new NumberInput { Value = input.Value * 2 });
    }

    public sealed class CollectTask : ITask<NumberInput>
    {
        public static string TaskName => "tests.collect";

        public Task ExecuteAsync(
            NumberInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        ) => Task.CompletedTask;
    }
}
