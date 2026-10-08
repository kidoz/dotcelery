using System.Collections.Concurrent;
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
        services.AddTransient<FailingTask>();
        services.AddTransient<ErrorCollectTask>();
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
        registry.Register(typeof(FailingTask), FailingTask.TaskName);
        registry.Register(typeof(ErrorCollectTask), ErrorCollectTask.TaskName);

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

    [Fact]
    public async Task Link_RunsWithTheTaskResultWhenTheTaskSucceeds()
    {
        CollectTask.Received.Clear();
        var client = CreateClient();
        var signature = new Signature<IncrementTask, NumberInput, NumberInput>
        {
            Input = new NumberInput { Value = 1 },
            // The task's result is passed to the callback, so this input is not used
            Link = new Signature<CollectTask, NumberInput>
            {
                Input = new NumberInput { Value = 42 },
            },
        };

        var group = await client.SendGroupAsync(new Group(signature));
        await _worker.StartAsync(CancellationToken.None);

        var taskResult = await WaitForResultAsync(group.TaskIds[0]);
        Assert.Equal(TaskState.Success, taskResult.State);

        var linkResult = await WaitForResultAsync($"{group.TaskIds[0]}:link");
        Assert.Equal(TaskState.Success, linkResult.State);
        Assert.Equal(2, Assert.Single(CollectTask.Received));
    }

    [Fact]
    public async Task LinkError_RunsWithTheFailureWhenTheTaskFails()
    {
        ErrorCollectTask.Received.Clear();
        var client = CreateClient();
        var signature = new Signature<FailingTask, NumberInput>
        {
            Input = new NumberInput { Value = 1 },
            Link = new Signature<CollectTask, NumberInput>(),
            LinkError = new Signature<ErrorCollectTask, TaskErrorInfo>(),
        };

        var group = await client.SendGroupAsync(new Group(signature));
        await _worker.StartAsync(CancellationToken.None);

        var taskResult = await WaitForResultAsync(group.TaskIds[0]);
        Assert.Equal(TaskState.Failure, taskResult.State);

        var linkResult = await WaitForResultAsync($"{group.TaskIds[0]}:link-error");
        Assert.Equal(TaskState.Success, linkResult.State);

        var error = Assert.Single(ErrorCollectTask.Received);
        Assert.Equal(group.TaskIds[0], error.TaskId);
        Assert.Equal(FailingTask.TaskName, error.TaskName);
        Assert.Equal("boom", error.ErrorMessage);

        // The success callback of a failed task is not run
        Assert.Null(await _backend.GetResultAsync($"{group.TaskIds[0]}:link"));
    }

    [Fact]
    public async Task Chain_CarriesEachStepsLinkToTheNextStep()
    {
        CollectTask.Received.Clear();
        var client = CreateClient();
        var chain = new Chain(
            new Signature<IncrementTask, NumberInput, NumberInput>
            {
                Input = new NumberInput { Value = 1 },
            },
            new Signature<DoubleTask, NumberInput, NumberInput>
            {
                Link = new Signature<CollectTask, NumberInput>(),
            }
        );

        var result = await client.SendChainAsync(chain);
        await _worker.StartAsync(CancellationToken.None);

        var last = await WaitForResultAsync(result.LastTaskId);
        Assert.Equal(TaskState.Success, last.State);

        var linkResult = await WaitForResultAsync($"{result.LastTaskId}:link");
        Assert.Equal(TaskState.Success, linkResult.State);

        // 1 -> increment 2 -> double 4, and the second step's link ran with its result
        Assert.Equal(4, Assert.Single(CollectTask.Received));
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

        public static ConcurrentQueue<int> Received { get; } = new();

        public Task ExecuteAsync(
            NumberInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            Received.Enqueue(input.Value);
            return Task.CompletedTask;
        }
    }

    public sealed class FailingTask : ITask<NumberInput>
    {
        public static string TaskName => "tests.failing";

        public Task ExecuteAsync(
            NumberInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        ) => throw new InvalidOperationException("boom");
    }

    public sealed class ErrorCollectTask : ITask<TaskErrorInfo>
    {
        public static string TaskName => "tests.error-collect";

        public static ConcurrentQueue<TaskErrorInfo> Received { get; } = new();

        public Task ExecuteAsync(
            TaskErrorInfo input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            Received.Enqueue(input);
            return Task.CompletedTask;
        }
    }
}
