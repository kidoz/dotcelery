using DotCelery.Backend.InMemory.Storage;
using DotCelery.Broker.InMemory;
using DotCelery.Client;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Filters;
using DotCelery.Worker.Registry;
using DotCelery.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Unit.Worker;

/// <summary>
/// Runs published tasks through a worker, so the queue metrics are proven by the counts they
/// report: a published message waits in its queue, and running it clears the waiting count.
/// </summary>
public sealed class QueueMetricsFilterTests : IAsyncDisposable
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(15);

    private readonly InMemoryStorageProvider _storage;
    private readonly QueueMetrics _metrics;
    private readonly InMemoryBroker _broker;
    private readonly JsonMessageSerializer _serializer = new();
    private readonly ResultBackend _backend;
    private readonly ServiceProvider _services;
    private readonly CeleryWorkerService _worker;
    private readonly CeleryClient _client;

    public QueueMetricsFilterTests()
    {
        _storage = new InMemoryStorageProvider();
        _metrics = new QueueMetrics(_storage);

        // The broker records what it publishes, as the bundled brokers do
        _broker = new InMemoryBroker(
            Options.Create(new InMemoryBrokerOptions()),
            queueMetrics: _metrics
        );
        _backend = new ResultBackend(_storage);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddTransient<CountingTask>();
        services.AddSingleton<IQueueMetrics>(_metrics);
        services.AddTransient<QueueMetricsFilter>();
        _services = services.BuildServiceProvider();

        var options = Options.Create(
            new WorkerOptions
            {
                Concurrency = 1,
                PrefetchCount = 1,
                EnableRevocation = false,
                EnableRateLimiting = false,
                ShutdownTimeout = WaitTimeout,
            }
        );

        var pipeline = new TaskFilterPipeline(
            _services,
            Options.Create(new TaskFilterOptions()),
            NullLogger<TaskFilterPipeline>.Instance
        );
        pipeline.AddGlobalFilter(typeof(QueueMetricsFilter));

        var registry = new TaskRegistry();
        registry.Register(typeof(CountingTask), CountingTask.TaskName);

        var executor = new TaskExecutor(
            registry,
            _services,
            _serializer,
            _backend,
            new RevocationManager(options, NullLogger<RevocationManager>.Instance),
            pipeline,
            options,
            NullLogger<TaskExecutor>.Instance
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

        _client = new CeleryClient(
            _broker,
            _backend,
            _serializer,
            Options.Create(new CeleryClientOptions()),
            NullLogger<CeleryClient>.Instance
        );
    }

    [Fact]
    public async Task PublishedTasks_WaitInTheirQueueAndAreProcessedFromIt()
    {
        CountingTask.Reset();

        for (var index = 0; index < 3; index++)
        {
            await _client.SendAsync<CountingTask, NumberInput>(new NumberInput { Value = index });
        }

        // Nothing consumes the queue yet, so all three messages are waiting
        Assert.Equal(3, await _metrics.GetWaitingCountAsync("celery"));
        Assert.Equal(0, await _metrics.GetRunningCountAsync("celery"));
        Assert.Equal(["celery"], await _metrics.GetQueuesAsync());

        await _worker.StartAsync(CancellationToken.None);
        await WaitForProcessedAsync(3);

        // Every message left the queue, none of them is running, and all three were processed
        Assert.Equal(0, await _metrics.GetWaitingCountAsync("celery"));
        Assert.Equal(0, await _metrics.GetRunningCountAsync("celery"));
        Assert.Equal(3, await _metrics.GetProcessedCountAsync("celery"));
        Assert.Equal([0, 1, 2], CountingTask.Values);
    }

    [Fact]
    public async Task ATaskSentWhileTheWorkerRuns_PassesThroughTheQueue()
    {
        CountingTask.Reset();
        await _worker.StartAsync(CancellationToken.None);

        await _client.SendAsync<CountingTask, NumberInput>(new NumberInput { Value = 7 });
        await WaitForProcessedAsync(1);

        Assert.Equal(0, await _metrics.GetWaitingCountAsync("celery"));
        Assert.Equal(0, await _metrics.GetRunningCountAsync("celery"));
        Assert.Equal(7, Assert.Single(CountingTask.Values));
    }

    public async ValueTask DisposeAsync()
    {
        await _worker.StopAsync(CancellationToken.None);
        _worker.Dispose();
        await _services.DisposeAsync();
        await _backend.DisposeAsync();
        await _broker.DisposeAsync();
    }

    private async Task WaitForProcessedAsync(long count)
    {
        using var timeout = new CancellationTokenSource(WaitTimeout);

        while (await _metrics.GetProcessedCountAsync("celery") < count)
        {
            timeout.Token.ThrowIfCancellationRequested();
            await Task.Delay(20, timeout.Token);
        }
    }

    public sealed class NumberInput
    {
        public int Value { get; init; }
    }

    public sealed class CountingTask : ITask<NumberInput>
    {
        private static readonly List<int> Received = [];

        public static string TaskName => "tests.counting";

        public static IReadOnlyList<int> Values
        {
            get
            {
                lock (Received)
                {
                    return [.. Received];
                }
            }
        }

        public static void Reset()
        {
            lock (Received)
            {
                Received.Clear();
            }
        }

        public Task ExecuteAsync(
            NumberInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            lock (Received)
            {
                Received.Add(input.Value);
            }

            return Task.CompletedTask;
        }
    }
}
