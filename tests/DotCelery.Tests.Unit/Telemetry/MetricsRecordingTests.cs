using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using DotCelery.Backend.InMemory.Storage;
using DotCelery.Broker.InMemory;
using DotCelery.Client;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Instrumentation;
using DotCelery.Core.Models;
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

namespace DotCelery.Tests.Unit.Telemetry;

/// <summary>
/// Sends and runs a task while listening to the <c>DotCelery</c> meter, so the metrics are
/// proven recorded by the components and not only defined.
/// </summary>
public sealed class MetricsRecordingTests : IAsyncDisposable
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(15);

    private readonly InMemoryBroker _broker = new();
    private readonly InMemoryStorageProvider _storage = new();
    private readonly JsonMessageSerializer _serializer = new();
    private readonly ServiceProvider _services;
    private readonly ResultBackend _backend;
    private readonly CeleryClient _client;
    private readonly CeleryWorkerService _worker;

    public MetricsRecordingTests()
    {
        _backend = new ResultBackend(_storage);

        var services = new ServiceCollection();
        services.AddTransient<EchoTask>();
        services.AddTransient<FailingTask>();
        _services = services.BuildServiceProvider();

        _client = new CeleryClient(
            _broker,
            _backend,
            _serializer,
            Options.Create(new CeleryClientOptions()),
            NullLogger<CeleryClient>.Instance
        );

        var workerOptions = Options.Create(
            new WorkerOptions
            {
                Concurrency = 1,
                PrefetchCount = 1,
                EnableRevocation = false,
                EnableRateLimiting = false,
                ShutdownTimeout = WaitTimeout,
            }
        );
        var registry = new TaskRegistry();
        registry.Register(typeof(EchoTask), EchoTask.TaskName);
        registry.Register(typeof(FailingTask), FailingTask.TaskName);

        var executor = new TaskExecutor(
            registry,
            _services,
            _serializer,
            _backend,
            new RevocationManager(workerOptions, NullLogger<RevocationManager>.Instance),
            new TaskFilterPipeline(
                _services,
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
            NullLogger<CeleryWorkerService>.Instance
        );
    }

    [Fact]
    public async Task SendingAndRunningATask_RecordsItsMetrics()
    {
        // The meter is shared by every test in the process, so only this task's measurements count
        using var metrics = new MetricsCollector(EchoTask.TaskName);

        var result = await _client.SendAsync<EchoTask, TestInput, TestInput>(
            new TestInput { Value = 1 }
        );

        await _worker.StartAsync(CancellationToken.None);
        await _client.WaitForResultAsync(result.TaskId, WaitTimeout);
        await _worker.StopAsync(CancellationToken.None);

        Assert.Equal(1, metrics.Sum("dotcelery.tasks.sent"));
        Assert.Equal(1, metrics.Sum("dotcelery.tasks.received"));
        Assert.Equal(1, metrics.Sum("dotcelery.tasks.succeeded"));
        Assert.Equal(0, metrics.Sum("dotcelery.tasks.failed"));
        Assert.Equal(1, metrics.Count("dotcelery.tasks.duration"));
        Assert.Equal(1, metrics.Count("dotcelery.tasks.queue_time"));

        // The task went in progress and back out again: the gauge rose to one and now reads zero
        Assert.Equal(2, metrics.Count("dotcelery.tasks.in_progress"));
        Assert.Equal(0, metrics.Sum("dotcelery.tasks.in_progress"));
    }

    [Fact]
    public async Task AFailingTask_IsRecordedAsFailed()
    {
        using var metrics = new MetricsCollector(FailingTask.TaskName);

        await _client.SendAsync<FailingTask, TestInput>(new TestInput { Value = 1 });

        await _worker.StartAsync(CancellationToken.None);
        await WaitForAsync(() => metrics.Sum("dotcelery.tasks.failed") >= 1);
        await _worker.StopAsync(CancellationToken.None);

        Assert.Equal(1, metrics.Sum("dotcelery.tasks.received"));
        Assert.Equal(1, metrics.Sum("dotcelery.tasks.failed"));
        Assert.Equal(0, metrics.Sum("dotcelery.tasks.succeeded"));
        Assert.Equal(1, metrics.Count("dotcelery.tasks.duration"));
    }

    private static async Task WaitForAsync(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(WaitTimeout);

        while (!condition())
        {
            await Task.Delay(20, timeout.Token);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _worker.StopAsync(CancellationToken.None);
        _worker.Dispose();
        await _services.DisposeAsync();
        await _backend.DisposeAsync();
        await _broker.DisposeAsync();
    }

    public sealed class TestInput
    {
        public int Value { get; init; }
    }

    public sealed class FailingTask : ITask<TestInput>
    {
        public static string TaskName => "tests.metrics.failing";

        public Task ExecuteAsync(
            TestInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        ) => throw new InvalidOperationException("boom");
    }

    public sealed class EchoTask : ITask<TestInput, TestInput>
    {
        public static string TaskName => "tests.metrics";

        public Task<TestInput> ExecuteAsync(
            TestInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        ) => Task.FromResult(input);
    }

    private sealed class MetricsCollector : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly ConcurrentDictionary<string, double> _sums = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, long> _counts = new(StringComparer.Ordinal);
        private readonly HashSet<string> _taskNames;

        public MetricsCollector(params string[] taskNames)
        {
            _taskNames = new HashSet<string>(taskNames, StringComparer.Ordinal);

            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == DotCeleryMetrics.Meter.Name)
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>(OnMeasurement);
            _listener.SetMeasurementEventCallback<double>(OnMeasurement);
            _listener.Start();
        }

        public double Sum(string instrument) =>
            _sums.TryGetValue(instrument, out var sum) ? sum : 0;

        public long Count(string instrument) =>
            _counts.TryGetValue(instrument, out var count) ? count : 0;

        public void Dispose() => _listener.Dispose();

        private void OnMeasurement<T>(
            Instrument instrument,
            T measurement,
            ReadOnlySpan<KeyValuePair<string, object?>> tags,
            object? state
        )
            where T : struct
        {
            var taskName =
                tags.ToArray().FirstOrDefault(tag => tag.Key == "task.name").Value as string;

            if (taskName is null || !_taskNames.Contains(taskName))
            {
                return;
            }

            _sums.AddOrUpdate(
                instrument.Name,
                Convert.ToDouble(measurement, System.Globalization.CultureInfo.InvariantCulture),
                (_, sum) =>
                    sum
                    + Convert.ToDouble(
                        measurement,
                        System.Globalization.CultureInfo.InvariantCulture
                    )
            );
            _counts.AddOrUpdate(instrument.Name, 1, (_, count) => count + 1);
        }
    }
}
