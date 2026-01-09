using BenchmarkDotNet.Attributes;
using DotCelery.Broker.InMemory;
using DotCelery.Core.Models;
using Microsoft.Extensions.Options;

namespace DotCelery.Benchmarks.Broker;

/// <summary>
/// Benchmarks for in-memory broker publish operations.
/// </summary>
[MemoryDiagnoser]
public class InMemoryBrokerBenchmarks
{
    private InMemoryBroker _broker = null!;
    private TaskMessage _message = null!;

    [GlobalSetup]
    public void Setup()
    {
        _broker = new InMemoryBroker(
            Options.Create(new InMemoryBrokerOptions { MaxQueueCapacity = null })
        );

        _message = new TaskMessage
        {
            Id = Guid.NewGuid().ToString("N"),
            Task = "benchmark.task",
            Args = new byte[100],
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Queue = "benchmark-queue",
        };
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _broker.DisposeAsync();
    }

    [Benchmark(Baseline = true)]
    public ValueTask PublishSingle() => _broker.PublishAsync(_message);

    [Benchmark]
    [Arguments(10)]
    [Arguments(100)]
    [Arguments(1000)]
    public async Task PublishBatch(int count)
    {
        for (var i = 0; i < count; i++)
        {
            await _broker.PublishAsync(_message);
        }
    }

    [IterationSetup(Target = nameof(PublishToFreshQueue))]
    public void IterationSetupFreshQueue()
    {
        _broker.PurgeQueue("fresh-queue");
    }

    [Benchmark]
    public async Task PublishToFreshQueue()
    {
        var message = _message with { Queue = "fresh-queue" };
        for (var i = 0; i < 100; i++)
        {
            await _broker.PublishAsync(message);
        }
    }
}

/// <summary>
/// Benchmarks for broker publish and consume round-trip.
/// </summary>
[MemoryDiagnoser]
public class BrokerRoundTripBenchmarks
{
    private InMemoryBroker _broker = null!;
    private TaskMessage _message = null!;

    [GlobalSetup]
    public void Setup()
    {
        _broker = new InMemoryBroker(
            Options.Create(new InMemoryBrokerOptions { MaxQueueCapacity = null })
        );

        _message = new TaskMessage
        {
            Id = Guid.NewGuid().ToString("N"),
            Task = "benchmark.task",
            Args = new byte[100],
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Queue = "roundtrip-queue",
        };
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _broker.DisposeAsync();
    }

    [Benchmark]
    public async Task PublishAndConsumeSingle()
    {
        await _broker.PublishAsync(_message);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        await foreach (var msg in _broker.ConsumeAsync(["roundtrip-queue"], cts.Token))
        {
            await _broker.AckAsync(msg);
            break;
        }
    }

    [Benchmark]
    [Arguments(10)]
    [Arguments(100)]
    public async Task PublishAndConsumeBatch(int count)
    {
        for (var i = 0; i < count; i++)
        {
            await _broker.PublishAsync(_message);
        }

        var consumed = 0;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await foreach (var msg in _broker.ConsumeAsync(["roundtrip-queue"], cts.Token))
        {
            await _broker.AckAsync(msg);
            consumed++;
            if (consumed >= count)
            {
                break;
            }
        }
    }
}
