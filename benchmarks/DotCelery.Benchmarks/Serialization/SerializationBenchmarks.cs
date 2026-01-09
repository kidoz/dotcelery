using BenchmarkDotNet.Attributes;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;

namespace DotCelery.Benchmarks.Serialization;

/// <summary>
/// Benchmarks for JSON serialization and deserialization.
/// </summary>
[MemoryDiagnoser]
public class SerializationBenchmarks
{
    private JsonMessageSerializer _serializer = null!;
    private TaskMessage _simpleMessage = null!;
    private TaskMessage _complexMessage = null!;
    private TaskResult _result = null!;
    private byte[] _serializedSimpleMessage = null!;
    private byte[] _serializedComplexMessage = null!;
    private byte[] _serializedResult = null!;

    [GlobalSetup]
    public void Setup()
    {
        _serializer = new JsonMessageSerializer();

        _simpleMessage = new TaskMessage
        {
            Id = Guid.NewGuid().ToString("N"),
            Task = "benchmark.simple",
            Args = new byte[100],
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Queue = "celery",
        };

        _complexMessage = new TaskMessage
        {
            Id = Guid.NewGuid().ToString("N"),
            Task = "benchmark.complex",
            Args = new byte[1000],
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Queue = "celery",
            Eta = DateTimeOffset.UtcNow.AddMinutes(5),
            Expires = DateTimeOffset.UtcNow.AddHours(1),
            Retries = 2,
            MaxRetries = 5,
            ParentId = Guid.NewGuid().ToString("N"),
            RootId = Guid.NewGuid().ToString("N"),
            BatchId = Guid.NewGuid().ToString("N"),
            CorrelationId = Guid.NewGuid().ToString("N"),
            Priority = 5,
            PartitionKey = "partition-1",
            TenantId = "tenant-abc",
            Headers = new Dictionary<string, string>
            {
                ["header1"] = "value1",
                ["header2"] = "value2",
                ["header3"] = "value3",
            },
        };

        _result = new TaskResult
        {
            TaskId = Guid.NewGuid().ToString("N"),
            State = TaskState.Success,
            Result = new byte[500],
            ContentType = "application/json",
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromMilliseconds(150),
            Retries = 1,
            Worker = "worker-1",
        };

        _serializedSimpleMessage = _serializer.Serialize(_simpleMessage);
        _serializedComplexMessage = _serializer.Serialize(_complexMessage);
        _serializedResult = _serializer.Serialize(_result);
    }

    [Benchmark(Baseline = true)]
    public byte[] SerializeSimpleMessage() => _serializer.Serialize(_simpleMessage);

    [Benchmark]
    public byte[] SerializeComplexMessage() => _serializer.Serialize(_complexMessage);

    [Benchmark]
    public byte[] SerializeResult() => _serializer.Serialize(_result);

    [Benchmark]
    public TaskMessage DeserializeSimpleMessage() =>
        _serializer.Deserialize<TaskMessage>(_serializedSimpleMessage);

    [Benchmark]
    public TaskMessage DeserializeComplexMessage() =>
        _serializer.Deserialize<TaskMessage>(_serializedComplexMessage);

    [Benchmark]
    public TaskResult DeserializeResult() => _serializer.Deserialize<TaskResult>(_serializedResult);
}
