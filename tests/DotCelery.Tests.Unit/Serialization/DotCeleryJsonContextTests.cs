using System.Text.Json;
using DotCelery.Core.Batches;
using DotCelery.Core.Canvas;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Models;
using DotCelery.Core.Outbox;
using DotCelery.Core.Progress;
using DotCelery.Core.Sagas;
using DotCelery.Core.Serialization;
using DotCelery.Core.Signals;

namespace DotCelery.Tests.Unit.Serialization;

/// <summary>
/// Tests for the AOT-friendly JSON serialization context.
/// </summary>
public class DotCeleryJsonContextTests
{
    [Fact]
    public void Default_ReturnsNonNullInstance()
    {
        var context = DotCeleryJsonContext.Default;

        Assert.NotNull(context);
    }

    [Fact]
    public void AotOptions_ReturnsNonNullOptions()
    {
        var options = DotCeleryJsonContext.AotOptions;

        Assert.NotNull(options);
    }

    [Fact]
    public void AotOptions_UsesCamelCaseNaming()
    {
        var options = DotCeleryJsonContext.AotOptions;

        Assert.Equal(JsonNamingPolicy.CamelCase, options.PropertyNamingPolicy);
    }

    [Theory]
    [InlineData(typeof(TaskMessage))]
    [InlineData(typeof(TaskResult))]
    [InlineData(typeof(TaskExceptionInfo))]
    [InlineData(typeof(TaskState))]
    [InlineData(typeof(RevokeOptions))]
    public void GetTypeInfo_CoreModels_ReturnsTypeInfo(Type type)
    {
        var typeInfo = DotCeleryJsonContext.Default.GetTypeInfo(type);

        Assert.NotNull(typeInfo);
        Assert.Equal(type, typeInfo.Type);
    }

    [Theory]
    [InlineData(typeof(Signature))]
    [InlineData(typeof(Chain))]
    [InlineData(typeof(Group))]
    [InlineData(typeof(Chord))]
    public void GetTypeInfo_CanvasTypes_ReturnsTypeInfo(Type type)
    {
        var typeInfo = DotCeleryJsonContext.Default.GetTypeInfo(type);

        Assert.NotNull(typeInfo);
        Assert.Equal(type, typeInfo.Type);
    }

    [Theory]
    [InlineData(typeof(DeadLetterMessage))]
    [InlineData(typeof(DeadLetterReason))]
    public void GetTypeInfo_DeadLetterTypes_ReturnsTypeInfo(Type type)
    {
        var typeInfo = DotCeleryJsonContext.Default.GetTypeInfo(type);

        Assert.NotNull(typeInfo);
        Assert.Equal(type, typeInfo.Type);
    }

    [Theory]
    [InlineData(typeof(OutboxMessage))]
    [InlineData(typeof(OutboxMessageStatus))]
    public void GetTypeInfo_OutboxTypes_ReturnsTypeInfo(Type type)
    {
        var typeInfo = DotCeleryJsonContext.Default.GetTypeInfo(type);

        Assert.NotNull(typeInfo);
        Assert.Equal(type, typeInfo.Type);
    }

    [Theory]
    [InlineData(typeof(SignalMessage))]
    [InlineData(typeof(BeforeTaskPublishSignal))]
    [InlineData(typeof(AfterTaskPublishSignal))]
    [InlineData(typeof(TaskPreRunSignal))]
    [InlineData(typeof(TaskPostRunSignal))]
    [InlineData(typeof(TaskSuccessSignal))]
    [InlineData(typeof(TaskFailureSignal))]
    [InlineData(typeof(TaskRetrySignal))]
    [InlineData(typeof(TaskRevokedSignal))]
    [InlineData(typeof(TaskRejectedSignal))]
    public void GetTypeInfo_SignalTypes_ReturnsTypeInfo(Type type)
    {
        var typeInfo = DotCeleryJsonContext.Default.GetTypeInfo(type);

        Assert.NotNull(typeInfo);
        Assert.Equal(type, typeInfo.Type);
    }

    [Theory]
    [InlineData(typeof(Saga))]
    [InlineData(typeof(SagaStep))]
    [InlineData(typeof(SagaState))]
    [InlineData(typeof(SagaStepState))]
    public void GetTypeInfo_SagaTypes_ReturnsTypeInfo(Type type)
    {
        var typeInfo = DotCeleryJsonContext.Default.GetTypeInfo(type);

        Assert.NotNull(typeInfo);
        Assert.Equal(type, typeInfo.Type);
    }

    [Theory]
    [InlineData(typeof(Batch))]
    [InlineData(typeof(BatchState))]
    public void GetTypeInfo_BatchTypes_ReturnsTypeInfo(Type type)
    {
        var typeInfo = DotCeleryJsonContext.Default.GetTypeInfo(type);

        Assert.NotNull(typeInfo);
        Assert.Equal(type, typeInfo.Type);
    }

    [Theory]
    [InlineData(typeof(ProgressInfo))]
    [InlineData(typeof(ProgressUpdatedSignal))]
    public void GetTypeInfo_ProgressTypes_ReturnsTypeInfo(Type type)
    {
        var typeInfo = DotCeleryJsonContext.Default.GetTypeInfo(type);

        Assert.NotNull(typeInfo);
        Assert.Equal(type, typeInfo.Type);
    }

    [Theory]
    [InlineData(typeof(IReadOnlyList<string>))]
    [InlineData(typeof(List<string>))]
    [InlineData(typeof(string[]))]
    [InlineData(typeof(Dictionary<string, string>))]
    public void GetTypeInfo_CollectionTypes_ReturnsTypeInfo(Type type)
    {
        var typeInfo = DotCeleryJsonContext.Default.GetTypeInfo(type);

        Assert.NotNull(typeInfo);
        Assert.Equal(type, typeInfo.Type);
    }

    [Fact]
    public void GetTypeInfo_UnregisteredType_ReturnsNull()
    {
        var typeInfo = DotCeleryJsonContext.Default.GetTypeInfo(typeof(UnregisteredTestType));

        Assert.Null(typeInfo);
    }

    [Fact]
    public void TaskMessage_TypeInfo_CanSerialize()
    {
        var message = new TaskMessage
        {
            Id = "test-id",
            Task = "test.task",
            Args = [],
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Queue = "celery",
        };

        var typeInfo = DotCeleryJsonContext.Default.TaskMessage;
        var json = JsonSerializer.Serialize(message, typeInfo);

        Assert.Contains("test-id", json);
        Assert.Contains("test.task", json);
    }

    [Fact]
    public void TaskMessage_TypeInfo_CanDeserialize()
    {
        var message = new TaskMessage
        {
            Id = "test-id",
            Task = "test.task",
            Args = [],
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Queue = "celery",
        };

        var typeInfo = DotCeleryJsonContext.Default.TaskMessage;
        var json = JsonSerializer.Serialize(message, typeInfo);
        var result = JsonSerializer.Deserialize(json, typeInfo);

        Assert.NotNull(result);
        Assert.Equal("test-id", result.Id);
        Assert.Equal("test.task", result.Task);
    }

    [Fact]
    public void TaskResult_TypeInfo_CanRoundTrip()
    {
        var result = new TaskResult
        {
            TaskId = "task-123",
            State = TaskState.Success,
            Result = [1, 2, 3],
            ContentType = "application/json",
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromSeconds(10),
            Retries = 0,
        };

        var typeInfo = DotCeleryJsonContext.Default.TaskResult;
        var json = JsonSerializer.Serialize(result, typeInfo);
        var deserialized = JsonSerializer.Deserialize(json, typeInfo);

        Assert.NotNull(deserialized);
        Assert.Equal("task-123", deserialized.TaskId);
        Assert.Equal(TaskState.Success, deserialized.State);
    }

    [Fact]
    public void Signature_TypeInfo_CanRoundTrip()
    {
        var signature = new Signature
        {
            TaskName = "test.task",
            Args = [1, 2, 3],
            Queue = "high-priority",
        };

        var typeInfo = DotCeleryJsonContext.Default.Signature;
        var json = JsonSerializer.Serialize(signature, typeInfo);
        var deserialized = JsonSerializer.Deserialize(json, typeInfo);

        Assert.NotNull(deserialized);
        Assert.Equal("test.task", deserialized.TaskName);
        Assert.Equal("high-priority", deserialized.Queue);
    }

    [Fact]
    public void Saga_TypeInfo_CanRoundTrip()
    {
        var saga = new Saga
        {
            Id = "saga-123",
            Name = "test-saga",
            State = SagaState.Executing,
            Steps = [],
            CreatedAt = DateTimeOffset.UtcNow,
        };

        var typeInfo = DotCeleryJsonContext.Default.Saga;
        var json = JsonSerializer.Serialize(saga, typeInfo);
        var deserialized = JsonSerializer.Deserialize(json, typeInfo);

        Assert.NotNull(deserialized);
        Assert.Equal("saga-123", deserialized.Id);
        Assert.Equal(SagaState.Executing, deserialized.State);
    }

    [Fact]
    public void Batch_TypeInfo_CanRoundTrip()
    {
        var batch = new Batch
        {
            Id = "batch-123",
            State = BatchState.Completed,
            TaskIds = ["task-1", "task-2", "task-3"],
            CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-5),
            CompletedAt = DateTimeOffset.UtcNow,
        };

        var typeInfo = DotCeleryJsonContext.Default.Batch;
        var json = JsonSerializer.Serialize(batch, typeInfo);
        var deserialized = JsonSerializer.Deserialize(json, typeInfo);

        Assert.NotNull(deserialized);
        Assert.Equal("batch-123", deserialized.Id);
        Assert.Equal(BatchState.Completed, deserialized.State);
        Assert.Equal(3, deserialized.TotalTasks);
    }

    [Fact]
    public void EnumSerialization_UsesStringConverter()
    {
        var result = new TaskResult
        {
            TaskId = "task-123",
            State = TaskState.Success,
            ContentType = "application/json",
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = TimeSpan.Zero,
            Retries = 0,
        };

        var typeInfo = DotCeleryJsonContext.Default.TaskResult;
        var json = JsonSerializer.Serialize(result, typeInfo);

        // Should serialize enum as string (camelCase)
        Assert.Contains("success", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("\"state\":1", json); // Not numeric
    }

    [Fact]
    public void NullValues_AreOmitted()
    {
        var result = new TaskResult
        {
            TaskId = "task-123",
            State = TaskState.Pending,
            ContentType = "application/json",
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = TimeSpan.Zero,
            Retries = 0,
            Exception = null, // Should be omitted
            Worker = null, // Should be omitted
        };

        var typeInfo = DotCeleryJsonContext.Default.TaskResult;
        var json = JsonSerializer.Serialize(result, typeInfo);

        Assert.DoesNotContain("exception", json);
        Assert.DoesNotContain("worker", json);
    }

    private sealed class UnregisteredTestType
    {
        public string? Name { get; set; }
    }
}
