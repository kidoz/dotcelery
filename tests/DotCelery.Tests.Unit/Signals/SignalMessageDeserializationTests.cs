using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Core.Signals;

namespace DotCelery.Tests.Unit.Signals;

/// <summary>
/// Tests that <see cref="SignalMessage.Deserialize"/> rejects untrusted types so
/// a poisoned signal queue cannot trigger gadget deserialization.
/// </summary>
public sealed class SignalMessageDeserializationTests
{
    [Fact]
    public void Deserialize_RejectsTypeThatIsNotITaskSignal()
    {
        var poisoned = new SignalMessage
        {
            Id = "msg-1",
            SignalType = typeof(System.IO.FileInfo).AssemblyQualifiedName!,
            TaskId = "task-1",
            TaskName = "evil.task",
            Payload = JsonSerializer.Serialize(new { Path = "/etc/passwd" }),
            CreatedAt = DateTimeOffset.UtcNow,
        };

        var ex = Assert.Throws<InvalidOperationException>(() => poisoned.Deserialize());
        Assert.Contains("Refusing to deserialize", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Deserialize_RejectsAbstractTypeEvenIfAssignable()
    {
        var poisoned = new SignalMessage
        {
            Id = "msg-1",
            SignalType = typeof(AbstractSignal).AssemblyQualifiedName!,
            TaskId = "task-1",
            TaskName = "task",
            Payload = "{}",
            CreatedAt = DateTimeOffset.UtcNow,
        };

        Assert.Throws<InvalidOperationException>(() => poisoned.Deserialize());
    }

    [Fact]
    public void Deserialize_AcceptsKnownTaskSignal()
    {
        var success = new TaskSuccessSignal
        {
            TaskId = "task-1",
            TaskName = "test.task",
            Timestamp = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromSeconds(2),
        };

        var message = SignalMessage.Create(success);
        Assert.Equal("task-success", message.SignalType);
        var roundtripped = message.Deserialize();

        Assert.IsType<TaskSuccessSignal>(roundtripped);
        Assert.Equal(success.TaskId, roundtripped.TaskId);
        var legacy = message with { SignalType = typeof(TaskSuccessSignal).AssemblyQualifiedName! };
        Assert.Equal(success.TaskId, legacy.Deserialize().TaskId);
    }

    [Fact]
    public void Deserialize_UnresolvableTypeName_ThrowsInvalidOperation()
    {
        var message = new SignalMessage
        {
            Id = "msg-1",
            SignalType = "System.DoesNotExist, NoSuchAssembly",
            TaskId = "task-1",
            TaskName = "task",
            Payload = "{}",
            CreatedAt = DateTimeOffset.UtcNow,
        };

        var ex = Assert.Throws<InvalidOperationException>(() => message.Deserialize());
        Assert.Contains("Cannot resolve signal type", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RegisteredCustomSignal_UsesItsExplicitMetadataAndStableName()
    {
        var registry = new SignalTypeRegistry();
        var options = new JsonSerializerOptions
        {
            TypeInfoResolver = new DefaultJsonTypeInfoResolver(),
        };
        registry.Register("custom", options.GetTypeInfo(typeof(CustomSignal)));
        var signal = new CustomSignal("1", "task", DateTimeOffset.UtcNow, "value");
        var envelope = registry.Create(signal);
        Assert.Equal("custom", envelope.SignalType);
        Assert.Equal(signal, registry.Deserialize(envelope));
        Assert.Throws<InvalidOperationException>(() => envelope.Deserialize());
    }

    public sealed record CustomSignal(
        string TaskId,
        string TaskName,
        DateTimeOffset Timestamp,
        string Value
    ) : ITaskSignal;

    private abstract class AbstractSignal : ITaskSignal
    {
        public string TaskId { get; init; } = string.Empty;
        public string TaskName { get; init; } = string.Empty;
        public DateTimeOffset Timestamp { get; init; }
    }
}
