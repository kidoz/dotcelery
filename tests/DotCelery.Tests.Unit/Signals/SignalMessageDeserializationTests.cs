using System.Text.Json;
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
        var roundtripped = message.Deserialize();

        Assert.IsType<TaskSuccessSignal>(roundtripped);
        Assert.Equal(success.TaskId, roundtripped.TaskId);
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

    private abstract class AbstractSignal : ITaskSignal
    {
        public string TaskId { get; init; } = string.Empty;
        public string TaskName { get; init; } = string.Empty;
        public DateTimeOffset Timestamp { get; init; }
    }
}
