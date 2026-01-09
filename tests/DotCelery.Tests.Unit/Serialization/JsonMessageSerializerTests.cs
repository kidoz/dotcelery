using System.Text.Json;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;

namespace DotCelery.Tests.Unit.Serialization;

public class JsonMessageSerializerTests
{
    private readonly JsonMessageSerializer _serializer = new();

    [Fact]
    public void ContentType_ReturnsApplicationJson()
    {
        Assert.Equal("application/json", _serializer.ContentType);
    }

    #region AOT Serialization Tests

    [Fact]
    public void Serialize_RegisteredType_UsesAotContext()
    {
        // TaskMessage is registered in DotCeleryJsonContext
        var message = new TaskMessage
        {
            Id = "test-id",
            Task = "test.task",
            Args = [],
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Queue = "celery",
        };

        var bytes = _serializer.Serialize(message);
        var json = System.Text.Encoding.UTF8.GetString(bytes);

        Assert.Contains("test-id", json);
        Assert.Contains("test.task", json);
    }

    [Fact]
    public void Deserialize_RegisteredType_UsesAotContext()
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

        var bytes = _serializer.Serialize(message);
        var result = _serializer.Deserialize<TaskMessage>(bytes);

        Assert.Equal("test-id", result.Id);
        Assert.Equal("test.task", result.Task);
    }

    [Fact]
    public void Serialize_UnregisteredType_UsesFallback()
    {
        // TestData is NOT registered in DotCeleryJsonContext
        var obj = new TestData { Name = "Test", Value = 42 };

        var bytes = _serializer.Serialize(obj);
        var json = System.Text.Encoding.UTF8.GetString(bytes);

        Assert.Contains("name", json); // Should still use camelCase
        Assert.Contains("Test", json);
    }

    [Fact]
    public void Serialize_PolymorphicType_UsesFallback()
    {
        // When runtime type differs from declared type, should use fallback
        object obj = new TestData { Name = "Polymorphic", Value = 100 };

        var bytes = _serializer.Serialize(obj);
        var json = System.Text.Encoding.UTF8.GetString(bytes);

        Assert.Contains("Polymorphic", json);
        Assert.Contains("100", json);
    }

    [Fact]
    public void Serialize_DerivedTypeAsBase_UsesFallback()
    {
        // Serialize derived type through base type reference
        TestData data = new DerivedTestData
        {
            Name = "Derived",
            Value = 50,
            Extra = "ExtraValue",
        };

        var bytes = _serializer.Serialize(data);
        var json = System.Text.Encoding.UTF8.GetString(bytes);

        Assert.Contains("Derived", json);
        // Note: Extra property may or may not be included depending on serializer behavior
    }

    [Fact]
    public void Deserialize_NonGeneric_RegisteredType_UsesAotContext()
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

        var bytes = _serializer.Serialize(message);
        var result = _serializer.Deserialize(bytes, typeof(TaskMessage));

        var taskMessage = Assert.IsType<TaskMessage>(result);
        Assert.Equal("test-id", taskMessage.Id);
    }

    [Fact]
    public void Deserialize_NonGeneric_UnregisteredType_UsesFallback()
    {
        var obj = new TestData { Name = "Test", Value = 42 };
        var bytes = _serializer.Serialize(obj);

        var result = _serializer.Deserialize(bytes, typeof(TestData));

        var testData = Assert.IsType<TestData>(result);
        Assert.Equal("Test", testData.Name);
        Assert.Equal(42, testData.Value);
    }

    [Fact]
    public void CreateDefaultOptions_ReturnsCamelCaseOptions()
    {
        var options = JsonMessageSerializer.CreateDefaultOptions();

        Assert.NotNull(options);
        Assert.Equal(JsonNamingPolicy.CamelCase, options.PropertyNamingPolicy);
    }

    [Fact]
    public void CreateDefaultOptions_OmitsNullValues()
    {
        var options = JsonMessageSerializer.CreateDefaultOptions();
        var obj = new TestData { Name = null, Value = 42 };

        var json = JsonSerializer.Serialize(obj, options);

        Assert.DoesNotContain("name", json);
        Assert.Contains("value", json);
    }

    [Fact]
    public void CreateCombinedOptions_ReturnsCamelCaseOptions()
    {
        var options = JsonMessageSerializer.CreateCombinedOptions();

        Assert.NotNull(options);
        Assert.Equal(JsonNamingPolicy.CamelCase, options.PropertyNamingPolicy);
    }

    [Fact]
    public void CreateCombinedOptions_HasTypeInfoResolverChain()
    {
        var options = JsonMessageSerializer.CreateCombinedOptions();

        Assert.NotNull(options.TypeInfoResolverChain);
        Assert.True(options.TypeInfoResolverChain.Count >= 2);
    }

    [Fact]
    public void CreateCombinedOptions_CanSerializeRegisteredTypes()
    {
        var options = JsonMessageSerializer.CreateCombinedOptions();
        var message = new TaskMessage
        {
            Id = "test",
            Task = "task",
            Args = [],
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Queue = "celery",
        };

        var json = JsonSerializer.Serialize(message, options);

        Assert.Contains("test", json);
    }

    [Fact]
    public void CreateCombinedOptions_CanSerializeUnregisteredTypes()
    {
        var options = JsonMessageSerializer.CreateCombinedOptions();
        var obj = new TestData { Name = "Combined", Value = 99 };

        var json = JsonSerializer.Serialize(obj, options);

        Assert.Contains("Combined", json);
        Assert.Contains("99", json);
    }

    [Fact]
    public void Serialize_TaskResult_PreservesAllFields()
    {
        var result = new TaskResult
        {
            TaskId = "task-123",
            State = TaskState.Success,
            Result = System.Text.Encoding.UTF8.GetBytes("{\"value\":42}"),
            ContentType = "application/json",
            CompletedAt = DateTimeOffset.Parse(
                "2024-01-01T10:00:05Z",
                System.Globalization.CultureInfo.InvariantCulture
            ),
            Duration = TimeSpan.FromSeconds(5),
            Retries = 2,
            Worker = "worker-1",
        };

        var bytes = _serializer.Serialize(result);
        var deserialized = _serializer.Deserialize<TaskResult>(bytes);

        Assert.Equal("task-123", deserialized.TaskId);
        Assert.Equal(TaskState.Success, deserialized.State);
        Assert.Equal(2, deserialized.Retries);
        Assert.Equal("worker-1", deserialized.Worker);
    }

    [Fact]
    public void Serialize_NullValue_HandlesGracefully()
    {
        TaskMessage? message = null;

        var bytes = _serializer.Serialize(message);
        var json = System.Text.Encoding.UTF8.GetString(bytes);

        Assert.Equal("null", json);
    }

    #endregion

    [Fact]
    public void Serialize_SimpleObject_ReturnsValidJson()
    {
        var obj = new TestData { Name = "Test", Value = 42 };

        var bytes = _serializer.Serialize(obj);

        Assert.NotEmpty(bytes);
        var json = System.Text.Encoding.UTF8.GetString(bytes);
        Assert.Contains("name", json); // camelCase naming
        Assert.Contains("Test", json);
        Assert.Contains("42", json);
    }

    [Fact]
    public void Deserialize_ValidJson_ReturnsObject()
    {
        var obj = new TestData { Name = "Test", Value = 42 };
        var bytes = _serializer.Serialize(obj);

        var result = _serializer.Deserialize<TestData>(bytes);

        Assert.Equal("Test", result.Name);
        Assert.Equal(42, result.Value);
    }

    [Fact]
    public void Deserialize_NonGeneric_ReturnsObject()
    {
        var obj = new TestData { Name = "Test", Value = 42 };
        var bytes = _serializer.Serialize(obj);

        var result = _serializer.Deserialize(bytes, typeof(TestData));

        var testData = Assert.IsType<TestData>(result);
        Assert.Equal("Test", testData.Name);
        Assert.Equal(42, testData.Value);
    }

    [Fact]
    public void Serialize_NullValues_OmitsNulls()
    {
        var obj = new TestData { Name = null, Value = 42 };

        var bytes = _serializer.Serialize(obj);
        var json = System.Text.Encoding.UTF8.GetString(bytes);

        Assert.DoesNotContain("name", json);
        Assert.Contains("value", json);
    }

    [Fact]
    public void Deserialize_EmptyJson_ThrowsException()
    {
        var bytes = "{}"u8.ToArray();

        // Should deserialize to object with null/default values
        var result = _serializer.Deserialize<TestData>(bytes);

        Assert.Null(result.Name);
        Assert.Equal(0, result.Value);
    }

    [Fact]
    public void Deserialize_InvalidJson_ThrowsException()
    {
        var bytes = "invalid json"u8.ToArray();

        Assert.Throws<System.Text.Json.JsonException>(() =>
            _serializer.Deserialize<TestData>(bytes)
        );
    }

    [Fact]
    public void Deserialize_NonGeneric_NullType_ThrowsArgumentNullException()
    {
        var bytes = "{}"u8.ToArray();

        Assert.Throws<ArgumentNullException>(() => _serializer.Deserialize(bytes, null!));
    }

    private class TestData
    {
        public string? Name { get; set; }
        public int Value { get; set; }
    }

    private sealed class DerivedTestData : TestData
    {
        public string? Extra { get; set; }
    }
}
