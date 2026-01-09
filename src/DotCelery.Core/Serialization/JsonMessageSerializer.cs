using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Core.Abstractions;

namespace DotCelery.Core.Serialization;

/// <summary>
/// JSON serializer using System.Text.Json with AOT support.
/// </summary>
public sealed class JsonMessageSerializer : IMessageSerializer
{
    private static readonly JsonSerializerOptions FallbackOptions = CreateDefaultOptions();
    private readonly JsonSerializerOptions _options;

    /// <summary>
    /// Initializes a new instance of the <see cref="JsonMessageSerializer"/> class
    /// using the AOT-friendly default options.
    /// </summary>
    public JsonMessageSerializer()
        : this(null) { }

    /// <summary>
    /// Initializes a new instance of the <see cref="JsonMessageSerializer"/> class.
    /// </summary>
    /// <param name="options">Optional JSON serializer options. If null, uses combined AOT + reflection options.</param>
    public JsonMessageSerializer(JsonSerializerOptions? options)
    {
        _options = options ?? CreateCombinedOptions();
    }

    /// <inheritdoc />
    public string ContentType => "application/json";

    /// <inheritdoc />
    public byte[] Serialize<T>(T value)
    {
        // For polymorphic serialization (runtime type != declared type), use fallback
        // This handles cases like Serialize<object>(someSpecificType)
        if (value is not null && typeof(T) != value.GetType())
        {
            return JsonSerializer.SerializeToUtf8Bytes(value, FallbackOptions);
        }

        // Try to use AOT-generated type info if available
        var typeInfo = TryGetTypeInfo<T>();
        return typeInfo is not null
            ? JsonSerializer.SerializeToUtf8Bytes(value, typeInfo)
            : JsonSerializer.SerializeToUtf8Bytes(value, FallbackOptions);
    }

    /// <inheritdoc />
    public T Deserialize<T>(ReadOnlySpan<byte> data)
    {
        // Try to use AOT-generated type info if available
        var typeInfo = TryGetTypeInfo<T>();
        var result = typeInfo is not null
            ? JsonSerializer.Deserialize(data, typeInfo)
            : JsonSerializer.Deserialize<T>(data, FallbackOptions);

        return result
            ?? throw new InvalidOperationException($"Failed to deserialize to {typeof(T).Name}");
    }

    /// <inheritdoc />
    public object Deserialize(ReadOnlySpan<byte> data, Type type)
    {
        ArgumentNullException.ThrowIfNull(type);

        // Try to use AOT-generated type info if available
        var typeInfo = DotCeleryJsonContext.Default.GetTypeInfo(type);
        var result = typeInfo is not null
            ? JsonSerializer.Deserialize(data, typeInfo)
            : JsonSerializer.Deserialize(data, type, FallbackOptions);

        return result
            ?? throw new InvalidOperationException($"Failed to deserialize to {type.Name}");
    }

    private static JsonTypeInfo<T>? TryGetTypeInfo<T>()
    {
        try
        {
            return (JsonTypeInfo<T>?)DotCeleryJsonContext.Default.GetTypeInfo(typeof(T));
        }
        catch
        {
            // Type not registered in context, fall back to reflection-based serialization
            return null;
        }
    }

    /// <summary>
    /// Creates the default JSON serializer options (non-AOT compatible, for backwards compatibility).
    /// </summary>
    /// <returns>The default JSON serializer options.</returns>
    public static JsonSerializerOptions CreateDefaultOptions()
    {
        return new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            WriteIndented = false,
            Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) },
        };
    }

    /// <summary>
    /// Creates combined options that first try AOT context, then fall back to reflection.
    /// </summary>
    /// <returns>Combined JSON serializer options.</returns>
    public static JsonSerializerOptions CreateCombinedOptions()
    {
        return new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            WriteIndented = false,
            Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) },
            TypeInfoResolverChain =
            {
                DotCeleryJsonContext.Default,
                new DefaultJsonTypeInfoResolver(),
            },
        };
    }
}
