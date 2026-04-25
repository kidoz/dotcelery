using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Core.Abstractions;

namespace DotCelery.Core.Serialization;

/// <summary>
/// Configuration for <see cref="JsonMessageSerializer"/>.
/// </summary>
public sealed class JsonMessageSerializerOptions
{
    /// <summary>
    /// Gets the JSON serializer options. When null, DotCelery uses the default
    /// AOT-friendly options with reflection fallback.
    /// </summary>
    public JsonSerializerOptions? SerializerOptions { get; init; }

    /// <summary>
    /// Gets whether deserialization is restricted to <see cref="AllowedDeserializationTypes"/>
    /// and, when enabled, built-in DotCelery model types known to <see cref="DotCeleryJsonContext"/>.
    /// </summary>
    public bool EnforceDeserializationTypeAllowlist { get; init; }

    /// <summary>
    /// Gets whether DotCelery model types registered in <see cref="DotCeleryJsonContext"/>
    /// are allowed when <see cref="EnforceDeserializationTypeAllowlist"/> is enabled.
    /// </summary>
    public bool AllowDotCeleryTypes { get; init; } = true;

    /// <summary>
    /// Gets the application DTO types that may be deserialized when
    /// <see cref="EnforceDeserializationTypeAllowlist"/> is enabled.
    /// </summary>
    public IReadOnlySet<Type> AllowedDeserializationTypes { get; init; } = new HashSet<Type>();
}

/// <summary>
/// JSON serializer using System.Text.Json with AOT support.
/// </summary>
public sealed class JsonMessageSerializer : IMessageSerializer
{
    private static readonly JsonSerializerOptions FallbackOptions = CreateDefaultOptions();
    private readonly JsonMessageSerializerOptions _serializerOptions;
    private readonly JsonSerializerOptions _options;

    /// <summary>
    /// Initializes a new instance of the <see cref="JsonMessageSerializer"/> class
    /// using the AOT-friendly default options.
    /// </summary>
    public JsonMessageSerializer()
        : this(null, new JsonMessageSerializerOptions()) { }

    /// <summary>
    /// Initializes a new instance of the <see cref="JsonMessageSerializer"/> class.
    /// </summary>
    /// <param name="options">Optional JSON serializer options. If null, uses combined AOT + reflection options.</param>
    public JsonMessageSerializer(JsonSerializerOptions? options)
        : this(options, null) { }

    /// <summary>
    /// Initializes a new instance of the <see cref="JsonMessageSerializer"/> class.
    /// </summary>
    /// <param name="jsonOptions">Optional JSON serializer options. If null, uses combined AOT + reflection options.</param>
    /// <param name="serializerOptions">DotCelery JSON serializer options.</param>
    public JsonMessageSerializer(
        JsonSerializerOptions? jsonOptions,
        JsonMessageSerializerOptions? serializerOptions
    )
    {
        _serializerOptions = serializerOptions ?? new JsonMessageSerializerOptions();
        _options = jsonOptions ?? _serializerOptions.SerializerOptions ?? CreateCombinedOptions();
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
        EnsureDeserializationAllowed(typeof(T));

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
        EnsureDeserializationAllowed(type);

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

    private void EnsureDeserializationAllowed(Type type)
    {
        if (!_serializerOptions.EnforceDeserializationTypeAllowlist)
        {
            return;
        }

        if (_serializerOptions.AllowedDeserializationTypes.Contains(type))
        {
            return;
        }

        if (_serializerOptions.AllowDotCeleryTypes && TryGetTypeInfo(type) is not null)
        {
            return;
        }

        throw new InvalidOperationException(
            $"Deserialization type '{type.FullName}' is not allowed. "
                + "Add the type to JsonMessageSerializerOptions.AllowedDeserializationTypes "
                + "or disable EnforceDeserializationTypeAllowlist."
        );
    }

    private static JsonTypeInfo? TryGetTypeInfo(Type type)
    {
        try
        {
            return DotCeleryJsonContext.Default.GetTypeInfo(type);
        }
        catch
        {
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
