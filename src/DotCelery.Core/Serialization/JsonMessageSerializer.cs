using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Core.Abstractions;

namespace DotCelery.Core.Serialization;

/// <summary>
/// Configuration for <see cref="JsonMessageSerializer"/>.
/// </summary>
/// <remarks>
/// The properties are settable, so the options can be configured through dependency injection,
/// for example with <c>services.Configure&lt;JsonMessageSerializerOptions&gt;(...)</c>.
/// </remarks>
public sealed class JsonMessageSerializerOptions
{
    /// <summary>
    /// Gets or sets the JSON serializer options. When null, DotCelery uses the default
    /// AOT-friendly options with reflection fallback.
    /// </summary>
    public JsonSerializerOptions? SerializerOptions { get; set; }

    /// <summary>
    /// Gets or sets the source-generated context that serializes the task inputs and results.
    /// </summary>
    /// <remarks>
    /// When set, every application payload type must be registered in the context, and the
    /// serializer never falls back to reflection for those payloads, so it works under Native
    /// AOT. DotCelery's own model types keep coming from <see cref="DotCeleryJsonContext"/>, so
    /// the context only registers the application's task inputs and results. Without a context,
    /// a type that no context knows is serialized through reflection, which a Native AOT
    /// application cannot do.
    /// </remarks>
    public JsonSerializerContext? Context { get; set; }

    /// <summary>
    /// Gets or sets whether deserialization is restricted to <see cref="AllowedDeserializationTypes"/>
    /// and, when enabled, built-in DotCelery model types known to <see cref="DotCeleryJsonContext"/>.
    /// </summary>
    public bool EnforceDeserializationTypeAllowlist { get; set; }

    /// <summary>
    /// Gets or sets whether DotCelery model types registered in <see cref="DotCeleryJsonContext"/>
    /// are allowed when <see cref="EnforceDeserializationTypeAllowlist"/> is enabled.
    /// </summary>
    public bool AllowDotCeleryTypes { get; set; } = true;

    /// <summary>
    /// Gets or sets the application DTO types that may be deserialized when
    /// <see cref="EnforceDeserializationTypeAllowlist"/> is enabled.
    /// </summary>
    public IReadOnlySet<Type> AllowedDeserializationTypes { get; set; } = new HashSet<Type>();
}

/// <summary>
/// JSON serializer using System.Text.Json with AOT support.
/// </summary>
public sealed class JsonMessageSerializer : IMessageSerializer
{
    private static readonly JsonSerializerOptions FallbackOptions = CreateDefaultOptions();
    private readonly JsonMessageSerializerOptions _serializerOptions;
    private readonly JsonSerializerContext? _context;
    private readonly JsonSerializerOptions _reflectionOptions;

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
    /// <param name="jsonOptions">Optional JSON serializer options, used by the reflection fallback.</param>
    /// <param name="serializerOptions">DotCelery JSON serializer options.</param>
    public JsonMessageSerializer(
        JsonSerializerOptions? jsonOptions,
        JsonMessageSerializerOptions? serializerOptions
    )
    {
        _serializerOptions = serializerOptions ?? new JsonMessageSerializerOptions();
        _context = _serializerOptions.Context;
        _reflectionOptions = jsonOptions ?? _serializerOptions.SerializerOptions ?? FallbackOptions;
    }

    /// <inheritdoc />
    public string ContentType => "application/json";

    /// <inheritdoc />
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026",
        Justification = "The reflection fallback is refused when the application targets Native AOT or trimming without a source-generated context; a Native AOT application configures JsonMessageSerializerOptions.Context."
    )]
    [UnconditionalSuppressMessage(
        "AOT",
        "IL3050",
        Justification = "The reflection fallback is refused when the application targets Native AOT without a source-generated context; a Native AOT application configures JsonMessageSerializerOptions.Context."
    )]
    public byte[] Serialize<T>(T value)
    {
        // The runtime type is the one that must be serializable, so a typed value sent through
        // an object-typed parameter uses its own metadata
        var type = value?.GetType() ?? typeof(T);

        if (TryGetPayloadTypeInfo(type) is { } typeInfo)
        {
            return JsonSerializer.SerializeToUtf8Bytes(value, typeInfo);
        }

        EnsureReflectionFallbackAllowed(type);
        return SerializeWithReflection(value, type);
    }

    /// <inheritdoc />
    /// <exception cref="JsonException">The payload is invalid, its type is disallowed, or required JSON metadata is missing.</exception>
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026",
        Justification = "The reflection fallback is refused when the application targets Native AOT or trimming without a source-generated context; a Native AOT application configures JsonMessageSerializerOptions.Context."
    )]
    [UnconditionalSuppressMessage(
        "AOT",
        "IL3050",
        Justification = "The reflection fallback is refused when the application targets Native AOT without a source-generated context; a Native AOT application configures JsonMessageSerializerOptions.Context."
    )]
    public T Deserialize<T>(ReadOnlySpan<byte> data)
    {
        EnsureDeserializationAllowed(typeof(T));

        if (TryGetPayloadTypeInfo(typeof(T)) is JsonTypeInfo<T> typeInfo)
        {
            return JsonSerializer.Deserialize(data, typeInfo)
                ?? throw new JsonException($"Failed to deserialize to {typeof(T).Name}");
        }

        EnsureDeserializationFallbackAllowed(typeof(T));
        return DeserializeWithReflection<T>(data);
    }

    /// <inheritdoc />
    /// <exception cref="JsonException">The payload is invalid, its type is disallowed, or required JSON metadata is missing.</exception>
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026",
        Justification = "The reflection fallback is refused when the application targets Native AOT or trimming without a source-generated context; a Native AOT application configures JsonMessageSerializerOptions.Context."
    )]
    [UnconditionalSuppressMessage(
        "AOT",
        "IL3050",
        Justification = "The reflection fallback is refused when the application targets Native AOT without a source-generated context; a Native AOT application configures JsonMessageSerializerOptions.Context."
    )]
    public object Deserialize(ReadOnlySpan<byte> data, Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        EnsureDeserializationAllowed(type);

        if (TryGetPayloadTypeInfo(type) is { } typeInfo)
        {
            return JsonSerializer.Deserialize(data, typeInfo)
                ?? throw new JsonException($"Failed to deserialize to {type.Name}");
        }

        EnsureDeserializationFallbackAllowed(type);
        return DeserializeWithReflection(data, type);
    }

    private JsonTypeInfo? TryGetPayloadTypeInfo(Type type)
    {
        // The configured context serializes the application's payload types; DotCelery's own
        // model types keep coming from its context, so an application registers only its own
        // inputs and results
        if (_context is not null)
        {
            JsonTypeInfo? typeInfo = null;
            try
            {
                typeInfo = _context.GetTypeInfo(type);
            }
            catch (NotSupportedException)
            {
                // Not registered in the application context; DotCelery's context may know it
            }

            if (typeInfo is not null)
            {
                return typeInfo;
            }
        }

        try
        {
            return DotCeleryJsonContext.Default.GetTypeInfo(type);
        }
        catch
        {
            // Type not registered in either context, fall back to reflection-based serialization
            return null;
        }
    }

    // A configured context is the whole contract for application payloads: a type that no
    // context knows is an error, not a reflection fallback. Without one, reflection is used,
    // which Native AOT cannot do.
    private void EnsureReflectionFallbackAllowed(Type type)
    {
        if (_context is not null)
        {
            throw new InvalidOperationException(
                $"Type '{type.FullName}' is not registered in DotCeleryJsonContext or in the "
                    + "JsonSerializerContext configured with JsonMessageSerializerOptions.Context. "
                    + "Add it to your context with [JsonSerializable(typeof(...))]."
            );
        }

        if (!RuntimeFeature.IsDynamicCodeSupported)
        {
            throw new InvalidOperationException(
                $"Type '{type.FullName}' is not registered in DotCeleryJsonContext and the application "
                    + "targets Native AOT, where reflection-based serialization is unavailable. Configure "
                    + "JsonMessageSerializerOptions.Context with a source-generated context that registers "
                    + "the type."
            );
        }
    }

    [RequiresUnreferencedCode(
        "Serializing a type that no source-generated JSON context registers needs reflection."
    )]
    [RequiresDynamicCode(
        "Serializing a type that no source-generated JSON context registers may need runtime code generation."
    )]
    private byte[] SerializeWithReflection(object? value, Type type) =>
        JsonSerializer.SerializeToUtf8Bytes(value, type, _reflectionOptions);

    [RequiresUnreferencedCode(
        "Deserializing a type that no source-generated JSON context registers needs reflection."
    )]
    [RequiresDynamicCode(
        "Deserializing a type that no source-generated JSON context registers may need runtime code generation."
    )]
    private T DeserializeWithReflection<T>(ReadOnlySpan<byte> data) =>
        JsonSerializer.Deserialize<T>(data, _reflectionOptions)
        ?? throw new JsonException($"Failed to deserialize to {typeof(T).Name}");

    [RequiresUnreferencedCode(
        "Deserializing a type that no source-generated JSON context registers needs reflection."
    )]
    [RequiresDynamicCode(
        "Deserializing a type that no source-generated JSON context registers may need runtime code generation."
    )]
    private object DeserializeWithReflection(ReadOnlySpan<byte> data, Type type) =>
        JsonSerializer.Deserialize(data, type, _reflectionOptions)
        ?? throw new JsonException($"Failed to deserialize to {type.Name}");

    private void EnsureDeserializationFallbackAllowed(Type type)
    {
        try
        {
            EnsureReflectionFallbackAllowed(type);
        }
        catch (InvalidOperationException ex)
        {
            throw new JsonException(ex.Message, ex);
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

        if (_serializerOptions.AllowDotCeleryTypes && IsDotCeleryType(type))
        {
            return;
        }

        throw new JsonException(
            $"Deserialization type '{type.FullName}' is not allowed. "
                + "Add the type to JsonMessageSerializerOptions.AllowedDeserializationTypes "
                + "or disable EnforceDeserializationTypeAllowlist."
        );
    }

    // The allowlist names the types DotCelery itself defines, whatever context serializes payloads
    private static bool IsDotCeleryType(Type type)
    {
        try
        {
            return DotCeleryJsonContext.Default.GetTypeInfo(type) is not null;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Creates the default JSON serializer options (reflection based, for backwards compatibility).
    /// </summary>
    /// <returns>The default JSON serializer options.</returns>
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026",
        Justification = "These options are the reflection fallback, which Native AOT applications do not use."
    )]
    [UnconditionalSuppressMessage(
        "AOT",
        "IL3050",
        Justification = "These options are the reflection fallback, which Native AOT applications do not use."
    )]
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
    /// Creates combined options that first try the AOT context, then fall back to reflection.
    /// </summary>
    /// <returns>Combined JSON serializer options.</returns>
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026",
        Justification = "These options fall back to reflection for types the context does not register, which Native AOT applications avoid by configuring a context."
    )]
    [UnconditionalSuppressMessage(
        "AOT",
        "IL3050",
        Justification = "These options fall back to reflection for types the context does not register, which Native AOT applications avoid by configuring a context."
    )]
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
