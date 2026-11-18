using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Core.Progress;
using DotCelery.Core.Sagas;

namespace DotCelery.Core.Signals;

/// <summary>Maps queued signal discriminators to the concrete types allowed by the host.</summary>
public sealed class SignalTypeRegistry
{
    internal const string ReflectionWarning =
        "Built-in signal types may use reflection-based serialization. Native AOT applications dispatch signals inline.";

    private static readonly FrozenDictionary<string, Type> BuiltIns = new Dictionary<string, Type>(
        StringComparer.Ordinal
    )
    {
        ["before-task-publish"] = typeof(BeforeTaskPublishSignal),
        ["after-task-publish"] = typeof(AfterTaskPublishSignal),
        ["task-pre-run"] = typeof(TaskPreRunSignal),
        ["task-post-run"] = typeof(TaskPostRunSignal),
        ["task-success"] = typeof(TaskSuccessSignal),
        ["task-failure"] = typeof(TaskFailureSignal),
        ["task-retry"] = typeof(TaskRetrySignal),
        ["task-revoked"] = typeof(TaskRevokedSignal),
        ["task-rejected"] = typeof(TaskRejectedSignal),
        ["task-requeued"] = typeof(TaskRequeuedSignal),
        ["progress-updated"] = typeof(ProgressUpdatedSignal),
        ["saga-state-changed"] = typeof(SagaStateChangedSignal),
        ["saga-step-completed"] = typeof(SagaStepCompletedSignal),
        ["saga-step-compensated"] = typeof(SagaStepCompensatedSignal),
        ["saga-compensation-started"] = typeof(SagaCompensationStartedSignal),
    }.ToFrozenDictionary(StringComparer.Ordinal);

    private static readonly FrozenDictionary<Type, string> BuiltInIds = BuiltIns.ToFrozenDictionary(
        pair => pair.Value,
        pair => pair.Key
    );
    private readonly ConcurrentDictionary<string, JsonTypeInfo> _custom = new(
        StringComparer.Ordinal
    );

    /// <summary>Gets or sets whether to write legacy type names during a rolling upgrade.</summary>
    public bool WriteLegacyTypeNames { get; set; }

    /// <summary>Registers a concrete custom signal and the serializer metadata to use for it.</summary>
    /// <param name="discriminator">A stable, unique name shared by producers and consumers.</param>
    /// <param name="typeInfo">JSON metadata for a concrete <see cref="ITaskSignal"/>.</param>
    public void Register(string discriminator, JsonTypeInfo typeInfo)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(discriminator);
        ArgumentNullException.ThrowIfNull(typeInfo);
        if (
            !typeof(ITaskSignal).IsAssignableFrom(typeInfo.Type)
            || typeInfo.Type.IsAbstract
            || typeInfo.Type.IsInterface
        )
            throw new ArgumentException(
                "A queued signal needs a concrete ITaskSignal type.",
                nameof(typeInfo)
            );
        if (
            BuiltIns.ContainsKey(discriminator)
            || BuiltInIds.ContainsKey(typeInfo.Type)
            || discriminator.Contains(',')
        )
            throw new ArgumentException(
                "The signal discriminator or type conflicts with a reserved signal name.",
                nameof(discriminator)
            );
        lock (_custom)
        {
            if (
                _custom.Values.Any(existing => existing.Type == typeInfo.Type)
                || !_custom.TryAdd(discriminator, typeInfo)
            )
                throw new ArgumentException(
                    "The signal discriminator or type is already registered.",
                    nameof(discriminator)
                );
        }
    }

    /// <summary>Serializes a built-in or registered signal to a queue envelope.</summary>
    /// <param name="signal">The signal to serialize.</param>
    /// <param name="jsonOptions">Optional serialization settings for built-in signals.</param>
    /// <returns>The queued signal envelope.</returns>
    [RequiresUnreferencedCode(ReflectionWarning)]
    [RequiresDynamicCode(ReflectionWarning)]
    public SignalMessage Create(ITaskSignal signal, JsonSerializerOptions? jsonOptions = null)
    {
        ArgumentNullException.ThrowIfNull(signal);
        var type = signal.GetType();
        string payload;
        if (BuiltInIds.TryGetValue(type, out var discriminator))
            payload = JsonSerializer.Serialize(signal, type, jsonOptions);
        else
        {
            var registration = _custom.FirstOrDefault(pair => pair.Value.Type == type);
            discriminator =
                registration.Key
                ?? throw new InvalidOperationException(
                    "The custom signal type must be registered before it can be queued."
                );
            payload = JsonSerializer.Serialize(signal, registration.Value);
        }

        return new SignalMessage
        {
            Id = Guid.NewGuid().ToString(),
            SignalType = WriteLegacyTypeNames ? type.AssemblyQualifiedName! : discriminator,
            TaskId = signal.TaskId,
            TaskName = signal.TaskName,
            Payload = payload,
            CreatedAt = DateTimeOffset.UtcNow,
        };
    }

    /// <summary>Deserializes a queued signal using this registry.</summary>
    /// <param name="message">The stored signal envelope.</param>
    /// <param name="jsonOptions">Optional serialization settings for built-in signals.</param>
    /// <returns>The signal from the envelope.</returns>
    [RequiresUnreferencedCode(ReflectionWarning)]
    [RequiresDynamicCode(ReflectionWarning)]
    public ITaskSignal Deserialize(SignalMessage message, JsonSerializerOptions? jsonOptions = null)
    {
        ArgumentNullException.ThrowIfNull(message);
        var discriminator = message.SignalType;
        if (string.IsNullOrWhiteSpace(discriminator))
            throw new InvalidOperationException(
                "Cannot resolve signal type without a discriminator."
            );

        var builtIn =
            BuiltIns.GetValueOrDefault(discriminator)
            ?? BuiltInIds.Keys.FirstOrDefault(type => IsLegacyName(discriminator, type));
        object? value;
        if (builtIn is not null)
            value = JsonSerializer.Deserialize(message.Payload, builtIn, jsonOptions);
        else
        {
            var custom =
                _custom.GetValueOrDefault(discriminator)
                ?? _custom.Values.FirstOrDefault(info => IsLegacyName(discriminator, info.Type))
                ?? throw new InvalidOperationException(
                    "Cannot resolve signal type. Refusing to deserialize an unregistered signal."
                );
            value = JsonSerializer.Deserialize(message.Payload, custom);
        }
        return value as ITaskSignal
            ?? throw new InvalidOperationException("Failed to deserialize signal payload.");
    }

    private static bool IsLegacyName(string name, Type type) =>
        type.FullName is { } fullName
        && name.Length > fullName.Length
        && name[fullName.Length] == ','
        && name.StartsWith(fullName, StringComparison.Ordinal);
}
