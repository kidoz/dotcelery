using System.Text.Json;

namespace DotCelery.Core.Signals;

/// <summary>
/// Message envelope for queued task signals.
/// </summary>
public sealed record SignalMessage
{
    /// <summary>
    /// Gets the unique message ID.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Gets the signal type name.
    /// </summary>
    public required string SignalType { get; init; }

    /// <summary>
    /// Gets the task ID associated with the signal.
    /// </summary>
    public required string TaskId { get; init; }

    /// <summary>
    /// Gets the task name associated with the signal.
    /// </summary>
    public required string TaskName { get; init; }

    /// <summary>
    /// Gets the serialized signal payload as JSON.
    /// </summary>
    public required string Payload { get; init; }

    /// <summary>
    /// Gets the timestamp when the message was created.
    /// </summary>
    public required DateTimeOffset CreatedAt { get; init; }

    /// <summary>
    /// Creates a signal message from a signal instance.
    /// </summary>
    /// <typeparam name="TSignal">The signal type.</typeparam>
    /// <param name="signal">The signal instance.</param>
    /// <param name="jsonOptions">Optional JSON serializer options.</param>
    /// <returns>A signal message.</returns>
    public static SignalMessage Create<TSignal>(
        TSignal signal,
        JsonSerializerOptions? jsonOptions = null
    )
        where TSignal : ITaskSignal
    {
        ArgumentNullException.ThrowIfNull(signal);

        return new SignalMessage
        {
            Id = Guid.NewGuid().ToString(),
            SignalType = typeof(TSignal).AssemblyQualifiedName!,
            TaskId = signal.TaskId,
            TaskName = signal.TaskName,
            Payload = JsonSerializer.Serialize(signal, jsonOptions),
            CreatedAt = DateTimeOffset.UtcNow,
        };
    }

    /// <summary>
    /// Deserializes the signal from the payload.
    /// </summary>
    /// <param name="jsonOptions">Optional JSON serializer options.</param>
    /// <returns>The deserialized signal.</returns>
    /// <exception cref="InvalidOperationException">If the signal type cannot be resolved.</exception>
    public ITaskSignal Deserialize(JsonSerializerOptions? jsonOptions = null)
    {
        var type =
            Type.GetType(SignalType)
            ?? throw new InvalidOperationException($"Cannot resolve signal type: {SignalType}");

        return (ITaskSignal)(
            JsonSerializer.Deserialize(Payload, type, jsonOptions)
            ?? throw new InvalidOperationException("Failed to deserialize signal payload")
        );
    }
}
