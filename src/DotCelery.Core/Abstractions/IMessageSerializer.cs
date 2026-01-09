namespace DotCelery.Core.Abstractions;

/// <summary>
/// Serializer for task messages and results.
/// </summary>
public interface IMessageSerializer
{
    /// <summary>
    /// Gets the content type identifier (e.g., "application/json").
    /// </summary>
    string ContentType { get; }

    /// <summary>
    /// Serializes an object to bytes.
    /// </summary>
    /// <typeparam name="T">Type to serialize.</typeparam>
    /// <param name="value">Value to serialize.</param>
    /// <returns>Serialized bytes.</returns>
    byte[] Serialize<T>(T value);

    /// <summary>
    /// Deserializes bytes to an object.
    /// </summary>
    /// <typeparam name="T">Target type.</typeparam>
    /// <param name="data">Serialized data.</param>
    /// <returns>Deserialized object.</returns>
    T Deserialize<T>(ReadOnlySpan<byte> data);

    /// <summary>
    /// Deserializes bytes to an object of the specified type.
    /// </summary>
    /// <param name="data">Serialized data.</param>
    /// <param name="type">Target type.</param>
    /// <returns>Deserialized object.</returns>
    object Deserialize(ReadOnlySpan<byte> data, Type type);
}
