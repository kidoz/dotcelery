using System.Text.Json;
using System.Text.Json.Serialization;

namespace DotCelery.Backend.Redis.Serialization;

/// <summary>
/// Provides consistent JSON serialization options for Redis backend.
/// Use AOT type info from <see cref="RedisBackendJsonContext"/> when available,
/// or <see cref="FallbackOptions"/> for types not registered in the AOT context.
/// </summary>
internal static class RedisJsonHelper
{
    /// <summary>
    /// Gets shared fallback options for types not registered in AOT context.
    /// Configured consistently with <see cref="RedisBackendJsonContext"/> options.
    /// </summary>
    public static JsonSerializerOptions FallbackOptions { get; } = CreateFallbackOptions();

    private static JsonSerializerOptions CreateFallbackOptions()
    {
        return new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            WriteIndented = false,
            Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) },
        };
    }
}
