using System.Text.Json.Serialization;
using DotCelery.Core.Dashboard;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// Source-generated serialization for the store records that no other context registers, so
/// those records serialize without reflection and work under Native AOT.
/// </summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    WriteIndented = false,
    UseStringEnumConverter = true
)]
[JsonSerializable(typeof(ResultBackend.TaskRecord))]
[JsonSerializable(typeof(MetricsSnapshot))]
internal sealed partial class StoreJsonContext : JsonSerializerContext;
