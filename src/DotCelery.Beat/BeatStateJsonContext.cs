using System.Text.Json.Serialization;
using DotCelery.Core.Models;

namespace DotCelery.Beat;

internal sealed record BeatEntryState(DateTimeOffset LastRunTime, byte[]? PendingPayload);

[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
)]
[JsonSerializable(typeof(BeatEntryState))]
internal sealed partial class SharedBeatJsonContext : JsonSerializerContext;
