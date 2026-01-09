using System.Text.Json.Serialization;
using DotCelery.Core.Batches;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Migrations;
using DotCelery.Core.Models;
using DotCelery.Core.Outbox;
using DotCelery.Core.Sagas;
using DotCelery.Core.Serialization;
using DotCelery.Core.Signals;

namespace DotCelery.Backend.Redis.Serialization;

/// <summary>
/// AOT-friendly JSON serialization context for Redis backend types.
/// Extends the core context with Redis-specific internal types.
/// </summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    WriteIndented = false,
    UseStringEnumConverter = true
)]
// Types from DotCelery.Core that are serialized by Redis backend
[JsonSerializable(typeof(TaskMessage))]
[JsonSerializable(typeof(TaskResult))]
[JsonSerializable(typeof(Batch))]
[JsonSerializable(typeof(DeadLetterMessage))]
[JsonSerializable(typeof(OutboxMessage))]
[JsonSerializable(typeof(SignalMessage))]
[JsonSerializable(typeof(MigrationRecord))]
[JsonSerializable(typeof(Saga))]
// Redis-specific internal types
[JsonSerializable(typeof(RevocationEntry))]
#pragma warning disable CA1852 // Type can be sealed - partial class for source generation
internal partial class RedisBackendJsonContext : JsonSerializerContext;
#pragma warning restore CA1852

/// <summary>
/// Internal type for storing revocation information in Redis.
/// </summary>
internal sealed record RevocationEntry
{
    /// <summary>
    /// Gets the task ID.
    /// </summary>
    public required string TaskId { get; init; }

    /// <summary>
    /// Gets the revocation options.
    /// </summary>
    public required RevokeOptions Options { get; init; }

    /// <summary>
    /// Gets when the task was revoked.
    /// </summary>
    public required DateTimeOffset RevokedAt { get; init; }

    /// <summary>
    /// Gets when the revocation expires.
    /// </summary>
    public DateTimeOffset? ExpiresAt { get; init; }
}
