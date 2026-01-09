using System.Text.Json;
using System.Text.Json.Serialization;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Batches;
using DotCelery.Core.Canvas;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Migrations;
using DotCelery.Core.Models;
using DotCelery.Core.Outbox;
using DotCelery.Core.Progress;
using DotCelery.Core.Sagas;
using DotCelery.Core.Signals;

namespace DotCelery.Core.Serialization;

/// <summary>
/// AOT-friendly JSON serialization context for DotCelery core types.
/// </summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    WriteIndented = false,
    UseStringEnumConverter = true
)]
// Core Models
[JsonSerializable(typeof(TaskMessage))]
[JsonSerializable(typeof(TaskResult))]
[JsonSerializable(typeof(TaskExceptionInfo))]
[JsonSerializable(typeof(TaskState))]
[JsonSerializable(typeof(RevokeOptions))]
[JsonSerializable(typeof(CancellationSignal))]
// Canvas Types
[JsonSerializable(typeof(Signature))]
[JsonSerializable(typeof(Chain))]
[JsonSerializable(typeof(Group))]
[JsonSerializable(typeof(Chord))]
// Dead Letter Queue
[JsonSerializable(typeof(DeadLetterMessage))]
[JsonSerializable(typeof(DeadLetterReason))]
// Outbox Pattern
[JsonSerializable(typeof(OutboxMessage))]
[JsonSerializable(typeof(OutboxMessageStatus))]
// Signals
[JsonSerializable(typeof(SignalMessage))]
[JsonSerializable(typeof(BeforeTaskPublishSignal))]
[JsonSerializable(typeof(AfterTaskPublishSignal))]
[JsonSerializable(typeof(TaskPreRunSignal))]
[JsonSerializable(typeof(TaskPostRunSignal))]
[JsonSerializable(typeof(TaskSuccessSignal))]
[JsonSerializable(typeof(TaskFailureSignal))]
[JsonSerializable(typeof(TaskRetrySignal))]
[JsonSerializable(typeof(TaskRevokedSignal))]
[JsonSerializable(typeof(TaskRejectedSignal))]
// Saga Types
[JsonSerializable(typeof(Saga))]
[JsonSerializable(typeof(SagaStep))]
[JsonSerializable(typeof(SagaState))]
[JsonSerializable(typeof(SagaStepState))]
[JsonSerializable(typeof(SagaStateChangedSignal))]
[JsonSerializable(typeof(SagaStepCompletedSignal))]
[JsonSerializable(typeof(SagaStepCompensatedSignal))]
[JsonSerializable(typeof(SagaCompensationStartedSignal))]
// Batch Types
[JsonSerializable(typeof(Batch))]
[JsonSerializable(typeof(BatchState))]
// Progress Types
[JsonSerializable(typeof(ProgressInfo))]
[JsonSerializable(typeof(ProgressUpdatedSignal))]
// Revocation Types
[JsonSerializable(typeof(RevocationEvent))]
// Migration Types
[JsonSerializable(typeof(MigrationRecord))]
// Collection Types (commonly used in models)
[JsonSerializable(typeof(IReadOnlyList<string>))]
[JsonSerializable(typeof(List<string>))]
[JsonSerializable(typeof(string[]))]
[JsonSerializable(typeof(IReadOnlyList<SagaStep>))]
[JsonSerializable(typeof(List<SagaStep>))]
[JsonSerializable(typeof(IReadOnlyDictionary<string, string>))]
[JsonSerializable(typeof(Dictionary<string, string>))]
[JsonSerializable(typeof(IReadOnlyList<Signature>))]
[JsonSerializable(typeof(List<Signature>))]
// Primitive types for completeness
[JsonSerializable(typeof(byte[]))]
[JsonSerializable(typeof(DateTimeOffset))]
[JsonSerializable(typeof(DateTimeOffset?))]
[JsonSerializable(typeof(TimeSpan))]
[JsonSerializable(typeof(TimeSpan?))]
[JsonSerializable(typeof(int))]
[JsonSerializable(typeof(long))]
[JsonSerializable(typeof(double))]
[JsonSerializable(typeof(bool))]
[JsonSerializable(typeof(string))]
// For dynamic metadata (will serialize as JsonElement)
[JsonSerializable(typeof(JsonElement))]
[JsonSerializable(typeof(Dictionary<string, JsonElement>))]
[JsonSerializable(typeof(IReadOnlyDictionary<string, JsonElement>))]
public partial class DotCeleryJsonContext : JsonSerializerContext
{
    /// <summary>
    /// Gets an AOT-compatible JSON serializer options instance.
    /// Uses the source-generated Default context.
    /// </summary>
    public static JsonSerializerOptions AotOptions => Default.Options;
}
