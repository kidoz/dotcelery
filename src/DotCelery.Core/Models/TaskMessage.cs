namespace DotCelery.Core.Models;

/// <summary>
/// Message format for task transport through brokers.
/// </summary>
/// <remarks>
/// <para>
/// Schema versioning policy:
/// </para>
/// <list type="bullet">
/// <item>Patch versions (1.0.x): No message format changes</item>
/// <item>Minor versions (1.x.0): Additive changes only (new optional fields)</item>
/// <item>Major versions (x.0.0): Breaking changes allowed with migration path</item>
/// </list>
/// <para>
/// Version history:
/// </para>
/// <list type="bullet">
/// <item>Version 1: Initial schema (all fields up to Headers)</item>
/// </list>
/// </remarks>
public sealed record TaskMessage
{
    /// <summary>
    /// Current schema version for newly created messages.
    /// </summary>
    public const int CurrentSchemaVersion = 1;

    /// <summary>
    /// Gets the schema version of this message.
    /// Used for wire-compatibility during rolling upgrades.
    /// </summary>
    /// <remarks>
    /// Workers should accept messages where <c>SchemaVersion &lt;= CurrentSchemaVersion</c>.
    /// Unknown fields in newer versions are ignored by older workers.
    /// </remarks>
    public int SchemaVersion { get; init; } = CurrentSchemaVersion;

    /// <summary>
    /// Gets the unique task invocation ID.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Gets the task name for routing.
    /// </summary>
    public required string Task { get; init; }

    /// <summary>
    /// Gets the serialized task arguments.
    /// </summary>
#pragma warning disable CA1819 // Properties should not return arrays - Required for serialization
    public required byte[] Args { get; init; }
#pragma warning restore CA1819

    /// <summary>
    /// Gets the content type of Args (e.g., "application/json").
    /// </summary>
    public required string ContentType { get; init; }

    /// <summary>
    /// Gets the UTC timestamp when message was created.
    /// </summary>
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the scheduled execution time.
    /// </summary>
    public DateTimeOffset? Eta { get; init; }

    /// <summary>
    /// Gets the message expiration time.
    /// </summary>
    public DateTimeOffset? Expires { get; init; }

    /// <summary>
    /// Gets the current retry count.
    /// </summary>
    public int Retries { get; init; }

    /// <summary>
    /// Gets the maximum allowed retries.
    /// </summary>
    public int MaxRetries { get; init; } = 3;

    /// <summary>
    /// Gets the parent task ID for chains.
    /// </summary>
    public string? ParentId { get; init; }

    /// <summary>
    /// Gets the root task ID for workflows.
    /// </summary>
    public string? RootId { get; init; }

    /// <summary>
    /// Gets the batch ID (if task is part of a batch).
    /// </summary>
    public string? BatchId { get; init; }

    /// <summary>
    /// Gets the correlation ID for tracing.
    /// </summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// Gets the task priority (higher = more urgent).
    /// </summary>
    public int Priority { get; init; }

    /// <summary>
    /// Gets the target queue name.
    /// </summary>
    public string Queue { get; init; } = "celery";

    /// <summary>
    /// Gets the partition key for ordered processing.
    /// Messages with the same partition key are processed sequentially.
    /// </summary>
    public string? PartitionKey { get; init; }

    /// <summary>
    /// Gets the tenant ID for multi-tenancy routing.
    /// </summary>
    public string? TenantId { get; init; }

    /// <summary>
    /// Gets the custom headers for extensions.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
}
