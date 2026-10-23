using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using DotCelery.Core.Canvas;

namespace DotCelery.Core.Models;

/// <summary>
/// Creates the messages every dispatch path publishes, with the fields each path shares.
/// </summary>
/// <remarks>
/// A follow-up keeps the identity of the message that produced it: its root, parent, tenant,
/// and partition. That propagation lives here so a chain step and a linked callback cannot
/// drop it.
/// </remarks>
public static class TaskMessageFactory
{
    /// <summary>Derives a stable, bounded publication ID from a workflow step and explicit retry attempt.</summary>
    /// <param name="workflowId">The workflow identity.</param>
    /// <param name="partId">The step or callback identity.</param>
    /// <param name="phase">Execution or compensation.</param>
    /// <param name="attempt">An explicit new attempt, not a redelivery count.</param>
    /// <returns>A stable ID that fits every storage provider's key limit.</returns>
    public static string ContinuationId(
        string workflowId,
        string partId,
        string phase,
        int attempt = 0
    )
    {
        ArgumentNullException.ThrowIfNull(workflowId);
        ArgumentNullException.ThrowIfNull(partId);
        ArgumentNullException.ThrowIfNull(phase);
        ArgumentOutOfRangeException.ThrowIfNegative(attempt);
        var identity =
            $"{workflowId.Length}:{workflowId}{partId.Length}:{partId}{phase.Length}:{phase}:{attempt.ToString(CultureInfo.InvariantCulture)}";
        return "dc-" + Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(identity)));
    }

    /// <summary>
    /// Creates a message for a task the application or the scheduler sends.
    /// </summary>
    /// <param name="id">The task ID.</param>
    /// <param name="task">The task name.</param>
    /// <param name="args">The serialized task input.</param>
    /// <param name="contentType">The input's content type.</param>
    /// <param name="timestamp">When the message was created.</param>
    /// <param name="queue">The queue the message is published to.</param>
    /// <param name="eta">When the task becomes due, if it is scheduled.</param>
    /// <param name="expires">When the message expires, if it may.</param>
    /// <param name="maxRetries">The maximum number of retries.</param>
    /// <param name="priority">The message priority.</param>
    /// <param name="correlationId">The correlation ID, if the sender set one.</param>
    /// <param name="tenantId">The tenant the task belongs to, if the sender set one.</param>
    /// <param name="partitionKey">The partition the task belongs to, if the sender set one.</param>
    /// <param name="headers">The message headers, if any.</param>
    /// <param name="batchId">The batch the task belongs to, if it is a batch member.</param>
    /// <returns>The task message.</returns>
    public static TaskMessage ForTask(
        string id,
        string task,
        byte[] args,
        string contentType,
        DateTimeOffset timestamp,
        string queue,
        DateTimeOffset? eta = null,
        DateTimeOffset? expires = null,
        int maxRetries = 0,
        int priority = 0,
        string? correlationId = null,
        string? tenantId = null,
        string? partitionKey = null,
        IReadOnlyDictionary<string, string>? headers = null,
        string? batchId = null
    ) =>
        new()
        {
            Id = id,
            Task = task,
            Args = args,
            ContentType = contentType,
            Timestamp = timestamp,
            Eta = eta,
            Expires = expires,
            MaxRetries = maxRetries,
            Priority = priority,
            Queue =
                DotCelery.Core.MultiTenancy.TenantContext.QueueFor(
                    tenantId ?? DotCelery.Core.MultiTenancy.TenantContext.Current.TenantId
                ) ?? queue,
            CorrelationId = correlationId,
            TenantId = tenantId ?? DotCelery.Core.MultiTenancy.TenantContext.Current.TenantId,
            PartitionKey = partitionKey,
            Headers = headers,
            BatchId = batchId,
        };

    /// <summary>
    /// Creates the message a processed task publishes for its next chain step or callback.
    /// </summary>
    /// <param name="parent">The message whose task produced the follow-up.</param>
    /// <param name="id">The follow-up task ID.</param>
    /// <param name="signature">The signature that describes the follow-up.</param>
    /// <param name="args">The serialized follow-up input.</param>
    /// <param name="contentType">The input's content type.</param>
    /// <param name="timestamp">When the message was created.</param>
    /// <param name="queue">The queue the message is published to.</param>
    /// <param name="chain">The remaining chain steps, when the follow-up continues a chain.</param>
    /// <returns>The follow-up task message.</returns>
    public static TaskMessage ForFollowUp(
        TaskMessage parent,
        string id,
        Signature signature,
        byte[] args,
        string contentType,
        DateTimeOffset timestamp,
        string queue,
        IReadOnlyList<ChainStep>? chain = null
    ) =>
        new()
        {
            Id = id,
            Task = signature.TaskName,
            Args = args,
            ContentType = contentType,
            Timestamp = timestamp,
            Chain = chain,
            Link = signature.Link,
            LinkError = signature.LinkError,
            RootId = parent.RootId ?? parent.Id,
            ParentId = parent.Id,
            TenantId = parent.TenantId,
            PartitionKey = parent.PartitionKey,
            Queue = DotCelery.Core.MultiTenancy.TenantContext.QueueFor(parent.TenantId) ?? queue,
            Priority = signature.Priority,
            MaxRetries = signature.MaxRetries,
            Headers = signature.Headers,
            Eta = signature.EffectiveEta,
            Expires = signature.Expires,
        };

    /// <summary>
    /// Creates the placeholder message a serialized payload is validated against when only its
    /// signature is checked.
    /// </summary>
    /// <returns>The placeholder task message.</returns>
    public static TaskMessage ForSignatureCheck() =>
        new()
        {
            Id = "unknown",
            Task = "unknown",
            Args = [],
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
        };
}
