using DotCelery.Core.Models;

namespace DotCelery.Core.Abstractions;

/// <summary>
/// Keeps a successful outcome with its inbox marker for the entire deduplication period.
/// Recovery does not depend on the independently configured result-cache expiry.
/// </summary>
public interface IInboxOutcomeStore : IInboxStore
{
    /// <summary>Reads the recovery outcome, or null for a missing or legacy marker.</summary>
    /// <param name="taskId">The task invocation ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The retained outcome.</returns>
    ValueTask<TaskResult?> GetOutcomeAsync(
        string taskId,
        CancellationToken cancellationToken = default
    );

    /// <summary>Atomically stores the outcome and its processed marker.</summary>
    /// <param name="result">The successful outcome and publication checkpoints.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask StoreOutcomeAsync(TaskResult result, CancellationToken cancellationToken = default);
}
