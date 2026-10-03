using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// Records a task outcome: it stores the result and, with an inbox store registered, marks a
/// successful message as processed.
/// </summary>
/// <remarks>
/// <para>
/// When the result backend and the inbox store keep their records in the same transactional
/// storage, both writes commit in one transaction, so a message is marked as processed exactly
/// when its result is stored. A worker that stops after the commit does not run the task again
/// when the message is redelivered, and one that stops before it leaves neither the result nor
/// the record, so the task runs again.
/// </para>
/// <para>
/// Otherwise the result is stored first and the message is marked afterwards, so a failure
/// between the two leaves the message to be processed again.
/// </para>
/// </remarks>
public sealed class OutcomeRecorder
{
    private readonly IResultBackend _results;
    private readonly IInboxStore _inbox;
    private readonly ITransactionalStorage? _storage;

    private OutcomeRecorder(
        IResultBackend results,
        IInboxStore inbox,
        ITransactionalStorage? storage
    )
    {
        _results = results;
        _inbox = inbox;
        _storage = storage;
    }

    /// <summary>
    /// Creates a recorder for the given stores, or returns <c>null</c> when no inbox store is
    /// registered.
    /// </summary>
    /// <param name="results">The result backend.</param>
    /// <param name="inbox">The inbox store, if one is registered.</param>
    /// <returns>The recorder, or <c>null</c> when results can be stored as they come.</returns>
    public static OutcomeRecorder? Create(IResultBackend results, IInboxStore? inbox)
    {
        ArgumentNullException.ThrowIfNull(results);

        return inbox is null
            ? null
            : new OutcomeRecorder(results, inbox, SharedStorage(results, inbox));
    }

    /// <summary>
    /// Stores the outcome, and marks a successful message as processed.
    /// </summary>
    /// <param name="result">The task result.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async ValueTask RecordAsync(
        TaskResult result,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(result);

        if (result.State != TaskState.Success)
        {
            await _results
                .StoreResultAsync(result, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        if (_storage is null)
        {
            // The result first: a failure between the writes leaves the message unmarked, so it
            // is processed again rather than skipped without a result.
            await _results
                .StoreResultAsync(result, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
            await _inbox
                .MarkProcessedAsync(result.TaskId, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        await _storage
            .RunInTransactionAsync(
                async token =>
                {
                    await _results
                        .StoreResultAsync(result, cancellationToken: token)
                        .ConfigureAwait(false);
                    await _inbox
                        .MarkProcessedAsync(result.TaskId, cancellationToken: token)
                        .ConfigureAwait(false);
                },
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    // Records can only commit together when both stores write to the same database
    private static ITransactionalStorage? SharedStorage(IResultBackend results, IInboxStore inbox)
    {
        if (
            results is not IStorageBackedStore resultStore
            || inbox is not IStorageBackedStore inboxStore
            || !ReferenceEquals(resultStore.Storage, inboxStore.Storage)
        )
        {
            return null;
        }

        return resultStore.Storage as ITransactionalStorage;
    }
}
