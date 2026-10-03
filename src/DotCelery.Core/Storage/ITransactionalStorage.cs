namespace DotCelery.Core.Storage;

/// <summary>
/// Storage that can write in a transaction the caller owns, such as the transaction of the
/// application's own database session.
/// </summary>
/// <remarks>
/// A store hands the caller's transaction to the provider for one operation, so the record it
/// writes commits or rolls back together with the caller's other changes. The transaction
/// object is provider-specific, such as a <c>DbTransaction</c> for the SQL providers. A store
/// that is given a transaction the provider cannot write in refuses the operation instead of
/// writing outside the transaction.
/// </remarks>
public interface ITransactionalStorage
{
    /// <summary>
    /// Gets whether the storage can write in the given transaction, which it can when the
    /// transaction belongs to the database the storage writes to.
    /// </summary>
    /// <param name="transaction">The caller's transaction object.</param>
    /// <returns><c>true</c> if the transaction can be written in.</returns>
    bool CanWriteIn(object transaction);

    /// <summary>
    /// Runs work with the writes it makes joined to the given transaction. The storage does not
    /// commit or roll back the transaction; the caller decides its outcome.
    /// </summary>
    /// <param name="transaction">The caller's transaction object.</param>
    /// <param name="work">The work; the storage writes it makes run in the transaction.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask WriteInAsync(
        object transaction,
        Func<CancellationToken, ValueTask> work,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Runs work in a transaction this storage starts, commits when the work completes, and
    /// rolls back when the work fails. Store writes made by the work run in the transaction.
    /// </summary>
    /// <param name="work">
    /// The work to run in the transaction. It can run again when a failure is safely retryable,
    /// so it must be safe to repeat.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask RunInTransactionAsync(
        Func<CancellationToken, ValueTask> work,
        CancellationToken cancellationToken = default
    );
}
