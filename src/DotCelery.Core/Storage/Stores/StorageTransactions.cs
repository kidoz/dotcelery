namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// Support for writing store records inside a transaction the caller owns.
/// </summary>
internal static class StorageTransactions
{
    /// <summary>
    /// Gets how the storage writes in a transaction the caller passed.
    /// </summary>
    /// <param name="storage">The storage provider.</param>
    /// <param name="transaction">The caller's transaction; must not be <c>null</c>.</param>
    /// <returns>The transactional storage.</returns>
    /// <exception cref="NotSupportedException">
    /// The storage cannot write in the caller's transaction. The store refuses the operation
    /// rather than writing outside the transaction, which would break the guarantee the caller
    /// asked for by passing one.
    /// </exception>
    public static ITransactionalStorage Resolve(IStorageProvider storage, object transaction)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentNullException.ThrowIfNull(transaction);

        if (storage is ITransactionalStorage transactional && transactional.CanWriteIn(transaction))
        {
            return transactional;
        }

        throw new NotSupportedException(
            $"The {storage.Name} storage cannot write in a caller's transaction "
                + $"({transaction.GetType().Name}); the write is refused rather than "
                + "committed outside the transaction."
        );
    }
}
