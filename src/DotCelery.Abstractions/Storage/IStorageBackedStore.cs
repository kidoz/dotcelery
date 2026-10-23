namespace DotCelery.Core.Storage;

/// <summary>
/// A store that keeps its records in one storage provider, so it can take part in a transaction
/// the provider runs.
/// </summary>
public interface IStorageBackedStore
{
    /// <summary>
    /// Gets the storage provider the store keeps its records in.
    /// </summary>
    IStorageProvider Storage { get; }
}
