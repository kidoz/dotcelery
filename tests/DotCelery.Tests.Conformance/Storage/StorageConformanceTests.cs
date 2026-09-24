using System.Text;
using DotCelery.Core.Storage;
using Microsoft.Extensions.Time.Testing;

namespace DotCelery.Tests.Conformance.Storage;

/// <summary>
/// Base class for storage conformance tests. Each provider derives from the test classes and
/// creates the provider under test, so every provider is held to the same behavior.
/// </summary>
/// <remarks>
/// Tests use unique collection, queue, and key names, so a provider may share one database
/// across tests.
/// </remarks>
public abstract class StorageConformanceTests : IAsyncLifetime
{
    protected static readonly DateTimeOffset Start = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    protected FakeTimeProvider Time { get; } = new(Start);

    protected IStorageProvider Provider { get; private set; } = null!;

    /// <summary>
    /// Creates the provider under test. It must read the current time from <paramref name="timeProvider"/>.
    /// </summary>
    protected abstract ValueTask<IStorageProvider> CreateProviderAsync(TimeProvider timeProvider);

    public virtual async ValueTask InitializeAsync()
    {
        Provider = await CreateProviderAsync(Time);
    }

    public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;

    protected static string Unique(string prefix) => $"{prefix}-{Guid.NewGuid():N}";

    protected static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    protected static string Text(ReadOnlyMemory<byte> bytes) => Encoding.UTF8.GetString(bytes.Span);

    protected static Task<T[]> RunConcurrentlyAsync<T>(int count, Func<int, Task<T>> action) =>
        Task.WhenAll(Enumerable.Range(0, count).Select(i => Task.Run(() => action(i))));
}
