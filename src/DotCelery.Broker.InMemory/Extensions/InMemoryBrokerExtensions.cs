using DotCelery.Core.Extensions;

namespace DotCelery.Broker.InMemory.Extensions;

/// <summary>
/// Extension methods for configuring the in-memory broker.
/// </summary>
public static class InMemoryBrokerExtensions
{
    /// <summary>
    /// Uses the in-memory broker for testing and development.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <returns>The builder.</returns>
    public static DotCeleryBuilder UseInMemoryBroker(this DotCeleryBuilder builder)
    {
        return builder.UseBroker<InMemoryBroker>();
    }
}
