using System.Threading.Channels;

namespace DotCelery.Broker.InMemory;

/// <summary>
/// Options for the in-memory message broker.
/// </summary>
public sealed class InMemoryBrokerOptions
{
    /// <summary>
    /// Gets or sets the maximum capacity per queue.
    /// When null, queues are unbounded. Default is 10000.
    /// </summary>
    public int? MaxQueueCapacity { get; set; } = 10000;

    /// <summary>
    /// Gets or sets the behavior when a bounded queue is full.
    /// Default is <see cref="BoundedChannelFullMode.Wait"/>.
    /// </summary>
    public BoundedChannelFullMode FullMode { get; set; } = BoundedChannelFullMode.Wait;
}
