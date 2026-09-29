using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;

namespace DotCelery.Tests.Conformance.Stores;

/// <summary>
/// A broker that records published messages, or fails to publish when <see cref="Fail"/> is set.
/// </summary>
public sealed class RecordingBroker : IMessageBroker
{
    private readonly List<TaskMessage> _published = [];

    public bool Fail { get; set; }

    public IReadOnlyList<TaskMessage> Published
    {
        get
        {
            lock (_published)
            {
                return [.. _published];
            }
        }
    }

    public ValueTask PublishAsync(
        TaskMessage message,
        CancellationToken cancellationToken = default
    )
    {
        if (Fail)
        {
            throw new InvalidOperationException("Broker unavailable");
        }

        lock (_published)
        {
            _published.Add(message);
        }

        return ValueTask.CompletedTask;
    }

    public IAsyncEnumerable<BrokerMessage> ConsumeAsync(
        IReadOnlyList<string> queues,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();

    public ValueTask AckAsync(
        BrokerMessage message,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();

    public ValueTask RejectAsync(
        BrokerMessage message,
        bool requeue = false,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();

    public ValueTask<bool> IsHealthyAsync(CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(true);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
