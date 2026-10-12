using DotCelery.Broker.InMemory;
using DotCelery.Core.Abstractions;
using DotCelery.Tests.Conformance.Brokers;

namespace DotCelery.Tests.Unit.Broker;

/// <summary>
/// Runs the broker contract against the in-memory broker, so the tests that use it exercise
/// the same delivery behavior the networked brokers provide.
/// </summary>
public sealed class InMemoryBrokerConformanceTests : BrokerConformanceTests
{
    protected override ValueTask<IMessageBroker> CreateBrokerAsync() =>
        ValueTask.FromResult<IMessageBroker>(new InMemoryBroker());
}
