using DotCelery.Broker.RabbitMQ;
using DotCelery.Core.Abstractions;
using DotCelery.Tests.Conformance.Brokers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Testcontainers.RabbitMq;

namespace DotCelery.Tests.Integration.RabbitMQ;

/// <summary>
/// Runs the broker contract against RabbitMQ, so the broker is held to the same delivery
/// behavior as the other brokers.
/// </summary>
public sealed class RabbitMqBrokerFixture : IAsyncLifetime
{
    public RabbitMqContainer Container { get; } =
        new RabbitMqBuilder("rabbitmq:3.13-management-alpine").Build();

    public async ValueTask InitializeAsync() => await Container.StartAsync();

    public async ValueTask DisposeAsync() => await Container.DisposeAsync();
}

[CollectionDefinition(Name)]
public sealed class RabbitMqBrokerTestGroup : ICollectionFixture<RabbitMqBrokerFixture>
{
    public const string Name = "RabbitMQBroker";
}

[Collection(RabbitMqBrokerTestGroup.Name)]
public sealed class RabbitMQBrokerConformanceTests(RabbitMqBrokerFixture fixture)
    : BrokerConformanceTests
{
    protected override ValueTask<IMessageBroker> CreateBrokerAsync() =>
        ValueTask.FromResult<IMessageBroker>(
            new RabbitMQBroker(
                Options.Create(
                    new RabbitMQBrokerOptions
                    {
                        ConnectionString = fixture.Container.GetConnectionString(),
                        ConnectionRetryCount = 1,
                        ConnectionRetryDelay = TimeSpan.FromSeconds(1),
                        ReconnectDelay = TimeSpan.FromSeconds(1),
                    }
                ),
                NullLogger<RabbitMQBroker>.Instance
            )
        );
}
