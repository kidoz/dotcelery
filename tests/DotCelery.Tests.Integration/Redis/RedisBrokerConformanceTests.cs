using DotCelery.Broker.Redis;
using DotCelery.Core.Abstractions;
using DotCelery.Tests.Conformance.Brokers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Testcontainers.Redis;

namespace DotCelery.Tests.Integration.Redis;

/// <summary>
/// Runs the broker contract against the Redis Streams broker, so the broker is held to the same
/// delivery behavior as the other brokers.
/// </summary>
public sealed class RedisBrokerFixture : IAsyncLifetime
{
    public RedisContainer Container { get; } = new RedisBuilder("redis:7-alpine").Build();

    public async ValueTask InitializeAsync() => await Container.StartAsync();

    public async ValueTask DisposeAsync() => await Container.DisposeAsync();
}

[CollectionDefinition(Name)]
public sealed class RedisBrokerTestGroup : ICollectionFixture<RedisBrokerFixture>
{
    public const string Name = "RedisBroker";
}

[Collection(RedisBrokerTestGroup.Name)]
public sealed class RedisBrokerConformanceTests(RedisBrokerFixture fixture) : BrokerConformanceTests
{
    protected override ValueTask<IMessageBroker> CreateBrokerAsync() =>
        ValueTask.FromResult<IMessageBroker>(
            new RedisBroker(
                Options.Create(
                    new RedisBrokerOptions
                    {
                        ConnectionString = fixture.Container.GetConnectionString(),

                        // Short claim, block, and pending-check times, so an unsettled message
                        // comes back within the test's budget instead of after minutes
                        ClaimTimeout = TimeSpan.FromSeconds(1),
                        BlockTimeout = TimeSpan.FromMilliseconds(100),
                        PendingCheckInterval = TimeSpan.FromMilliseconds(250),
                    }
                ),
                NullLogger<RedisBroker>.Instance
            )
        );
}
