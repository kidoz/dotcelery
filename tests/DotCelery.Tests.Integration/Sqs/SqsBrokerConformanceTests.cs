using DotCelery.Backend.InMemory.Storage;
using DotCelery.Broker.Sqs;
using DotCelery.Client;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Security;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage.Stores;
using DotCelery.Tests.Conformance.Brokers;
using DotCelery.Tests.Conformance.Stores;
using DotCelery.Worker;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Filters;
using DotCelery.Worker.Registry;
using DotCelery.Worker.Services;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Integration.Sqs;

/// <summary>
/// A LocalStack container with the SQS service, so the broker is exercised against the SQS API
/// without an AWS account.
/// </summary>
public sealed class LocalStackFixture : IAsyncLifetime
{
    private const int EdgePort = 4566;

    private const string Image = "localstack/localstack:3";

    public IContainer Container { get; } =
        new ContainerBuilder(Image)
            .WithEnvironment("SERVICES", "sqs")
            .WithPortBinding(EdgePort, assignRandomHostPort: true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Ready."))
            .Build();

    public string ServiceUrl =>
        $"http://{Container.Hostname}:{Container.GetMappedPublicPort(EdgePort)}";

    public async ValueTask InitializeAsync() => await Container.StartAsync();

    public async ValueTask DisposeAsync() => await Container.DisposeAsync();
}

[CollectionDefinition(Name)]
public sealed class SqsBrokerTestGroup : ICollectionFixture<LocalStackFixture>
{
    public const string Name = "SqsBroker";
}

/// <summary>
/// Runs the broker contract against Amazon SQS, so the broker is held to the same delivery
/// behavior as the other brokers.
/// </summary>
[Collection(SqsBrokerTestGroup.Name)]
public sealed class SqsBrokerConformanceTests(LocalStackFixture fixture) : BrokerConformanceTests
{
    protected override ValueTask<IMessageBroker> CreateBrokerAsync() =>
        ValueTask.FromResult<IMessageBroker>(SqsConformanceBroker.Create(fixture));
}

/// <summary>
/// The behavior specific to SQS: a scheduled message is held back by the broker itself, and the
/// broker declares what it guarantees.
/// </summary>
[Collection(SqsBrokerTestGroup.Name)]
public sealed class SqsBrokerIntegrationTests(LocalStackFixture fixture) : IAsyncLifetime
{
    private SqsBroker _broker = null!;

    public ValueTask InitializeAsync()
    {
        _broker = SqsConformanceBroker.Create(fixture);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync() => await _broker.DisposeAsync();

    [Fact]
    public async Task PublishAsync_WithQueueMetrics_RecordsTheEnqueue()
    {
        var metrics = new RecordingQueueMetrics();
        await using var broker = SqsConformanceBroker.Create(fixture, metrics);
        var queue = $"metrics-{Guid.NewGuid():N}";

        await broker.PublishAsync(SqsConformanceBroker.CreateMessage(queue));

        Assert.Equal(1, metrics.CountOf(queue));
    }

    [Fact]
    public async Task PublishAsync_OfAnOversizedMessage_RecordsNothing()
    {
        var metrics = new RecordingQueueMetrics();
        await using var broker = SqsConformanceBroker.Create(fixture, metrics);
        var queue = $"metrics-{Guid.NewGuid():N}";

        // A message over the broker's limit is refused before it reaches the queue
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await broker.PublishAsync(
                SqsConformanceBroker.CreateMessage(queue) with
                {
                    Args = new byte[300_000],
                }
            )
        );

        Assert.Empty(metrics.Enqueued);
    }

    [Fact]
    public void TheBroker_DeclaresItsCapabilities()
    {
        var capabilities = Assert.IsAssignableFrom<IBrokerCapabilities>(_broker).Capabilities;

        Assert.True(capabilities.NativeDelay);
        Assert.True(capabilities.RedeliveryOnConsumerStop);
        Assert.Equal(262_144, capabilities.MaxMessageSizeBytes);
        Assert.False(capabilities.Priority);
    }

    [Fact]
    public async Task AMessageWithAnEta_IsHeldBackByTheBroker()
    {
        var queue = $"scheduled-{Guid.NewGuid():N}";
        var message = new DotCelery.Core.Models.TaskMessage
        {
            Id = Guid.NewGuid().ToString("N"),
            Task = "tests.scheduled",
            Args = "{}"u8.ToArray(),
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Queue = queue,
            Eta = DateTimeOffset.UtcNow.AddSeconds(3),
        };

        await _broker.PublishAsync(message);

        // The broker delays the message itself, so nothing is delivered before the ETA; the
        // consume ends when its token cancels, which is what is expected here
        using (var beforeEta = new CancellationTokenSource(TimeSpan.FromSeconds(1)))
        {
            try
            {
                await foreach (var delivered in _broker.ConsumeAsync([queue], beforeEta.Token))
                {
                    Assert.Fail($"Message {delivered.Message.Id} was delivered before its ETA");
                }
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        }

        using var afterEta = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await foreach (var delivered in _broker.ConsumeAsync([queue], afterEta.Token))
        {
            await _broker.AckAsync(delivered);
            Assert.Equal(message.Id, delivered.Message.Id);
            break;
        }
    }

    [Fact]
    public async Task AWorker_RunsATaskPublishedThroughSqs()
    {
        var queue = $"e2e-{Guid.NewGuid():N}";
        var storage = new InMemoryStorageProvider();
        var backend = new ResultBackend(storage);
        var serializer = new JsonMessageSerializer();

        var services = new ServiceCollection();
        services.AddTransient<ConformanceEchoTask>();
        using var provider = services.BuildServiceProvider();

        var options = Options.Create(
            new WorkerOptions
            {
                Concurrency = 1,
                PrefetchCount = 1,
                Queues = [queue],
                EnableRevocation = false,
                EnableRateLimiting = false,
                ShutdownTimeout = TimeSpan.FromSeconds(15),
            }
        );

        var registry = new TaskRegistry();
        registry.Register(typeof(ConformanceEchoTask), ConformanceEchoTask.TaskName);

        var executor = new TaskExecutor(
            registry,
            provider,
            serializer,
            backend,
            new RevocationManager(options, NullLogger<RevocationManager>.Instance),
            new TaskFilterPipeline(
                provider,
                Options.Create(new TaskFilterOptions()),
                NullLogger<TaskFilterPipeline>.Instance
            ),
            options,
            NullLogger<TaskExecutor>.Instance
        );

        var worker = new CeleryWorkerService(
            _broker,
            executor,
            options,
            NullLogger<CeleryWorkerService>.Instance,
            shutdownHandler: new GracefulShutdownHandler(
                NullLogger<GracefulShutdownHandler>.Instance
            )
        );

        var client = new CeleryClient(
            _broker,
            backend,
            serializer,
            Options.Create(new CeleryClientOptions { DefaultQueue = queue }),
            NullLogger<CeleryClient>.Instance
        );

        try
        {
            // The client publishes through SQS and the worker consumes from it
            var sent = await client.SendAsync<
                ConformanceEchoTask,
                ConformanceInput,
                ConformanceInput
            >(new ConformanceInput { Value = 7 });

            await worker.StartAsync(CancellationToken.None);

            var result = await backend.WaitForResultAsync(sent.TaskId, TimeSpan.FromSeconds(20));

            Assert.Equal(TaskState.Success, result.State);
            var output = serializer.Deserialize<ConformanceInput>(result.Result!)!;
            Assert.Equal(7, output.Value);
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
            worker.Dispose();
            await backend.DisposeAsync();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UnsafeDirectWrite_IsRejectedAndTheNextMessageIsDelivered(bool requireSigning)
    {
        var queue = $"guard-{Guid.NewGuid():N}";
        using var validator = new MessageSecurityValidator(
            Options.Create(
                new MessageSecurityOptions
                {
                    EnableMessageSigning = true,
                    SigningKey = "0123456789abcdef0123456789abcdef"u8.ToArray(),
                }
            ),
            NullLogger<MessageSecurityValidator>.Instance
        );
        await using var receiver = new SqsBroker(
            Options.Create(
                new SqsBrokerOptions
                {
                    ServiceUrl = fixture.ServiceUrl,
                    AccessKey = "test",
                    SecretKey = "test",
                    WaitTime = TimeSpan.FromSeconds(1),
                    MaxMessageSizeBytes = 2048,
                }
            ),
            NullLogger<SqsBroker>.Instance,
            messageSecurityValidator: requireSigning ? validator : null
        );
        var invalid = SqsConformanceBroker.CreateMessage(queue);
        await _broker.PublishAsync(
            requireSigning ? invalid : invalid with { Args = new byte[4096] }
        );
        var valid = SqsConformanceBroker.CreateMessage(queue);
        await receiver.PublishAsync(valid);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await foreach (var delivery in receiver.ConsumeAsync([queue], timeout.Token))
        {
            Assert.Equal(valid.Id, delivery.Message.Id);
            await receiver.AckAsync(delivery, timeout.Token);
            break;
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RequeuedDelivery_IsNotRenewedByItsPreviousConsumeSession(bool renewVisibility)
    {
        var queue = $"requeue-{Guid.NewGuid():N}";
        await using var broker = new SqsBroker(
            Options.Create(
                new SqsBrokerOptions
                {
                    ServiceUrl = fixture.ServiceUrl,
                    AccessKey = "test",
                    SecretKey = "test",
                    WaitTime = TimeSpan.FromSeconds(1),
                    VisibilityTimeout = TimeSpan.FromSeconds(3),
                    RenewVisibilityWhileProcessing = renewVisibility,
                }
            ),
            NullLogger<SqsBroker>.Instance
        );
        await broker.PublishAsync(SqsConformanceBroker.CreateMessage(queue));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(12));
        await using var consumer = broker.ConsumeAsync([queue], timeout.Token).GetAsyncEnumerator();
        Assert.True(await consumer.MoveNextAsync());
        var original = consumer.Current;
        var eta = DateTimeOffset.UtcNow.AddSeconds(4);
        var returned = new BrokerMessage
        {
            Message = original.Message with { Eta = eta },
            DeliveryTag = original.DeliveryTag,
            Queue = original.Queue,
            ReceivedAt = original.ReceivedAt,
        };
        await broker.RejectAsync(returned, requeue: true, timeout.Token);
        Assert.True(await consumer.MoveNextAsync());
        Assert.Equal(original.Message.Id, consumer.Current.Message.Id);
        Assert.True(DateTimeOffset.UtcNow >= eta);
        await broker.AckAsync(consumer.Current, timeout.Token);
    }

    public sealed class ConformanceInput
    {
        public int Value { get; init; }
    }

    public sealed class ConformanceEchoTask : ITask<ConformanceInput, ConformanceInput>
    {
        public static string TaskName => "tests.sqs.echo";

        public Task<ConformanceInput> ExecuteAsync(
            ConformanceInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        ) => Task.FromResult(input);
    }
}

/// <summary>
/// Creates brokers pointed at the LocalStack container with test-friendly times.
/// </summary>
internal static class SqsConformanceBroker
{
    /// <summary>
    /// Creates a message for a queue.
    /// </summary>
    public static TaskMessage CreateMessage(string queue) =>
        new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Task = "tests.metrics",
            Args = "{}"u8.ToArray(),
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Queue = queue,
        };

    public static SqsBroker Create(LocalStackFixture fixture, IQueueMetrics? queueMetrics = null) =>
        new(
            Options.Create(
                new SqsBrokerOptions
                {
                    ServiceUrl = fixture.ServiceUrl,
                    AccessKey = "test",
                    SecretKey = "test",

                    // Short times, so an unsettled message returns within the test's budget
                    VisibilityTimeout = TimeSpan.FromSeconds(2),
                    WaitTime = TimeSpan.FromSeconds(1),
                }
            ),
            NullLogger<SqsBroker>.Instance,
            queueMetrics: queueMetrics
        );
}
