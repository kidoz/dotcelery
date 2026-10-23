using Docker.DotNet;
using Docker.DotNet.Models;
using DotCelery.Broker.RabbitMQ;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.RabbitMq;

namespace DotCelery.Tests.Integration.Chaos;

/// <summary>
/// Chaos experiment against a real broker: the broker is killed while a consumer holds a message
/// unsettled, and the consumer must reconnect and receive the message again after the broker
/// comes back. It proves the delivery guarantee a worker relies on — an unacknowledged message is
/// not lost when the broker restarts.
/// </summary>
[Collection("RabbitMQ")]
public sealed class RabbitMQRestartChaosTests : IAsyncLifetime
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(60);

    private readonly ITestOutputHelper _output;
    private readonly RabbitMqContainer _container = new RabbitMqBuilder(
        "rabbitmq:3.13-management-alpine"
    ).Build();

    private RabbitMQBroker? _broker;

    public RabbitMQRestartChaosTests(ITestOutputHelper output)
    {
        _output = output;
    }

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();

        var loggerFactory = LoggerFactory.Create(builder =>
            builder.AddProvider(new XunitLoggerProvider(_output)).SetMinimumLevel(LogLevel.Debug)
        );

        _broker = new RabbitMQBroker(
            Options.Create(
                new RabbitMQBrokerOptions
                {
                    ConnectionString = _container.GetConnectionString(),
                    PrefetchCount = 1,
                    ConnectionRetryCount = 1,
                    ConnectionRetryDelay = TimeSpan.FromSeconds(1),
                    ReconnectDelay = TimeSpan.FromMilliseconds(250),
                }
            ),
            loggerFactory.CreateLogger<RabbitMQBroker>()
        );
    }

    public async ValueTask DisposeAsync()
    {
        if (_broker is not null)
        {
            await _broker.DisposeAsync();
        }

        await _container.DisposeAsync();
    }

    [Fact]
    public async Task ABrokerRestart_RedeliversAnUnsettledMessageAndTheConsumerRecovers()
    {
        var queue = $"chaos-{Guid.NewGuid():N}";
        var message = new TaskMessage
        {
            Id = Guid.NewGuid().ToString("N"),
            Task = "chaos.task",
            Args = "{}"u8.ToArray(),
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Queue = queue,
        };

        await _broker!.PublishAsync(message);

        var connectionBefore = _container.GetConnectionString();
        var deliveries = new List<BrokerMessage>();
        using var consumerCts = new CancellationTokenSource(WaitTimeout);
        var consumer = Task.Run(async () =>
        {
            await foreach (var delivery in _broker.ConsumeAsync([queue], consumerCts.Token))
            {
                lock (deliveries)
                {
                    deliveries.Add(delivery);
                }
            }
        });

        await WaitUntilAsync(
            () => Count(deliveries) >= 1,
            "the published message should be delivered"
        );

        // Kill the broker process while the delivery is unsettled. The container, its port
        // mapping, and its filesystem stay in place, so the same broker must reconnect and the
        // message must come back.
        using (var docker = new DockerClientBuilder().Build())
        {
            _output.WriteLine($"Restarting container {_container.Id}");
            await docker.Containers.RestartContainerAsync(
                _container.Id,
                new ContainerRestartParameters { WaitBeforeKillSeconds = 0 }
            );

            for (var i = 0; i < 5; i++)
            {
                var inspect = await docker.Containers.InspectContainerAsync(_container.Id);
                _output.WriteLine(
                    $"State after restart: running={inspect.State?.Running}, status={inspect.State?.Status}"
                );
                await Task.Delay(1000);
            }
        }

        var connectionAfter = _container.GetConnectionString();
        _output.WriteLine(
            $"Connection before the restart: {connectionBefore}; after: {connectionAfter}"
        );

        await WaitUntilAsync(
            () => Count(deliveries) >= 2 || consumer.IsFaulted,
            "the unsettled message should be redelivered after the broker restarts"
        );
        Assert.False(consumer.IsFaulted, $"The consumer failed: {consumer.Exception}");

        BrokerMessage redelivered;
        lock (deliveries)
        {
            redelivered = deliveries[^1];
        }

        Assert.Equal(message.Id, redelivered.Message.Id);
        await _broker.AckAsync(redelivered, CancellationToken.None);

        await consumerCts.CancelAsync();
        try
        {
            await consumer;
        }
        catch (OperationCanceledException)
        {
            // The consumer stops with the test's token
        }
    }

    private static int Count(List<BrokerMessage> deliveries)
    {
        lock (deliveries)
        {
            return deliveries.Count;
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, string description)
    {
        var deadline = DateTime.UtcNow + WaitTimeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(100);
        }

        Assert.Fail($"Timed out after {WaitTimeout}: {description}.");
    }

    private sealed class XunitLoggerProvider(ITestOutputHelper output) : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => new XunitLogger(output, categoryName);

        public void Dispose() { }

        private sealed class XunitLogger(ITestOutputHelper output, string category) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter
            )
            {
                output.WriteLine(
                    $"{logLevel}: {category}: {formatter(state, exception)}"
                        + (exception is null ? string.Empty : $" {exception}")
                );
            }
        }
    }
}
