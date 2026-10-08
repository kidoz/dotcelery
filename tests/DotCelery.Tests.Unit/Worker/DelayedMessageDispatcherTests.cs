using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker;
using DotCelery.Worker.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Unit.Worker;

/// <summary>
/// Runs the delayed message dispatcher against an in-memory store, so due messages are proven
/// to be published with their ETA cleared, future ones to stay stored, and a failed publish
/// to be kept for another try.
/// </summary>
public sealed class DelayedMessageDispatcherTests : IAsyncDisposable
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(10);

    private readonly InMemoryStorageProvider _storage = new();
    private readonly RecordingBroker _broker = new();
    private readonly DelayedMessageStore _store;
    private readonly JsonMessageSerializer _serializer = new();

    public DelayedMessageDispatcherTests()
    {
        _store = new DelayedMessageStore(_storage);
    }

    [Fact]
    public async Task ADueMessage_IsPublishedWithItsEtaCleared()
    {
        await _store.AddAsync(CreateMessage("t1"), DateTimeOffset.UtcNow.AddMinutes(-1));

        var dispatcher = CreateDispatcher();
        await dispatcher.StartAsync(CancellationToken.None);
        await WaitForAsync(() => _broker.PublishedCount == 1);
        await dispatcher.StopAsync(CancellationToken.None);

        var published = Assert.Single(_broker.Published);
        Assert.Equal("t1", published.Id);
        Assert.Null(published.Eta);
        Assert.Equal(0, await _store.GetPendingCountAsync());
    }

    [Fact]
    public async Task AFutureMessage_StaysInTheStore()
    {
        await _store.AddAsync(CreateMessage("t1"), DateTimeOffset.UtcNow.AddHours(1));

        var dispatcher = CreateDispatcher();
        await dispatcher.StartAsync(CancellationToken.None);

        // Several poll intervals pass without the message becoming due
        await Task.Delay(150);
        await dispatcher.StopAsync(CancellationToken.None);

        Assert.Equal(0, _broker.PublishedCount);
        Assert.Equal(1, await _store.GetPendingCountAsync());
    }

    [Fact]
    public async Task AFailedPublish_KeepsTheMessageAndRetries()
    {
        await _store.AddAsync(CreateMessage("t1"), DateTimeOffset.UtcNow.AddMinutes(-1));
        _broker.FailPublish = true;

        var dispatcher = CreateDispatcher();
        await dispatcher.StartAsync(CancellationToken.None);
        await WaitForAsync(() => _broker.FailedAttempts >= 1);

        _broker.FailPublish = false;
        await WaitForAsync(() => _broker.PublishedCount == 1);
        await dispatcher.StopAsync(CancellationToken.None);

        Assert.Equal(0, await _store.GetPendingCountAsync());
    }

    [Fact]
    public async Task ADisabledDispatcher_DoesNotPublish()
    {
        await _store.AddAsync(CreateMessage("t1"), DateTimeOffset.UtcNow.AddMinutes(-1));

        var dispatcher = CreateDispatcher(enabled: false);
        await dispatcher.StartAsync(CancellationToken.None);
        await dispatcher.ExecuteTask!.WaitAsync(WaitTimeout);

        Assert.Equal(0, _broker.PublishedCount);
        Assert.Equal(1, await _store.GetPendingCountAsync());
    }

    public async ValueTask DisposeAsync()
    {
        await _store.DisposeAsync();
        await _broker.DisposeAsync();
    }

    private DelayedMessageDispatcher CreateDispatcher(bool enabled = true) =>
        new(
            _store,
            _broker,
            Options.Create(
                new WorkerOptions
                {
                    UseDelayQueue = enabled,
                    DelayedMessagePollInterval = TimeSpan.FromMilliseconds(20),
                    DelayedMessageRetryInterval = TimeSpan.FromMilliseconds(50),
                }
            ),
            NullLogger<DelayedMessageDispatcher>.Instance
        );

    private TaskMessage CreateMessage(string taskId) =>
        new()
        {
            Id = taskId,
            Task = "tests.delayed",
            Args = _serializer.Serialize(new TestInput { Value = 1 }),
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Eta = DateTimeOffset.UtcNow.AddHours(1),
            Queue = "celery",
        };

    private static async Task WaitForAsync(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(WaitTimeout);
        while (!condition())
        {
            await Task.Delay(10, timeout.Token);
        }
    }

    public sealed class TestInput
    {
        public int Value { get; init; }
    }

    /// <summary>
    /// Broker that records what the dispatcher publishes and can be made to fail.
    /// </summary>
    private sealed class RecordingBroker : IMessageBroker
    {
        private readonly ConcurrentQueue<TaskMessage> _published = new();

        private int _failedAttempts;
        private int _publishedCount;

        public bool FailPublish { get; set; }

        public int FailedAttempts => _failedAttempts;

        public int PublishedCount => _publishedCount;

        public IReadOnlyCollection<TaskMessage> Published => _published;

        public ValueTask PublishAsync(
            TaskMessage message,
            CancellationToken cancellationToken = default
        )
        {
            if (FailPublish)
            {
                Interlocked.Increment(ref _failedAttempts);
                return ValueTask.FromException(new InvalidOperationException("Broker unavailable"));
            }

            _published.Enqueue(message);
            Interlocked.Increment(ref _publishedCount);
            return ValueTask.CompletedTask;
        }

        public async IAsyncEnumerable<BrokerMessage> ConsumeAsync(
            IReadOnlyList<string> queues,
            [EnumeratorCancellation] CancellationToken cancellationToken = default
        )
        {
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
            yield break;
        }

        public ValueTask AckAsync(
            BrokerMessage message,
            CancellationToken cancellationToken = default
        ) => ValueTask.CompletedTask;

        public ValueTask RejectAsync(
            BrokerMessage message,
            bool requeue = false,
            CancellationToken cancellationToken = default
        ) => ValueTask.CompletedTask;

        public ValueTask<bool> IsHealthyAsync(CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(true);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
