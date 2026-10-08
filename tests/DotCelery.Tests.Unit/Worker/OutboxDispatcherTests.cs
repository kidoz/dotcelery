using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Outbox;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Unit.Worker;

/// <summary>
/// Runs the outbox dispatcher against an in-memory store, so claiming, publishing, and
/// retrying a failed publish are proven by their effects on the store and the broker.
/// </summary>
public sealed class OutboxDispatcherTests : IAsyncDisposable
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(10);

    private readonly InMemoryStorageProvider _storage = new();
    private readonly RecordingBroker _broker = new();
    private readonly OutboxStore _store;
    private readonly JsonMessageSerializer _serializer = new();

    public OutboxDispatcherTests()
    {
        _store = new OutboxStore(
            _storage,
            Options.Create(
                new StorageStoreOptions { OutboxRetryDelay = TimeSpan.FromMilliseconds(20) }
            )
        );
    }

    [Fact]
    public async Task PendingMessages_ArePublishedAndRemoved()
    {
        await StoreAsync("m1", "t1");
        await StoreAsync("m2", "t2");

        var dispatcher = CreateDispatcher();
        await dispatcher.StartAsync(CancellationToken.None);
        await WaitForAsync(() => _broker.PublishedCount == 2);
        await dispatcher.StopAsync(CancellationToken.None);

        Assert.Equal(0, await _store.GetPendingCountAsync());
        Assert.Contains(_broker.Published, message => message.Id == "t1");
        Assert.Contains(_broker.Published, message => message.Id == "t2");
    }

    [Fact]
    public async Task TwoDispatchers_DoNotPublishTheSameMessageTwice()
    {
        await StoreAsync("m1", "t1");

        var first = CreateDispatcher();
        var second = CreateDispatcher();
        await first.StartAsync(CancellationToken.None);
        await second.StartAsync(CancellationToken.None);
        await WaitForAsync(() => _broker.PublishedCount == 1);

        // Long enough that a second dispatch of the same message would have happened
        await Task.Delay(100);
        await first.StopAsync(CancellationToken.None);
        await second.StopAsync(CancellationToken.None);

        Assert.Equal(1, _broker.PublishedCount);
        Assert.Equal(0, await _store.GetPendingCountAsync());
    }

    [Fact]
    public async Task AFailedPublish_IsRetriedAndDelivered()
    {
        await StoreAsync("m1", "t1");
        _broker.FailPublish = true;

        var dispatcher = CreateDispatcher();
        await dispatcher.StartAsync(CancellationToken.None);
        await WaitForAsync(() => _broker.FailedAttempts >= 1);

        // The message is kept for another try
        Assert.Equal(1, await _store.GetPendingCountAsync());

        _broker.FailPublish = false;
        await WaitForAsync(() => _broker.PublishedCount == 1);
        await dispatcher.StopAsync(CancellationToken.None);

        Assert.Equal(0, await _store.GetPendingCountAsync());
    }

    [Fact]
    public async Task ADisabledDispatcher_DoesNotPublish()
    {
        await StoreAsync("m1", "t1");

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

    private OutboxDispatcher CreateDispatcher(bool enabled = true) =>
        new(
            _store,
            _broker,
            Options.Create(
                new OutboxOptions
                {
                    Enabled = enabled,
                    DispatchInterval = TimeSpan.FromMilliseconds(20),
                    BatchSize = 10,
                }
            ),
            NullLogger<OutboxDispatcher>.Instance
        );

    private ValueTask StoreAsync(string messageId, string taskId) =>
        _store.StoreAsync(
            new OutboxMessage
            {
                Id = messageId,
                TaskMessage = CreateMessage(taskId),
                CreatedAt = DateTimeOffset.UtcNow,
            }
        );

    private TaskMessage CreateMessage(string taskId) =>
        new()
        {
            Id = taskId,
            Task = "tests.outbox",
            Args = _serializer.Serialize(new TestInput { Value = 1 }),
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
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
