using DotCelery.Backend.InMemory.Storage;
using DotCelery.Broker.InMemory;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Batches;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using DotCelery.Core.Signals;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker.Batches;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;

namespace DotCelery.Tests.Unit.Batches;

public sealed class BatchCompletionHandlerTests : IAsyncDisposable
{
    private static readonly DateTimeOffset Start = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly BatchStore _store = new(
        new InMemoryStorageProvider(new FakeTimeProvider(Start))
    );
    private readonly InMemoryBroker _broker = new();
    private readonly JsonMessageSerializer _serializer = new();

    [Fact]
    public async Task LastTaskCompleting_PublishesTheCallbackOnce()
    {
        await CreateBatchAsync("batch-1", "task-1", "task-2");
        var handler = CreateHandler();

        await handler.HandleAsync(Success("task-1"), CancellationToken.None);
        Assert.Equal(0, _broker.GetQueueLength("celery"));

        await handler.HandleAsync(Success("task-2"), CancellationToken.None);

        var callback = Assert.Single(await ReadAsync("celery"));
        Assert.Equal("batch.callback", callback.Task);
        Assert.Equal("batch-1", callback.BatchId);
        Assert.Equal("{}"u8.ToArray(), callback.Args);
    }

    [Fact]
    public async Task LastTasksSettlingTogether_PublishTheCallbackOnce()
    {
        await CreateBatchAsync("batch-1", "task-1", "task-2");
        var handler = CreateHandler();

        // Both workers settle the last two tasks at the same time
        await Task.WhenAll(
            Task.Run(() => handler.HandleAsync(Success("task-1"), CancellationToken.None).AsTask()),
            Task.Run(() => handler.HandleAsync(Success("task-2"), CancellationToken.None).AsTask())
        );

        Assert.Single(await ReadAsync("celery"));
    }

    [Fact]
    public async Task LastTaskFailing_PublishesTheCallback()
    {
        await CreateBatchAsync("batch-1", "task-1");
        var handler = CreateHandler();

        await handler.HandleAsync(Failure("task-1"), CancellationToken.None);

        var callback = Assert.Single(await ReadAsync("celery"));
        Assert.Equal("batch.callback", callback.Task);
    }

    [Fact]
    public async Task TaskOfAnotherBatch_IsIgnored()
    {
        await CreateBatchAsync("batch-1", "task-1");
        var handler = CreateHandler();

        await handler.HandleAsync(Success("other-task"), CancellationToken.None);

        Assert.Equal(0, _broker.GetQueueLength("celery"));
    }

    [Fact]
    public async Task WhenTheCallbackCannotBePublished_TheClaimIsReleased()
    {
        await CreateBatchAsync("batch-1", "task-1");
        var broker = new FailingBroker();
        var handler = CreateHandler(broker);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await handler.HandleAsync(Success("task-1"), CancellationToken.None)
        );

        // The claimed callback can be dispatched by the next completion
        var batch = await _store.GetAsync("batch-1");
        Assert.Null(batch!.CallbackDispatchedAt);
    }

    public async ValueTask DisposeAsync()
    {
        await _broker.DisposeAsync();
        await _store.DisposeAsync();
    }

    private BatchCompletionHandler CreateHandler(IMessageBroker? broker = null) =>
        new(
            _store,
            broker ?? _broker,
            _serializer,
            NullLogger<BatchCompletionHandler>.Instance,
            new FakeTimeProvider(Start)
        );

    private async Task CreateBatchAsync(string batchId, params string[] taskIds)
    {
        var batch = new Batch
        {
            Id = batchId,
            State = BatchState.Pending,
            TaskIds = taskIds,
            CreatedAt = Start,
            Callback = new BatchCallback
            {
                TaskName = "batch.callback",
                Args = "{}"u8.ToArray(),
                ContentType = "application/json",
                Queue = "celery",
            },
        };

        await _store.CreateAsync(batch);
    }

    private async Task<List<TaskMessage>> ReadAsync(string queue)
    {
        var messages = new List<TaskMessage>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await foreach (var message in _broker.ConsumeAsync([queue], cts.Token))
        {
            messages.Add(message.Message);
            await _broker.AckAsync(message, cts.Token);
        }

        return messages;
    }

    private static TaskSuccessSignal Success(string taskId) =>
        new()
        {
            TaskId = taskId,
            TaskName = "tests.task",
            Timestamp = Start,
            Duration = TimeSpan.Zero,
        };

    private static TaskFailureSignal Failure(string taskId) =>
        new()
        {
            TaskId = taskId,
            TaskName = "tests.task",
            Timestamp = Start,
            Duration = TimeSpan.Zero,
            Exception = new InvalidOperationException("boom"),
        };

    private sealed class FailingBroker : IMessageBroker
    {
        public ValueTask PublishAsync(
            TaskMessage message,
            CancellationToken cancellationToken = default
        ) => throw new InvalidOperationException("The broker is unavailable");

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
            ValueTask.FromResult(false);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
