using DotCelery.Backend.InMemory.Storage;
using DotCelery.Client;
using DotCelery.Client.Batches;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Batches;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace DotCelery.Tests.Unit.Batches;

public sealed class BatchClientTests : IAsyncDisposable
{
    private readonly InMemoryStorageProvider _storage = new();
    private readonly BatchStore _store;
    private readonly JsonMessageSerializer _serializer = new();

    public BatchClientTests()
    {
        _store = new BatchStore(_storage);
    }

    [Fact]
    public async Task CreateBatchAsync_CreatesTheRecordBeforePublishingTheTasks()
    {
        var broker = new RecordingBroker(_store);
        var client = CreateClient(broker);

        await client.CreateBatchAsync(batch =>
        {
            batch.Enqueue<TestTask, TestInput>(new TestInput());
            batch.Enqueue<TestTask, TestInput>(new TestInput());
        });

        // A worker that finishes a task before the record exists would not count it
        Assert.Equal(2, broker.ExistedAtPublish.Count);
        Assert.All(broker.ExistedAtPublish, existed => Assert.True(existed));
    }

    [Fact]
    public async Task CreateBatchAsync_WithACallback_StoresTheCallbackToDispatch()
    {
        var broker = new RecordingBroker(_store);
        var client = CreateClient(broker);

        var batchId = await client.CreateBatchAsync(batch =>
        {
            batch.Enqueue<TestTask, TestInput>(new TestInput());
            batch.OnComplete<TestCallbackTask, TestInput>(new TestInput { Value = 42 });
        });

        var batch = await _store.GetAsync(batchId);
        Assert.NotNull(batch!.Callback);
        Assert.Equal(TestCallbackTask.TaskName, batch.Callback.TaskName);
        Assert.Equal("celery", batch.Callback.Queue);
        Assert.NotNull(batch.Callback.Args);
        Assert.Null(batch.CallbackDispatchedAt);
    }

    [Fact]
    public async Task CreateBatchAsync_WhenAPublishFails_RecordsTheUnpublishedTasksAsFailed()
    {
        var broker = new FailingBroker(_store, failFromCall: 2);
        var client = CreateClient(broker);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await client.CreateBatchAsync(batch =>
            {
                batch.Enqueue<TestTask, TestInput>(new TestInput());
                batch.Enqueue<TestTask, TestInput>(new TestInput());
                batch.Enqueue<TestTask, TestInput>(new TestInput());
            })
        );

        // The tasks that were never published cannot report, so the batch must not wait for them
        var batchId = Assert.Single(broker.Published).BatchId;
        var batch = await _store.GetAsync(batchId!);
        Assert.NotNull(batch);
        Assert.Equal(2, batch.FailedCount);
        Assert.Equal(0, batch.CompletedCount);
    }

    public async ValueTask DisposeAsync() => await _store.DisposeAsync();

    private BatchClient CreateClient(IMessageBroker broker) =>
        new(
            broker,
            _serializer,
            Substitute.For<ICeleryClient>(),
            Options.Create(new CeleryClientOptions()),
            NullLogger<BatchClient>.Instance,
            _store
        );

    private sealed class TestTask : ITask<TestInput>
    {
        public static string TaskName => "tests.task";

        public Task ExecuteAsync(
            TestInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        ) => Task.CompletedTask;
    }

    private sealed class TestCallbackTask : ITask<TestInput>
    {
        public static string TaskName => "tests.callback";

        public Task ExecuteAsync(
            TestInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        ) => Task.CompletedTask;
    }

    private sealed class TestInput
    {
        public int Value { get; init; }
    }

    private class RecordingBroker(IBatchStore store) : IMessageBroker
    {
        public List<bool> ExistedAtPublish { get; } = [];

        public List<TaskMessage> Published { get; } = [];

        public virtual async ValueTask PublishAsync(
            TaskMessage message,
            CancellationToken cancellationToken = default
        )
        {
            ExistedAtPublish.Add(
                message.BatchId is not null
                    && await store.GetAsync(message.BatchId, cancellationToken) is not null
            );
            Published.Add(message);
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

    private sealed class FailingBroker(IBatchStore store, int failFromCall) : RecordingBroker(store)
    {
        private int _calls;

        public override ValueTask PublishAsync(
            TaskMessage message,
            CancellationToken cancellationToken = default
        )
        {
            if (++_calls >= failFromCall)
            {
                throw new InvalidOperationException("The broker is unavailable");
            }

            return base.PublishAsync(message, cancellationToken);
        }
    }
}
