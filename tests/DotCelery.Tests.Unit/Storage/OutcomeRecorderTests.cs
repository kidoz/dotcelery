using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Models;
using DotCelery.Core.Storage;
using DotCelery.Core.Storage.Stores;

namespace DotCelery.Tests.Unit.Storage;

public sealed class OutcomeRecorderTests
{
    private readonly InMemoryStorageProvider _storage = new();
    private readonly ResultBackend _results;
    private readonly InboxStore _inbox;

    public OutcomeRecorderTests()
    {
        _results = new ResultBackend(_storage);
        _inbox = new InboxStore(_storage);
    }

    [Fact]
    public void Create_WithoutAnInboxStore_ReturnsNull() =>
        Assert.Null(OutcomeRecorder.Create(_results, inbox: null));

    [Fact]
    public async Task RecordAsync_SuccessfulResult_StoresTheResultAndMarksTheMessage()
    {
        var recorder = OutcomeRecorder.Create(_results, _inbox)!;

        await recorder.RecordAsync(Success("a"));

        Assert.Equal(TaskState.Success, (await _results.GetResultAsync("a"))!.State);
        Assert.True(await _inbox.IsProcessedAsync("a"));
    }

    [Fact]
    public async Task RecordAsync_FailedResult_StoresTheResultAndLeavesTheMessageUnprocessed()
    {
        var recorder = OutcomeRecorder.Create(_results, _inbox)!;

        await recorder.RecordAsync(Failure("a"));

        Assert.Equal(TaskState.Failure, (await _results.GetResultAsync("a"))!.State);
        Assert.False(await _inbox.IsProcessedAsync("a"));
    }

    [Fact]
    public async Task RecordAsync_WhenTheStoresShareATransactionalStorage_RecordsBothInOneTransaction()
    {
        var storage = new TransactionalStorage(_storage);
        var results = new ResultBackend(storage);
        var inbox = new InboxStore(storage);
        var recorder = OutcomeRecorder.Create(results, inbox)!;

        await recorder.RecordAsync(Success("a"));

        Assert.Equal(1, storage.Transactions);
        Assert.Equal(TaskState.Success, (await results.GetResultAsync("a"))!.State);
        Assert.True(await inbox.IsProcessedAsync("a"));
    }

    [Fact]
    public async Task RecordAsync_WhenTheStoresDoNotShareAStorage_DoesNotStartATransaction()
    {
        var transactional = new TransactionalStorage(_storage);
        var results = new ResultBackend(transactional);
        var inbox = new InboxStore(_storage);
        var recorder = OutcomeRecorder.Create(results, inbox)!;

        await recorder.RecordAsync(Success("a"));

        Assert.Equal(0, transactional.Transactions);
        Assert.True(await inbox.IsProcessedAsync("a"));
    }

    private static TaskResult Success(string taskId) => Result(taskId, TaskState.Success);

    private static TaskResult Failure(string taskId) => Result(taskId, TaskState.Failure);

    private static TaskResult Result(string taskId, TaskState state) =>
        new()
        {
            TaskId = taskId,
            State = state,
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromMilliseconds(1),
        };

    private sealed class TransactionalStorage(IStorageProvider inner)
        : IStorageProvider,
            ITransactionalStorage
    {
        public int Transactions { get; private set; }

        public string Name => inner.Name;

        public IDocumentStore Documents => inner.Documents;

        public ILeaseStore Leases => inner.Leases;

        public IQueueStore Queues => inner.Queues;

        public ICounterStore Counters => inner.Counters;

        public INotificationChannel? Notifications => inner.Notifications;

        public bool CanWriteIn(object transaction) => false;

        public ValueTask WriteInAsync(
            object transaction,
            Func<CancellationToken, ValueTask> work,
            CancellationToken cancellationToken = default
        ) => throw new NotSupportedException();

        public async ValueTask RunInTransactionAsync(
            Func<CancellationToken, ValueTask> work,
            CancellationToken cancellationToken = default
        )
        {
            Transactions++;
            await work(cancellationToken);
        }

        public ValueTask<long> PurgeExpiredAsync(CancellationToken cancellationToken = default) =>
            inner.PurgeExpiredAsync(cancellationToken);

        public ValueTask<bool> IsHealthyAsync(CancellationToken cancellationToken = default) =>
            inner.IsHealthyAsync(cancellationToken);
    }
}
