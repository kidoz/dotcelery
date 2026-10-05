using DotCelery.Backend.InMemory.Storage;
using DotCelery.Client;
using DotCelery.Client.Canvas;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Batches;
using DotCelery.Core.Canvas;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Unit.Canvas;

public sealed class CanvasClientTests : IAsyncDisposable
{
    private readonly RecordingBroker _broker = new();
    private readonly JsonMessageSerializer _serializer = new();
    private readonly BatchStore _batchStore = new(new InMemoryStorageProvider());

    [Fact]
    public async Task SendChainAsync_PublishesTheFirstStepWithTheRest()
    {
        var client = CreateClient();
        var chain = new Chain(
            new Signature { TaskName = "first.task" },
            new Signature { TaskName = "second.task" },
            new Signature { TaskName = "third.task" }
        );

        var result = await client.SendChainAsync(chain);

        // The chain travels with the first step, so no scheduler is needed to continue it
        var first = Assert.Single(_broker.Published);
        Assert.Equal(result.FirstTaskId, first.Id);
        Assert.Equal("first.task", first.Task);
        Assert.Equal(result.Id, first.RootId);
        Assert.Equal(result.TaskIds, [first.Id, .. first.Chain!.Select(step => step.TaskId)]);
        Assert.Equal(
            ["second.task", "third.task"],
            first.Chain!.Select(step => step.Signature.TaskName)
        );
        Assert.Equal(result.LastTaskId, first.Chain![^1].TaskId);
    }

    [Fact]
    public async Task SendChainAsync_WithATypedSignature_SendsItsSerializedInput()
    {
        var client = CreateClient();
        var chain = new Chain(
            new Signature<TestTask, TestInput> { Input = new TestInput { Value = 7 } },
            new Signature { TaskName = "second.task" }
        );

        await client.SendChainAsync(chain);

        var first = Assert.Single(_broker.Published);
        Assert.Equal(7, _serializer.Deserialize<TestInput>(first.Args)!.Value);
    }

    [Fact]
    public async Task SendGroupAsync_PublishesEverySignatureInParallel()
    {
        var client = CreateClient();
        var group = new Group(
            new Signature { TaskName = "first.task" },
            new Signature { TaskName = "second.task" }
        );

        var result = await client.SendGroupAsync(group);

        Assert.Equal(2, result.Count);
        Assert.Equal(result.TaskIds, _broker.Published.Select(message => message.Id));
        Assert.All(_broker.Published, message => Assert.Equal(result.Id, message.RootId));
        Assert.All(_broker.Published, message => Assert.Null(message.Chain));
    }

    [Fact]
    public async Task SendGroupAsync_WithATypedSignature_SendsItsSerializedInput()
    {
        var client = CreateClient();
        var group = new Group(
            new Signature<TestTask, TestInput> { Input = new TestInput { Value = 7 } }
        );

        await client.SendGroupAsync(group);

        var member = Assert.Single(_broker.Published);
        Assert.Equal(7, _serializer.Deserialize<TestInput>(member.Args)!.Value);
    }

    [Fact]
    public async Task SendChordAsync_WithTypedHeaderSignatures_SendsTheirSerializedInput()
    {
        var client = CreateClient();
        var chord = new Group(
            new Signature<TestTask, TestInput> { Input = new TestInput { Value = 7 } }
        ).WithCallback(new Signature { TaskName = "callback.task" });

        await client.SendChordAsync(chord);

        var member = Assert.Single(_broker.Published);
        Assert.Equal(7, _serializer.Deserialize<TestInput>(member.Args)!.Value);
    }

    [Fact]
    public async Task SendGroupAsync_WithANestedCanvas_IsRefused()
    {
        var client = CreateClient();
        var group = new Group(
            new Chain(
                new Signature { TaskName = "first.task" },
                new Signature { TaskName = "second.task" }
            )
        );

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await client.SendGroupAsync(group)
        );
    }

    [Fact]
    public async Task SendChordAsync_TracksTheHeaderAndRunsTheCallbackWhenItFinishes()
    {
        var client = CreateClient();
        var chord = new Group(
            new Signature { TaskName = "first.task" },
            new Signature { TaskName = "second.task" }
        ).WithCallback(new Signature { TaskName = "callback.task", Args = "{}"u8.ToArray() });

        var result = await client.SendChordAsync(chord);

        var batch = await _batchStore.GetAsync(result.Id);
        Assert.NotNull(batch);
        Assert.Equal(result.Header.TaskIds, batch.TaskIds);
        Assert.NotNull(batch.Callback);
        Assert.Equal("callback.task", batch.Callback.TaskName);
        Assert.Equal(result.CallbackTaskId, batch.Callback.TaskId);
        Assert.Equal("celery", batch.Callback.Queue);

        Assert.Equal(result.Header.TaskIds, _broker.Published.Select(message => message.Id));
        Assert.All(_broker.Published, message => Assert.Equal(result.Id, message.BatchId));
    }

    [Fact]
    public async Task SendChordAsync_WithoutABatchStore_IsRefused()
    {
        var client = CreateClient(withBatchStore: false);
        var chord = new Group(new Signature { TaskName = "first.task" }).WithCallback(
            new Signature { TaskName = "callback.task" }
        );

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await client.SendChordAsync(chord)
        );
    }

    public async ValueTask DisposeAsync()
    {
        await _broker.DisposeAsync();
        await _batchStore.DisposeAsync();
    }

    private CanvasClient CreateClient(bool withBatchStore = true) =>
        new(
            _broker,
            _serializer,
            Options.Create(new CeleryClientOptions()),
            NullLogger<CanvasClient>.Instance,
            withBatchStore ? _batchStore : null
        );

    private sealed class TestTask : ITask<TestInput>
    {
        public static string TaskName => "tests.typed";

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

    private sealed class RecordingBroker : IMessageBroker
    {
        public List<TaskMessage> Published { get; } = [];

        public ValueTask PublishAsync(
            TaskMessage message,
            CancellationToken cancellationToken = default
        )
        {
            Published.Add(message);
            return ValueTask.CompletedTask;
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
}
