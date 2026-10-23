using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Signals;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Unit.Signals;

public sealed class SignalQueueProcessorTests
{
    [Theory]
    [InlineData("missing.type", "{}")]
    [InlineData("System.String", "{}")]
    [InlineData(null, "bad json")]
    public async Task PoisonSignal_IsArchivedAndTheNextSignalIsHandled(
        string? signalType,
        string payload
    )
    {
        var storage = new InMemoryStorageProvider();
        await using var store = new SignalStore(storage);
        var valid = new TaskSuccessSignal
        {
            TaskId = "task",
            TaskName = "test",
            Timestamp = DateTimeOffset.UtcNow,
            Duration = TimeSpan.Zero,
        };
        var signal = SignalMessage.Create(valid);
        await store.EnqueueAsync(
            signal with
            {
                Id = "poison",
                SignalType = signalType ?? signal.SignalType,
                Payload = payload,
            }
        );
        var publisher = new QueuedTaskSignalDispatcher(
            store,
            NullLogger<QueuedTaskSignalDispatcher>.Instance
        );
        await publisher.DispatchAsync(valid);
        var handler = new RecordingHandler();
        var services = new ServiceCollection();
        services.AddSingleton<ITaskSignalHandler<TaskSuccessSignal>>(handler);
        await using var provider = services.BuildServiceProvider();
        using var processor = new SignalQueueProcessor(
            store,
            provider,
            Options.Create(
                new SignalQueueProcessorOptions
                {
                    PollInterval = TimeSpan.FromMilliseconds(10),
                    ParallelProcessing = false,
                }
            ),
            NullLogger<SignalQueueProcessor>.Instance
        );
        await processor.StartAsync(CancellationToken.None);
        try
        {
            await handler.Handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            while (await store.GetPendingCountAsync(timeout.Token) > 0)
                await Task.Delay(10, timeout.Token);
            Assert.NotNull(await storage.Documents.GetAsync("dotcelery.signals.failed", "poison"));
            Assert.False(processor.ExecuteTask!.IsCompleted);
            Assert.Equal(1, handler.Count);
        }
        finally
        {
            await processor.StopAsync(CancellationToken.None);
        }
    }

    private sealed class RecordingHandler : ITaskSignalHandler<TaskSuccessSignal>
    {
        public TaskCompletionSource Handled { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Count { get; private set; }

        public ValueTask HandleAsync(
            TaskSuccessSignal signal,
            CancellationToken cancellationToken = default
        )
        {
            Count++;
            Handled.TrySetResult();
            return ValueTask.CompletedTask;
        }
    }
}
