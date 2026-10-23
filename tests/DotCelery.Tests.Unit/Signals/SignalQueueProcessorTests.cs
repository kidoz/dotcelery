using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Signals;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

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

    [Fact]
    public async Task FailedEnqueue_PropagatesBeforeTheCallerCheckpointsCompletion()
    {
        var store = Substitute.For<ISignalStore>();
#pragma warning disable CA2012 // NSubstitute consumes the configured ValueTask.
        store
            .EnqueueAsync(Arg.Any<SignalMessage>(), Arg.Any<CancellationToken>())
            .Returns(_ => ValueTask.FromException(new IOException("queue unavailable")));
#pragma warning restore CA2012
        var dispatcher = new QueuedTaskSignalDispatcher(
            store,
            NullLogger<QueuedTaskSignalDispatcher>.Instance
        );
        await Assert.ThrowsAsync<IOException>(() =>
            dispatcher
                .DispatchAsync(
                    new TaskSuccessSignal
                    {
                        TaskId = "task",
                        TaskName = "test",
                        Timestamp = DateTimeOffset.UtcNow,
                        Duration = TimeSpan.Zero,
                    }
                )
                .AsTask()
        );
    }

    [Fact]
    public async Task CriticalHandlerFailure_RetriesTheSameDurableSignal()
    {
        await using var store = new SignalStore(new InMemoryStorageProvider());
        var publisher = new QueuedTaskSignalDispatcher(
            store,
            NullLogger<QueuedTaskSignalDispatcher>.Instance
        );
        await publisher.DispatchAsync(
            new TaskSuccessSignal
            {
                TaskId = "task",
                TaskName = "test",
                Timestamp = DateTimeOffset.UtcNow,
                Duration = TimeSpan.Zero,
                Result = "retained-result",
            }
        );
        var handler = new RecoveringHandler();
        await using var services = new ServiceCollection()
            .AddSingleton<ITaskSignalHandler<TaskSuccessSignal>>(handler)
            .BuildServiceProvider();
        using var processor = new SignalQueueProcessor(
            store,
            services,
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
            Assert.Equal(2, handler.Calls);
        }
        finally
        {
            await processor.StopAsync(CancellationToken.None);
        }
    }

    private sealed class RecoveringHandler
        : ITaskSignalHandler<TaskSuccessSignal>,
            ICriticalTaskSignalHandler
    {
        public int Calls { get; private set; }
        public TaskCompletionSource Handled { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask HandleAsync(
            TaskSuccessSignal signal,
            CancellationToken cancellationToken = default
        )
        {
            Assert.Equal("task", signal.TaskId);
            Assert.Equal("retained-result", signal.Result?.ToString());
            if (++Calls == 1)
                throw new IOException("workflow publish unavailable");
            Handled.TrySetResult();
            return ValueTask.CompletedTask;
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
