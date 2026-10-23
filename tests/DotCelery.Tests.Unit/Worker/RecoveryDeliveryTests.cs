using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Canvas;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using DotCelery.Core.Signals;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Filters;
using DotCelery.Worker.Registry;
using DotCelery.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;

namespace DotCelery.Tests.Unit.Worker;

public sealed class RecoveryDeliveryTests : IAsyncDisposable
{
    private readonly FakeTimeProvider _time = new();
    private readonly ResultBackend _results;
    private readonly InboxStore _inbox;
    private readonly ServiceProvider _services;
    private readonly Counter _counter = new();
    private readonly CriticalHandler _handler = new();
    private readonly TaskExecutor _executor;
    private readonly RevocationManager _revocations;

    public RecoveryDeliveryTests()
    {
        var storage = new InMemoryStorageProvider(_time);
        _results = new ResultBackend(storage, timeProvider: _time);
        _inbox = new InboxStore(storage, timeProvider: _time);
        _services = new ServiceCollection()
            .AddLogging()
            .AddSingleton(_counter)
            .AddSingleton<IInboxStore>(_inbox)
            .AddTransient<InboxDeduplicationFilter>()
            .AddSingleton<ITaskSignalHandler<TaskSuccessSignal>>(_handler)
            .AddTransient<EchoTask>()
            .BuildServiceProvider();
        var options = Options.Create(
            new WorkerOptions { EnableRevocation = false, EnableRateLimiting = false }
        );
        _revocations = new RevocationManager(options, NullLogger<RevocationManager>.Instance);
        var registry = new TaskRegistry();
        registry.Register(typeof(EchoTask), EchoTask.TaskName);
        var filters = new TaskFilterOptions();
        filters.GlobalFilterTypes.Add(typeof(InboxDeduplicationFilter));
        _executor = new TaskExecutor(
            registry,
            _services,
            new JsonMessageSerializer(),
            _results,
            _revocations,
            new TaskFilterPipeline(
                _services,
                Options.Create(filters),
                NullLogger<TaskFilterPipeline>.Instance
            ),
            options,
            NullLogger<TaskExecutor>.Instance,
            signalDispatcher: new TaskSignalDispatcher(
                _services,
                NullLogger<TaskSignalDispatcher>.Instance
            ),
            inboxStore: _inbox,
            timeProvider: _time
        );
    }

    [Fact]
    public async Task CriticalSignalFailure_ReplaysTheSignalWithoutRepeatingTheBody()
    {
        _handler.Fail = true;
        var delivery = Delivery("signal");
        await Assert.ThrowsAsync<IOException>(() =>
            _executor.ExecuteAsync(delivery, "worker", CancellationToken.None)
        );
        var recorded = await _results.GetResultAsync("signal");
        Assert.Equal(TaskState.Success, recorded!.State);
        Assert.False(recorded.SignalsPublished);
        Assert.True(await _inbox.IsProcessedAsync("signal"));
        _handler.Fail = false;

        var recovered = await _executor.ExecuteAsync(delivery, "recovery", CancellationToken.None);

        Assert.True(recovered.IsDuplicate);
        Assert.True(recovered.SignalsPublished);
        Assert.Equal(recorded.Result, recovered.Result);
        Assert.Equal(recorded.CompletedAt, recovered.CompletedAt);
        Assert.Equal(1, _counter.Calls);
        Assert.Equal(2, _handler.Calls);
    }

    [Theory]
    [InlineData("plain")]
    [InlineData("chain")]
    [InlineData("link")]
    public async Task ExpiredResult_UsesTheRetainedInboxOutcomeWithoutRerunningTheBody(
        string continuation
    )
    {
        var delivery = Delivery("retained", continuation);
        var original = await _executor.ExecuteAsync(delivery, "worker", CancellationToken.None);
        _time.Advance(TimeSpan.FromDays(2));
        Assert.Null(await _results.GetResultAsync("retained"));

        var duplicate = await _executor.ExecuteAsync(delivery, "recovery", CancellationToken.None);

        Assert.True(duplicate.IsDuplicate);
        Assert.Equal(original.Result, duplicate.Result);
        Assert.Equal(original.CompletedAt, duplicate.CompletedAt);
        Assert.Equal(original.FollowUpsPublished, duplicate.FollowUpsPublished);
        Assert.True(duplicate.SignalsPublished);
        Assert.Equal(1, _counter.Calls);
        Assert.Equal(1, _handler.Calls);
    }

    [Fact]
    public async Task LegacyMarkerWithoutRecoveryData_IsRejectedRatherThanRetriedAsInfrastructureFailure()
    {
        await _inbox.MarkProcessedAsync("legacy");
        var result = await _executor.ExecuteAsync(
            Delivery("legacy", "chain"),
            "recovery",
            CancellationToken.None
        );
        Assert.Equal(TaskState.Rejected, result.State);
        Assert.Equal(TaskState.Rejected, (await _results.GetResultAsync("legacy"))!.State);
        Assert.Equal(0, _counter.Calls);
    }

    private BrokerMessage Delivery(string id, string continuation = "plain") =>
        new()
        {
            DeliveryTag = id,
            Queue = "celery",
            ReceivedAt = _time.GetUtcNow(),
            Message = new TaskMessage
            {
                Id = id,
                Task = EchoTask.TaskName,
                Args = "{}"u8.ToArray(),
                ContentType = "application/json",
                Timestamp = _time.GetUtcNow(),
                Chain =
                    continuation == "chain"
                        ?
                        [
                            new ChainStep
                            {
                                TaskId = "next",
                                Signature = new Signature { TaskName = EchoTask.TaskName },
                            },
                        ]
                        : null,
                Link =
                    continuation == "link" ? new Signature { TaskName = EchoTask.TaskName } : null,
            },
        };

    public async ValueTask DisposeAsync()
    {
        _revocations.Dispose();
        await _services.DisposeAsync();
        await _results.DisposeAsync();
        await _inbox.DisposeAsync();
    }

    private sealed class Counter
    {
        public int Calls { get; set; }
    }

    public sealed class Input;

    private sealed class EchoTask(Counter counter) : ITask<Input, Input>
    {
        public static string TaskName => "recovery.echo";

        public Task<Input> ExecuteAsync(
            Input input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            counter.Calls++;
            return Task.FromResult(input);
        }
    }

    private sealed class CriticalHandler
        : ITaskSignalHandler<TaskSuccessSignal>,
            ICriticalTaskSignalHandler
    {
        public bool Fail { get; set; }
        public int Calls { get; private set; }

        public ValueTask HandleAsync(
            TaskSuccessSignal signal,
            CancellationToken cancellationToken = default
        )
        {
            Calls++;
            if (Fail)
                throw new IOException("Callback publication failed");
            return ValueTask.CompletedTask;
        }
    }
}
