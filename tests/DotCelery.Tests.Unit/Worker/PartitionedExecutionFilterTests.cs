using System.Threading.Channels;
using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Filters;
using DotCelery.Core.Models;
using DotCelery.Core.Partitioning;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Filters;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;

namespace DotCelery.Tests.Unit.Worker;

public sealed class PartitionedExecutionFilterTests : IAsyncDisposable
{
    private readonly FakeTimeProvider _time = new();
    private readonly ServiceProvider _services = new ServiceCollection().BuildServiceProvider();
    private readonly InMemoryStorageProvider _storage;
    private readonly ResultBackend _results;
    private readonly PartitionLockStore _locks;
    private readonly PartitionedExecutionFilter _filter;
    private readonly Channel<bool> _renewed = Channel.CreateUnbounded<bool>();
    private bool _loseLease;
    private bool _stallRenewal;
    private readonly TaskCompletionSource _renewalStarted = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    public PartitionedExecutionFilterTests()
    {
        _storage = new InMemoryStorageProvider(_time);
        _results = new ResultBackend(_storage);
        _locks = new PartitionLockStore(_storage);
        var store = Substitute.For<IPartitionLockStore>();
#pragma warning disable CA2012 // NSubstitute consumes the configured ValueTask.
        store
            .TryAcquireAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<TimeSpan>(),
                Arg.Any<CancellationToken>()
            )
            .Returns(c =>
                _locks.TryAcquireAsync(
                    c.ArgAt<string>(0),
                    c.ArgAt<string>(1),
                    c.ArgAt<TimeSpan>(2),
                    c.ArgAt<CancellationToken>(3)
                )
            );
        store
            .ReleaseAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(c =>
                _locks.ReleaseAsync(
                    c.ArgAt<string>(0),
                    c.ArgAt<string>(1),
                    c.ArgAt<CancellationToken>(2)
                )
            );
        store
            .ExtendAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<TimeSpan>(),
                Arg.Any<CancellationToken>()
            )
            .Returns(c =>
                RenewAsync(
                    c.ArgAt<string>(0),
                    c.ArgAt<string>(1),
                    c.ArgAt<TimeSpan>(2),
                    c.ArgAt<CancellationToken>(3)
                )
            );
#pragma warning restore CA2012
        _filter = new PartitionedExecutionFilter(
            store,
            Options.Create(new PartitionOptions { LockTimeout = TimeSpan.FromSeconds(3) }),
            NullLogger<PartitionedExecutionFilter>.Instance,
            _time
        );
    }

    private async ValueTask<bool> RenewAsync(
        string key,
        string owner,
        TimeSpan timeout,
        CancellationToken cancellationToken
    )
    {
        _renewalStarted.TrySetResult();
        if (_stallRenewal)
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        var extended =
            !_loseLease && await _locks.ExtendAsync(key, owner, timeout, cancellationToken);
        _renewed.Writer.TryWrite(extended);
        return extended;
    }

    [Fact]
    public async Task LongExecution_RenewsLeaseAndKeepsThePartitionExclusive()
    {
        var first = Context("first");
        await _filter.OnExecutingAsync(first, CancellationToken.None);
        for (var i = 0; i < 7; i++)
        {
            _time.Advance(TimeSpan.FromSeconds(1));
            Assert.True(
                await _renewed.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5))
            );
            // The simulated execution exceeds two original lease lifetimes.
            var contender = Context("contender");
            await _filter.OnExecutingAsync(contender, CancellationToken.None);
            Assert.True(contender.SkipExecution);
            Assert.True(contender.RequeueMessage);
        }
        var completed = Completed(first);
        await _filter.OnExecutedAsync(completed, CancellationToken.None);
        Assert.Null(completed.Exception);
        var next = Context("next");
        await _filter.OnExecutingAsync(next, CancellationToken.None);
        Assert.False(next.SkipExecution);
    }

    [Fact]
    public async Task LeaseLoss_PreventsSuccessfulCompletion()
    {
        var first = Context("first");
        await _filter.OnExecutingAsync(first, CancellationToken.None);
        _loseLease = true;
        _time.Advance(TimeSpan.FromSeconds(1));
        Assert.False(await _renewed.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5)));
        var completed = Completed(first);
        await _filter.OnExecutedAsync(completed, CancellationToken.None);
        Assert.IsType<InvalidOperationException>(completed.Exception);
        Assert.False(completed.Succeeded);
    }

    [Fact]
    public async Task StalledRenewal_PreventsSuccessfulCompletion()
    {
        _stallRenewal = true;
        var first = Context("stalled");
        await _filter.OnExecutingAsync(first, CancellationToken.None);
        _time.Advance(TimeSpan.FromSeconds(1));
        await _renewalStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        _time.Advance(TimeSpan.FromSeconds(1));
        var completed = Completed(first);
        await _filter
            .OnExecutedAsync(completed, CancellationToken.None)
            .AsTask()
            .WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsType<InvalidOperationException>(completed.Exception);
        Assert.IsType<TimeoutException>(completed.Exception.InnerException);
        Assert.False(completed.Succeeded);
    }

    [Fact]
    public async Task ScopeDisposal_ReleasesLeaseWhenALaterFilterSkipsExecution()
    {
        var first = Context("first");
        await _filter.OnExecutingAsync(first, CancellationToken.None);
        await _filter.DisposeAsync();
        Assert.False(await _locks.IsLockedAsync("partition"));
        _time.Advance(TimeSpan.FromMinutes(1));
        Assert.True(_renewed.Reader.TryRead(out _)); // Final ownership check during release.
        Assert.False(_renewed.Reader.TryRead(out _));
    }

    private TaskExecutingContext Context(string id)
    {
        var message = new TaskMessage
        {
            Id = id,
            Task = "partitioned",
            PartitionKey = "partition",
            Args = [],
            ContentType = "application/json",
            Queue = "celery",
            Timestamp = _time.GetUtcNow(),
        };
        return new TaskExecutingContext
        {
            TaskId = id,
            TaskName = message.Task,
            Message = message,
            TaskType = typeof(object),
            TaskContext = new TaskExecutionContext(message, _services, _results),
            ServiceProvider = _services,
        };
    }

    private static TaskExecutedContext Completed(TaskExecutingContext context) =>
        new()
        {
            TaskId = context.TaskId,
            TaskName = context.TaskName,
            Message = context.Message,
            TaskType = context.TaskType,
            TaskContext = context.TaskContext,
            ServiceProvider = context.ServiceProvider,
            Duration = TimeSpan.Zero,
            Properties = context.Properties,
        };

    public async ValueTask DisposeAsync()
    {
        await _filter.DisposeAsync();
        await _results.DisposeAsync();
        await _locks.DisposeAsync();
        await _services.DisposeAsync();
    }
}
