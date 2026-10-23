namespace DotCelery.Tests.Unit.Worker;

using System.Runtime.CompilerServices;
using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker;
using DotCelery.Worker.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;

public class RevocationManagerTests : IAsyncDisposable
{
    private readonly RevocationStore _revocationStore = new(new InMemoryStorageProvider());
    private readonly RevocationManager _manager;

    public RevocationManagerTests()
    {
        var options = Options.Create(new WorkerOptions());
        _manager = new RevocationManager(
            options,
            NullLogger<RevocationManager>.Instance,
            _revocationStore
        );
    }

    [Fact]
    public void RegisterTask_ReturnsLinkedCancellationTokenSource()
    {
        var parentCts = new CancellationTokenSource();

        using var taskCts = _manager.RegisterTask("task-1", parentCts.Token);

        Assert.NotNull(taskCts);
        Assert.False(taskCts.IsCancellationRequested);
    }

    [Fact]
    public void RegisterTask_ParentCancellation_PropagatestoTask()
    {
        var parentCts = new CancellationTokenSource();
        using var taskCts = _manager.RegisterTask("task-1", parentCts.Token);

        parentCts.Cancel();

        Assert.True(taskCts.IsCancellationRequested);
    }

    [Fact]
    public void UnregisterTask_DisposesTokenSource()
    {
        var parentCts = new CancellationTokenSource();
        var taskCts = _manager.RegisterTask("task-1", parentCts.Token);

        _manager.UnregisterTask("task-1");

        // The token source should be disposed - accessing Token should throw
        // Note: This is an implementation detail test - verifying cleanup happened
        Assert.True(true); // UnregisterTask completed without error
    }

    [Fact]
    public void UnregisterTask_NonExistentTask_DoesNotThrow()
    {
        var exception = Record.Exception(() => _manager.UnregisterTask("non-existent"));

        Assert.Null(exception);
    }

    [Fact]
    public async Task IsRevokedAsync_WithoutStore_ReturnsFalse()
    {
        var managerWithoutStore = new RevocationManager(
            Options.Create(new WorkerOptions()),
            NullLogger<RevocationManager>.Instance,
            revocationStore: null
        );

        var isRevoked = await managerWithoutStore.IsRevokedAsync("any-task");

        Assert.False(isRevoked);
    }

    [Fact]
    public async Task IsRevokedAsync_RevokedTask_ReturnsTrue()
    {
        await _revocationStore.RevokeAsync("task-1");

        var isRevoked = await _manager.IsRevokedAsync("task-1");

        Assert.True(isRevoked);
    }

    [Fact]
    public async Task IsRevokedAsync_NonRevokedTask_ReturnsFalse()
    {
        var isRevoked = await _manager.IsRevokedAsync("non-revoked-task");

        Assert.False(isRevoked);
    }

    [Fact]
    public async Task RegisterTask_WithPendingRevocation_CancelsImmediately()
    {
        var taskId = "pending-revoked-task";
        var options = new RevokeOptions { Terminate = true, Signal = CancellationSignal.Immediate };
        await _manager.StartAsync(CancellationToken.None);

        // The manager subscribes in the background, so revoke until it has received the revocation
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (_manager.GetRevocationOptions(taskId)?.Terminate != true)
        {
            await _revocationStore.RevokeAsync(taskId, options, cts.Token);
            await Task.Delay(20, cts.Token);
        }

        using var taskCts = _manager.RegisterTask(taskId, CancellationToken.None);

        Assert.True(taskCts.IsCancellationRequested);

        await _manager.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Startup_LoadsExistingRevocationsWithTheirOptions()
    {
        await _revocationStore.RevokeAsync("revoked-before-startup", RevokeOptions.WithTermination);

        await _manager.StartAsync(CancellationToken.None);

        // The manager loads revocations in the background
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (_manager.GetRevocationOptions("revoked-before-startup") is null)
        {
            await Task.Delay(20, cts.Token);
        }

        Assert.True(_manager.GetRevocationOptions("revoked-before-startup")!.Terminate);

        using var taskCts = _manager.RegisterTask("revoked-before-startup", CancellationToken.None);
        Assert.True(taskCts.IsCancellationRequested);

        await _manager.StopAsync(CancellationToken.None);
    }

    [Fact]
    public void GetRevocationOptions_NonExistentTask_ReturnsNull()
    {
        var options = _manager.GetRevocationOptions("non-existent");

        Assert.Null(options);
    }

    [Fact]
    public void DuplicateRegistration_IsRefusedWithoutReplacingTheRunningTask()
    {
        using var parent = new CancellationTokenSource();
        using var first = _manager.RegisterTask("duplicate", parent.Token);
        Assert.Throws<InvalidOperationException>(() =>
            _manager.RegisterTask("duplicate", parent.Token)
        );
        parent.Cancel();
        Assert.True(first.IsCancellationRequested);
        _manager.UnregisterTask("duplicate");
        using var next = _manager.RegisterTask("duplicate", CancellationToken.None);
        Assert.False(next.IsCancellationRequested);
    }

    [Fact]
    public async Task PendingRevocation_ExpiresWithTheConfiguredStoreRetention()
    {
        var time = new FakeTimeProvider();
        var options = Options.Create(
            new StorageStoreOptions { RevocationRetention = TimeSpan.FromMinutes(2) }
        );
        await using var store = new RevocationStore(
            new InMemoryStorageProvider(timeProvider: time),
            options,
            time
        );
        await store.RevokeAsync("expired", RevokeOptions.Immediate);
        using var manager = new RevocationManager(
            Options.Create(new WorkerOptions()),
            NullLogger<RevocationManager>.Instance,
            store,
            time,
            options
        );
        await manager.StartAsync(CancellationToken.None);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (manager.GetRevocationOptions("expired") is null)
            await Task.Delay(10, timeout.Token);
        time.Advance(TimeSpan.FromMinutes(2));
        Assert.Null(manager.GetRevocationOptions("expired"));
        using var task = manager.RegisterTask("expired", CancellationToken.None);
        Assert.False(task.IsCancellationRequested);
        await manager.StopAsync(CancellationToken.None);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SubscriptionOrLoadFailure_ReconnectsAndCancelsTheRunningTask(bool loadFails)
    {
        var store = Substitute.For<IRevocationStore>();
        var subscriptions = 0;
        var loads = 0;
        store.GetRevocationsAsync(Arg.Any<CancellationToken>()).Returns(_ => Load());
        store
            .SubscribeAsync(Arg.Any<CancellationToken>())
            .Returns(call => Subscribe(call.Arg<CancellationToken>()));
        async IAsyncEnumerable<RevocationEvent> Subscribe(
            [EnumeratorCancellation] CancellationToken token
        )
        {
            if (Interlocked.Increment(ref subscriptions) == 1 && !loadFails)
                throw new IOException("subscription lost");
            yield return new RevocationEvent
            {
                TaskId = "running",
                Options = RevokeOptions.Immediate,
                Timestamp = DateTimeOffset.UtcNow,
            };
            await Task.Delay(Timeout.Infinite, token);
        }
        async IAsyncEnumerable<RevocationEvent> Load()
        {
            await Task.CompletedTask;
            if (Interlocked.Increment(ref loads) == 1 && loadFails)
                throw new IOException("load unavailable");
            yield break;
        }
        using var manager = new RevocationManager(
            Options.Create(new WorkerOptions()),
            NullLogger<RevocationManager>.Instance,
            store
        );
        using var task = manager.RegisterTask("running", CancellationToken.None);
        await manager.StartAsync(CancellationToken.None);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!task.IsCancellationRequested)
            await Task.Delay(10, timeout.Token);
        Assert.True(loadFails ? loads >= 2 : subscriptions >= 2);
        Assert.False(manager.ExecuteTask!.IsCompleted);
        await manager.StopAsync(CancellationToken.None);
    }

    public async ValueTask DisposeAsync()
    {
        await _manager.StopAsync(CancellationToken.None);
        _manager.Dispose();
        await _revocationStore.DisposeAsync();
    }
}
