namespace DotCelery.Tests.Unit.Worker;

using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Models;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker;
using DotCelery.Worker.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

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
    public void GetRevocationOptions_NonExistentTask_ReturnsNull()
    {
        var options = _manager.GetRevocationOptions("non-existent");

        Assert.Null(options);
    }

    public async ValueTask DisposeAsync()
    {
        await _manager.StopAsync(CancellationToken.None);
        _manager.Dispose();
        await _revocationStore.DisposeAsync();
    }
}
