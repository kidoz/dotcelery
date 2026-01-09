using DotCelery.Backend.Redis.Revocation;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.Redis;

namespace DotCelery.Tests.Integration.Redis;

/// <summary>
/// Integration tests for Redis revocation store using Testcontainers.
/// These tests require Docker to be running.
/// </summary>
[Collection("Redis")]
public class RedisRevocationStoreIntegrationTests : IAsyncLifetime
{
    private readonly RedisContainer _container;
    private RedisRevocationStore? _store;

    public RedisRevocationStoreIntegrationTests()
    {
        _container = new RedisBuilder("redis:7-alpine").Build();
    }

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();

        var options = Options.Create(
            new RedisRevocationStoreOptions { ConnectionString = _container.GetConnectionString() }
        );

        var logger = LoggerFactory
            .Create(builder => builder.AddConsole())
            .CreateLogger<RedisRevocationStore>();

        _store = new RedisRevocationStore(options, logger);
    }

    public async ValueTask DisposeAsync()
    {
        if (_store is not null)
        {
            await _store.DisposeAsync();
        }

        await _container.DisposeAsync();
    }

    [Fact]
    public async Task RevokeAsync_StoresRevocation()
    {
        var taskId = Guid.NewGuid().ToString();

        await _store!.RevokeAsync(taskId);

        var isRevoked = await _store.IsRevokedAsync(taskId);
        Assert.True(isRevoked);
    }

    [Fact]
    public async Task IsRevokedAsync_ReturnsFalseForNonRevoked()
    {
        var isRevoked = await _store!.IsRevokedAsync("non-existent-task-id");

        Assert.False(isRevoked);
    }

    [Fact]
    public async Task RevokeAsync_WithOptions_StoresOptions()
    {
        var taskId = Guid.NewGuid().ToString();
        var options = new RevokeOptions { Terminate = true, Signal = CancellationSignal.Immediate };

        await _store!.RevokeAsync(taskId, options);

        var isRevoked = await _store.IsRevokedAsync(taskId);
        Assert.True(isRevoked);
    }

    [Fact]
    public async Task RevokeAsync_WithExpiry_RevocationExpires()
    {
        var taskId = Guid.NewGuid().ToString();
        var options = new RevokeOptions { Expiry = TimeSpan.FromMilliseconds(100) };

        await _store!.RevokeAsync(taskId, options);

        // Should be revoked initially
        var isRevokedBefore = await _store.IsRevokedAsync(taskId);
        Assert.True(isRevokedBefore);

        // Wait for expiry
        await Task.Delay(200);

        // Should not be revoked after expiry
        var isRevokedAfter = await _store.IsRevokedAsync(taskId);
        Assert.False(isRevokedAfter);
    }

    [Fact]
    public async Task RevokeAsync_MultipleTasks_AllRevoked()
    {
        var taskIds = Enumerable.Range(0, 5).Select(_ => Guid.NewGuid().ToString()).ToList();

        await _store!.RevokeAsync(taskIds);

        foreach (var taskId in taskIds)
        {
            var isRevoked = await _store.IsRevokedAsync(taskId);
            Assert.True(isRevoked);
        }
    }

    [Fact]
    public async Task GetRevokedTaskIdsAsync_ReturnsAllRevokedTasks()
    {
        var taskIds = Enumerable.Range(0, 5).Select(_ => Guid.NewGuid().ToString()).ToList();

        foreach (var taskId in taskIds)
        {
            await _store!.RevokeAsync(taskId);
        }

        var revokedIds = new List<string>();
        await foreach (var id in _store!.GetRevokedTaskIdsAsync())
        {
            revokedIds.Add(id);
        }

        Assert.Equal(5, revokedIds.Count);
        Assert.All(taskIds, id => Assert.Contains(id, revokedIds));
    }

    [Fact]
    public async Task GetRevokedTaskIdsAsync_ExcludesExpiredRevocations()
    {
        var expiredTaskId = Guid.NewGuid().ToString();
        var validTaskId = Guid.NewGuid().ToString();

        // Add an expired revocation
        await _store!.RevokeAsync(
            expiredTaskId,
            new RevokeOptions { Expiry = TimeSpan.FromMilliseconds(50) }
        );

        // Add a valid revocation
        await _store.RevokeAsync(validTaskId);

        // Wait for first one to expire
        await Task.Delay(100);

        var revokedIds = new List<string>();
        await foreach (var id in _store.GetRevokedTaskIdsAsync())
        {
            revokedIds.Add(id);
        }

        Assert.Single(revokedIds);
        Assert.Equal(validTaskId, revokedIds[0]);
    }

    [Fact]
    public async Task CleanupAsync_RemovesOldRevocations()
    {
        var taskId = Guid.NewGuid().ToString();
        await _store!.RevokeAsync(taskId);

        // Wait a bit
        await Task.Delay(100);

        // Cleanup with very short max age
        var removed = await _store.CleanupAsync(TimeSpan.FromMilliseconds(50));

        Assert.True(removed > 0);
        var isRevoked = await _store.IsRevokedAsync(taskId);
        Assert.False(isRevoked);
    }

    [Fact]
    public async Task SubscribeAsync_ReceivesRevocationEvents()
    {
        var taskId = Guid.NewGuid().ToString();
        RevocationEvent? receivedEvent = null;

        // Start subscription in background
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var subscriptionTask = Task.Run(
            async () =>
            {
                await foreach (var evt in _store!.SubscribeAsync(cts.Token))
                {
                    receivedEvent = evt;
                    break;
                }
            },
            cts.Token
        );

        // Give subscription time to start
        await Task.Delay(200);

        // Revoke a task
        await _store!.RevokeAsync(taskId, RevokeOptions.WithTermination);

        // Wait for subscription to receive the event
        try
        {
            await subscriptionTask.WaitAsync(TimeSpan.FromSeconds(2));
        }
        catch (TimeoutException)
        {
            // Acceptable if event wasn't received in time
        }

        Assert.NotNull(receivedEvent);
        Assert.Equal(taskId, receivedEvent.TaskId);
        Assert.True(receivedEvent.Options.Terminate);
    }

    [Fact]
    public async Task ConcurrentRevocations_AllSucceed()
    {
        var tasks = Enumerable
            .Range(0, 20)
            .Select(async i =>
            {
                var taskId = Guid.NewGuid().ToString();
                await _store!.RevokeAsync(taskId);
                return taskId;
            });

        var taskIds = await Task.WhenAll(tasks);

        foreach (var taskId in taskIds)
        {
            var isRevoked = await _store!.IsRevokedAsync(taskId);
            Assert.True(isRevoked);
        }
    }

    [Fact]
    public async Task RevokeAsync_SameTaskTwice_DoesNotDuplicate()
    {
        var taskId = Guid.NewGuid().ToString();

        await _store!.RevokeAsync(taskId);
        await _store.RevokeAsync(taskId);

        var revokedIds = new List<string>();
        await foreach (var id in _store.GetRevokedTaskIdsAsync())
        {
            revokedIds.Add(id);
        }

        Assert.Single(revokedIds);
        Assert.Equal(taskId, revokedIds[0]);
    }

    [Fact]
    public async Task TwoStores_ShareRevocations()
    {
        // Create a second store pointing to the same Redis
        var options = Options.Create(
            new RedisRevocationStoreOptions { ConnectionString = _container.GetConnectionString() }
        );
        var logger = LoggerFactory
            .Create(builder => builder.AddConsole())
            .CreateLogger<RedisRevocationStore>();
        await using var store2 = new RedisRevocationStore(options, logger);

        var taskId = Guid.NewGuid().ToString();

        // Revoke using first store
        await _store!.RevokeAsync(taskId);

        // Check using second store
        var isRevoked = await store2.IsRevokedAsync(taskId);
        Assert.True(isRevoked);
    }
}
