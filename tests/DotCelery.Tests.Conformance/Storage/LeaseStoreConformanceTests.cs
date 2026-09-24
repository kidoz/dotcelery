using DotCelery.Core.Storage;

namespace DotCelery.Tests.Conformance.Storage;

/// <summary>
/// Conformance tests for <see cref="ILeaseStore"/>.
/// </summary>
public abstract class LeaseStoreConformanceTests : StorageConformanceTests
{
    private static readonly TimeSpan Duration = TimeSpan.FromSeconds(30);

    private readonly string _prefix = Unique("lease");

    private ILeaseStore Leases => Provider.Leases;

    [Fact]
    public async Task TryAcquireAsync_FreeKey_GrantsLease()
    {
        var lease = await Leases.TryAcquireAsync(Key("a"), "owner-1", Duration);

        Assert.NotNull(lease);
        Assert.Equal(Key("a"), lease.Key);
        Assert.Equal("owner-1", lease.Owner);
        Assert.Equal(Start + Duration, lease.ExpiresAt);
        Assert.Equal(lease, await Leases.GetAsync(Key("a")));
    }

    [Fact]
    public async Task TryAcquireAsync_HeldByAnotherOwner_ReturnsNull()
    {
        await Leases.TryAcquireAsync(Key("a"), "owner-1", Duration);

        Assert.Null(await Leases.TryAcquireAsync(Key("a"), "owner-2", Duration));
    }

    [Fact]
    public async Task TryAcquireAsync_SameOwner_ExtendsAndKeepsToken()
    {
        var first = await Leases.TryAcquireAsync(Key("a"), "owner-1", Duration);
        Time.Advance(TimeSpan.FromSeconds(10));

        var second = await Leases.TryAcquireAsync(Key("a"), "owner-1", Duration);

        Assert.NotNull(second);
        Assert.Equal(first!.Token, second.Token);
        Assert.Equal(Start.AddSeconds(10) + Duration, second.ExpiresAt);
    }

    [Fact]
    public async Task TryAcquireAsync_ExpiredLease_GrantsNewerToken()
    {
        var first = await Leases.TryAcquireAsync(Key("a"), "owner-1", Duration);
        Time.Advance(Duration);

        var second = await Leases.TryAcquireAsync(Key("a"), "owner-2", Duration);

        Assert.NotNull(second);
        Assert.True(second.Token > first!.Token);
    }

    [Fact]
    public async Task RenewAsync_HeldLease_ExtendsIt()
    {
        var lease = await Leases.TryAcquireAsync(Key("a"), "owner-1", Duration);
        Time.Advance(TimeSpan.FromSeconds(10));

        var renewed = await Leases.RenewAsync(lease!, Duration);

        Assert.NotNull(renewed);
        Assert.Equal(lease!.Token, renewed.Token);
        Assert.Equal(Start.AddSeconds(10) + Duration, renewed.ExpiresAt);
    }

    [Fact]
    public async Task RenewAsync_ExpiredLease_ReturnsNull()
    {
        var lease = await Leases.TryAcquireAsync(Key("a"), "owner-1", Duration);
        Time.Advance(Duration);

        Assert.Null(await Leases.RenewAsync(lease!, Duration));
    }

    [Fact]
    public async Task RenewAsync_TakenOverLease_ReturnsNull()
    {
        var lease = await Leases.TryAcquireAsync(Key("a"), "owner-1", Duration);
        Time.Advance(Duration);
        var newHolder = await Leases.TryAcquireAsync(Key("a"), "owner-2", Duration);

        Assert.Null(await Leases.RenewAsync(lease!, Duration));
        Assert.Equal(newHolder, await Leases.GetAsync(Key("a")));
    }

    [Fact]
    public async Task ReleaseAsync_HeldLease_FreesTheKey()
    {
        var lease = await Leases.TryAcquireAsync(Key("a"), "owner-1", Duration);

        Assert.True(await Leases.ReleaseAsync(lease!));

        Assert.Null(await Leases.GetAsync(Key("a")));
        Assert.NotNull(await Leases.TryAcquireAsync(Key("a"), "owner-2", Duration));
    }

    [Fact]
    public async Task ReleaseAsync_TakenOverLease_KeepsTheNewHolder()
    {
        var lease = await Leases.TryAcquireAsync(Key("a"), "owner-1", Duration);
        Time.Advance(Duration);
        var newHolder = await Leases.TryAcquireAsync(Key("a"), "owner-2", Duration);

        Assert.False(await Leases.ReleaseAsync(lease!));
        Assert.Equal(newHolder, await Leases.GetAsync(Key("a")));
    }

    [Fact]
    public async Task GetAsync_ExpiredLease_ReturnsNull()
    {
        await Leases.TryAcquireAsync(Key("a"), "owner-1", Duration);
        Time.Advance(Duration);

        Assert.Null(await Leases.GetAsync(Key("a")));
    }

    [Fact]
    public async Task ListAsync_ReturnsCurrentLeasesWithPrefixInKeyOrder()
    {
        await Leases.TryAcquireAsync(Key("b"), "owner-1", TimeSpan.FromMinutes(1));
        await Leases.TryAcquireAsync(Key("a"), "owner-2", TimeSpan.FromMinutes(1));
        await Leases.TryAcquireAsync(Key("c"), "owner-3", TimeSpan.FromSeconds(10));
        await Leases.TryAcquireAsync(Unique("other"), "owner-4", TimeSpan.FromMinutes(1));

        Time.Advance(TimeSpan.FromSeconds(10));

        var keys = await Leases.ListAsync(_prefix + "/").Select(l => l.Key).ToListAsync();
        Assert.Equal([Key("a"), Key("b")], keys);
    }

    [Fact]
    public async Task TryAcquireAsync_Concurrent_OnlyOneOwnerWins()
    {
        var results = await RunConcurrentlyAsync(
            20,
            async i => await Leases.TryAcquireAsync(Key("a"), $"owner-{i}", Duration)
        );

        Assert.Single(results, r => r is not null);
    }

    private string Key(string name) => $"{_prefix}/{name}";
}
