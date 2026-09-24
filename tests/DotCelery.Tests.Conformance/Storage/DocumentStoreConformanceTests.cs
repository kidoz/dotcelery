using DotCelery.Core.Storage;

namespace DotCelery.Tests.Conformance.Storage;

/// <summary>
/// Conformance tests for <see cref="IDocumentStore"/>.
/// </summary>
public abstract class DocumentStoreConformanceTests : StorageConformanceTests
{
    private readonly string _collection = Unique("docs");

    private IDocumentStore Documents => Provider.Documents;

    [Fact]
    public async Task GetAsync_Missing_ReturnsNull()
    {
        Assert.Null(await Documents.GetAsync(_collection, "missing"));
    }

    [Fact]
    public async Task TryInsertAsync_NewDocument_StoresValueAndMetadata()
    {
        var sortKey = Start.AddMinutes(5);

        var version = await Documents.TryInsertAsync(
            _collection,
            "a",
            Bytes("one"),
            new DocumentWriteOptions
            {
                TimeToLive = TimeSpan.FromMinutes(1),
                IndexKey = "state:new",
                SortKey = sortKey,
            }
        );

        var document = await Documents.GetAsync(_collection, "a");
        Assert.NotNull(version);
        Assert.NotNull(document);
        Assert.Equal("a", document.Id);
        Assert.Equal("one", Text(document.Value));
        Assert.Equal(version, document.Version);
        Assert.Equal("state:new", document.IndexKey);
        Assert.Equal(sortKey, document.SortKey);
        Assert.Equal(Start.AddMinutes(1), document.ExpiresAt);
    }

    [Fact]
    public async Task TryInsertAsync_ExistingDocument_ReturnsNullAndKeepsValue()
    {
        await Documents.TryInsertAsync(_collection, "a", Bytes("one"));

        var version = await Documents.TryInsertAsync(_collection, "a", Bytes("two"));

        Assert.Null(version);
        Assert.Equal("one", Text((await Documents.GetAsync(_collection, "a"))!.Value));
    }

    [Fact]
    public async Task TryInsertAsync_ExpiredDocument_Succeeds()
    {
        await Documents.TryInsertAsync(_collection, "a", Bytes("one"), Expiring(1));
        Time.Advance(TimeSpan.FromMinutes(1));

        var version = await Documents.TryInsertAsync(_collection, "a", Bytes("two"));

        Assert.NotNull(version);
        Assert.Equal("two", Text((await Documents.GetAsync(_collection, "a"))!.Value));
    }

    [Fact]
    public async Task GetAsync_ExpiredDocument_ReturnsNull()
    {
        await Documents.TryInsertAsync(_collection, "a", Bytes("one"), Expiring(1));

        Time.Advance(TimeSpan.FromSeconds(59));
        Assert.NotNull(await Documents.GetAsync(_collection, "a"));

        Time.Advance(TimeSpan.FromSeconds(1));
        Assert.Null(await Documents.GetAsync(_collection, "a"));
    }

    [Fact]
    public async Task TryReplaceAsync_CurrentVersion_ReplacesValue()
    {
        var first = await Documents.TryInsertAsync(_collection, "a", Bytes("one"));

        var second = await Documents.TryReplaceAsync(_collection, "a", Bytes("two"), first!.Value);

        var document = await Documents.GetAsync(_collection, "a");
        Assert.NotNull(second);
        Assert.NotEqual(first, second);
        Assert.Equal("two", Text(document!.Value));
        Assert.Equal(second, document.Version);
    }

    [Fact]
    public async Task TryReplaceAsync_StaleVersion_ReturnsNullAndKeepsValue()
    {
        var first = await Documents.TryInsertAsync(_collection, "a", Bytes("one"));
        await Documents.TryReplaceAsync(_collection, "a", Bytes("two"), first!.Value);

        var result = await Documents.TryReplaceAsync(_collection, "a", Bytes("three"), first.Value);

        Assert.Null(result);
        Assert.Equal("two", Text((await Documents.GetAsync(_collection, "a"))!.Value));
    }

    [Fact]
    public async Task TryReplaceAsync_MissingDocument_ReturnsNull()
    {
        Assert.Null(await Documents.TryReplaceAsync(_collection, "missing", Bytes("one"), 1));
    }

    [Fact]
    public async Task TryReplaceAsync_WithoutOptions_ClearsExpiryAndKeys()
    {
        var version = await Documents.TryInsertAsync(
            _collection,
            "a",
            Bytes("one"),
            new DocumentWriteOptions
            {
                TimeToLive = TimeSpan.FromMinutes(1),
                IndexKey = "state:new",
                SortKey = Start,
            }
        );

        await Documents.TryReplaceAsync(_collection, "a", Bytes("two"), version!.Value);
        Time.Advance(TimeSpan.FromMinutes(2));

        var document = await Documents.GetAsync(_collection, "a");
        Assert.NotNull(document);
        Assert.Null(document.IndexKey);
        Assert.Null(document.SortKey);
        Assert.Null(document.ExpiresAt);
    }

    [Fact]
    public async Task UpsertAsync_CreatesThenOverwrites()
    {
        var first = await Documents.UpsertAsync(_collection, "a", Bytes("one"));
        var second = await Documents.UpsertAsync(_collection, "a", Bytes("two"));

        Assert.NotEqual(first, second);
        Assert.Equal("two", Text((await Documents.GetAsync(_collection, "a"))!.Value));
    }

    [Fact]
    public async Task TryInsertAsync_AfterDelete_UsesNewVersion()
    {
        var first = await Documents.TryInsertAsync(_collection, "a", Bytes("one"));
        await Documents.DeleteAsync(_collection, "a");

        var second = await Documents.TryInsertAsync(_collection, "a", Bytes("two"));

        Assert.NotEqual(first, second);
        Assert.Null(
            await Documents.TryReplaceAsync(_collection, "a", Bytes("three"), first!.Value)
        );
    }

    [Fact]
    public async Task DeleteAsync_ExpectedVersion_DeletesOnlyWhenItMatches()
    {
        var version = await Documents.TryInsertAsync(_collection, "a", Bytes("one"));

        Assert.False(await Documents.DeleteAsync(_collection, "a", version + 1));
        Assert.NotNull(await Documents.GetAsync(_collection, "a"));

        Assert.True(await Documents.DeleteAsync(_collection, "a", version));
        Assert.Null(await Documents.GetAsync(_collection, "a"));
    }

    [Fact]
    public async Task DeleteAsync_MissingDocument_ReturnsFalse()
    {
        Assert.False(await Documents.DeleteAsync(_collection, "missing"));
    }

    [Fact]
    public async Task Collections_AreIsolated()
    {
        var other = Unique("docs");
        await Documents.TryInsertAsync(_collection, "a", Bytes("one"));
        await Documents.TryInsertAsync(other, "a", Bytes("two"));

        await Documents.DeleteAsync(other, "a");

        Assert.Equal("one", Text((await Documents.GetAsync(_collection, "a"))!.Value));
    }

    [Fact]
    public async Task QueryAsync_FiltersByIndexKeyAndOrdersBySortKeyThenId()
    {
        await Insert("a", "x", Start.AddMinutes(2));
        await Insert("b", "x", Start.AddMinutes(1));
        await Insert("c", "x", sortKey: null);
        await Insert("e", "x", Start.AddMinutes(1));
        await Insert("d", "y", Start);

        var filter = new DocumentFilter { IndexKey = "x" };

        Assert.Equal(["c", "b", "e", "a"], await QueryIds(filter));
        Assert.Equal(
            ["a", "e", "b", "c"],
            await QueryIds(filter, new DocumentPage { Descending = true })
        );
    }

    [Fact]
    public async Task QueryAsync_SortKeyRange_ExcludesDocumentsWithoutSortKey()
    {
        await Insert("none", "x", sortKey: null);
        await Insert("s0", "x", Start);
        await Insert("s1", "x", Start.AddMinutes(1));
        await Insert("s2", "x", Start.AddMinutes(2));

        Assert.Equal(
            ["s1"],
            await QueryIds(
                new DocumentFilter
                {
                    SortKeyFrom = Start.AddMinutes(1),
                    SortKeyBefore = Start.AddMinutes(2),
                }
            )
        );
        Assert.Equal(
            ["s0", "s1", "s2"],
            await QueryIds(new DocumentFilter { SortKeyFrom = Start })
        );
    }

    [Fact]
    public async Task QueryAsync_Paging_SkipsAndLimits()
    {
        for (var i = 0; i < 5; i++)
        {
            await Insert($"d{i}", "x", Start.AddMinutes(i));
        }

        Assert.Equal(
            ["d1", "d2"],
            await QueryIds(DocumentFilter.All, new DocumentPage { Offset = 1, Limit = 2 })
        );
        Assert.Equal(
            ["d3", "d2"],
            await QueryIds(
                DocumentFilter.All,
                new DocumentPage
                {
                    Descending = true,
                    Offset = 1,
                    Limit = 2,
                }
            )
        );
    }

    [Fact]
    public async Task QueryAsync_SkipsExpiredDocuments()
    {
        await Documents.TryInsertAsync(_collection, "temporary", Bytes("t"), Expiring(1));
        await Documents.TryInsertAsync(_collection, "permanent", Bytes("p"));

        Time.Advance(TimeSpan.FromMinutes(1));

        Assert.Equal(["permanent"], await QueryIds(DocumentFilter.All));
        Assert.Equal(1, await Documents.CountAsync(_collection, DocumentFilter.All));
    }

    [Fact]
    public async Task CountAsyncAndDeleteManyAsync_UseTheFilter()
    {
        await Insert("x0", "x", Start);
        await Insert("x1", "x", Start.AddMinutes(1));
        await Insert("x2", "x", Start.AddMinutes(2));
        await Insert("y0", "y", Start);

        Assert.Equal(4, await Documents.CountAsync(_collection, DocumentFilter.All));
        Assert.Equal(
            3,
            await Documents.CountAsync(_collection, new DocumentFilter { IndexKey = "x" })
        );

        var deleted = await Documents.DeleteManyAsync(
            _collection,
            new DocumentFilter { IndexKey = "x", SortKeyBefore = Start.AddMinutes(2) }
        );

        Assert.Equal(2, deleted);
        Assert.Equal(["y0", "x2"], await QueryIds(DocumentFilter.All));
    }

    [Fact]
    public async Task TryReplaceAsync_Concurrent_OnlyOneWins()
    {
        var version = await Documents.TryInsertAsync(_collection, "a", Bytes("start"));

        var results = await RunConcurrentlyAsync(
            20,
            async i =>
                await Documents.TryReplaceAsync(_collection, "a", Bytes($"w{i}"), version!.Value)
        );

        Assert.Single(results, r => r is not null);
    }

    [Fact]
    public async Task TryInsertAsync_Concurrent_OnlyOneWins()
    {
        var results = await RunConcurrentlyAsync(
            20,
            async i => await Documents.TryInsertAsync(_collection, "a", Bytes($"w{i}"))
        );

        Assert.Single(results, r => r is not null);
    }

    [Fact]
    public async Task TryInsertAsync_StoresACopyOfTheValue()
    {
        var buffer = Bytes("one");
        await Documents.TryInsertAsync(_collection, "a", buffer);

        buffer[0] = (byte)'X';

        Assert.Equal("one", Text((await Documents.GetAsync(_collection, "a"))!.Value));
    }

    [Fact]
    public async Task Values_RoundTripArbitraryBytes()
    {
        var bytes = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray();

        await Documents.UpsertAsync(_collection, "binary", bytes);

        Assert.Equal(bytes, (await Documents.GetAsync(_collection, "binary"))!.Value.ToArray());
    }

    private static DocumentWriteOptions Expiring(int minutes) =>
        new() { TimeToLive = TimeSpan.FromMinutes(minutes) };

    private async Task Insert(string id, string indexKey, DateTimeOffset? sortKey) =>
        await Documents.TryInsertAsync(
            _collection,
            id,
            Bytes(id),
            new DocumentWriteOptions { IndexKey = indexKey, SortKey = sortKey }
        );

    private async Task<List<string>> QueryIds(DocumentFilter filter, DocumentPage? page = null) =>
        await Documents.QueryAsync(_collection, filter, page).Select(d => d.Id).ToListAsync();
}
