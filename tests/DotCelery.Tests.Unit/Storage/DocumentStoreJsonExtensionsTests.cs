namespace DotCelery.Tests.Unit.Storage;

using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage;

public class DocumentStoreJsonExtensionsTests
{
    private readonly IDocumentStore _documents = new InMemoryStorageProvider().Documents;

    [Fact]
    public async Task TypedWrites_RoundTripThroughTheSourceGeneratedContext()
    {
        var typeInfo = DotCeleryJsonContext.Default.TaskMessage;
        var message = CreateMessage("task-1", retries: 0);

        var version = await _documents.TryInsertAsync("messages", message.Id, message, typeInfo);
        var replaced = await _documents.TryReplaceAsync(
            "messages",
            message.Id,
            message with
            {
                Retries = 2,
            },
            version!.Value,
            typeInfo
        );

        var stored = await _documents.GetAsync("messages", message.Id, typeInfo);
        Assert.NotNull(stored);
        Assert.Equal(replaced, stored.Version);
        Assert.Equal("task-1", stored.Value.Id);
        Assert.Equal(2, stored.Value.Retries);
    }

    [Fact]
    public async Task QueryAsync_DeserializesEveryMatch()
    {
        var typeInfo = DotCeleryJsonContext.Default.TaskMessage;
        await _documents.UpsertAsync("messages", "a", CreateMessage("a", 0), typeInfo);
        await _documents.UpsertAsync("messages", "b", CreateMessage("b", 1), typeInfo);

        var ids = await _documents
            .QueryAsync("messages", DocumentFilter.All, typeInfo)
            .Select(d => d.Value.Id)
            .ToListAsync();

        Assert.Equal(["a", "b"], ids);
    }

    [Fact]
    public async Task GetAsync_DocumentContainingNull_Throws()
    {
        await _documents.UpsertAsync("messages", "null", "null"u8.ToArray());

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await _documents.GetAsync("messages", "null", DotCeleryJsonContext.Default.TaskMessage)
        );
    }

    private static TaskMessage CreateMessage(string id, int retries) =>
        new()
        {
            Id = id,
            Task = "test.task",
            Args = [],
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UnixEpoch,
            Retries = retries,
        };
}
