using System.Runtime.CompilerServices;
using DotCelery.Core.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.Storage;

/// <summary>
/// MongoDB <see cref="INotificationChannel"/> on a capped collection, read with tailable
/// cursors, which works without a replica set.
/// </summary>
/// <remarks>
/// The server stamps each message with a unique, increasing timestamp, so subscribers resume
/// in the order messages were written, whichever process wrote them.
/// </remarks>
internal sealed class MongoNotificationChannel(MongoContext context) : INotificationChannel
{
    private static readonly TimeSpan ReopenDelay = TimeSpan.FromMilliseconds(100);

    private static readonly FilterDefinitionBuilder<BsonDocument> Filter =
        Builders<BsonDocument>.Filter;

    // An empty timestamp in a top-level field is replaced by the server's current timestamp
    public static BsonDocument CreateMessage(string channel, string message) =>
        new()
        {
            { "ts", new BsonTimestamp(0) },
            { "ch", channel },
            { "m", message },
        };

    public async ValueTask PublishAsync(
        string channel,
        string message,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(channel);
        ArgumentNullException.ThrowIfNull(message);

        var notifications = await NotificationsAsync(cancellationToken).ConfigureAwait(false);
        await notifications
            .InsertOneAsync(CreateMessage(channel, message), cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    public async IAsyncEnumerable<string> SubscribeAsync(
        string channel,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(channel);

        var notifications = await NotificationsAsync(cancellationToken).ConfigureAwait(false);
        var newest = await notifications
            .Find(Filter.Empty)
            .Sort(new BsonDocument("$natural", -1))
            .Limit(1)
            .FirstAsync(cancellationToken)
            .ConfigureAwait(false);
        var position = newest["ts"].AsBsonTimestamp;

        var options = new FindOptions<BsonDocument>
        {
            CursorType = CursorType.TailableAwait,
            MaxAwaitTime = TimeSpan.FromSeconds(1),
            NoCursorTimeout = true,
        };

        while (true)
        {
            // Starting at the last message read keeps the cursor from matching nothing, which
            // would end it at once
            using (
                var cursor = await notifications
                    .FindAsync(Filter.Gte("ts", position), options, cancellationToken)
                    .ConfigureAwait(false)
            )
            {
                while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                {
                    foreach (var notification in cursor.Current)
                    {
                        var timestamp = notification["ts"].AsBsonTimestamp;
                        if (timestamp <= position)
                        {
                            continue;
                        }

                        position = timestamp;
                        if (notification["ch"].AsString == channel)
                        {
                            yield return notification["m"].AsString;
                        }
                    }
                }
            }

            // The cursor ended, for example because the collection wrapped around past it
            await Task.Delay(ReopenDelay, cancellationToken).ConfigureAwait(false);
        }
    }

    private Task<IMongoCollection<BsonDocument>> NotificationsAsync(
        CancellationToken cancellationToken
    ) => context.GetCollectionAsync(MongoContext.NotificationsName, cancellationToken);
}
