namespace DotCelery.Backend.Mongo.Storage;

/// <summary>
/// Times are kept as 64-bit microseconds since the Unix epoch, which keep more precision than
/// BSON dates and sort as numbers.
/// </summary>
internal static class MongoTime
{
    public static long ToMicroseconds(DateTimeOffset time) =>
        (time.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) / TimeSpan.TicksPerMicrosecond;

    public static long ToMicroseconds(TimeSpan duration) =>
        duration.Ticks / TimeSpan.TicksPerMicrosecond;

    public static DateTimeOffset FromMicroseconds(long microseconds) =>
        new(
            DateTimeOffset.UnixEpoch.UtcTicks + (microseconds * TimeSpan.TicksPerMicrosecond),
            TimeSpan.Zero
        );
}
