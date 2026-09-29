using System.Globalization;

namespace DotCelery.Backend.Redis.Storage;

/// <summary>
/// Times are kept in Redis as microseconds since the Unix epoch, which scripts compare as
/// numbers and sorted sets use as exact scores.
/// </summary>
internal static class RedisTime
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

    public static string Format(long microseconds) =>
        microseconds.ToString(CultureInfo.InvariantCulture);
}
