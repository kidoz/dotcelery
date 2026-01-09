using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;

namespace DotCelery.Cron;

/// <summary>
/// Represents a parsed cron expression that can calculate next occurrence times.
/// </summary>
/// <remarks>
/// Supports standard 5-field format (minute hour day month weekday),
/// extended 6-field format with seconds, and 7-field format with year.
/// </remarks>
public sealed class CronExpression
{
    private readonly CronField _second;
    private readonly CronField _minute;
    private readonly CronField _hour;
    private readonly CronDayOfMonthField _dayOfMonth;
    private readonly CronField _month;
    private readonly CronDayOfWeekField _dayOfWeek;
    private readonly CronYearField? _year;
    private readonly string _originalExpression;
    private readonly CronFormat _format;

    // Maximum years to search ahead
    private const int MaxYearsToSearch = 4;

    private CronExpression(
        string originalExpression,
        CronFormat format,
        CronField second,
        CronField minute,
        CronField hour,
        CronDayOfMonthField dayOfMonth,
        CronField month,
        CronDayOfWeekField dayOfWeek,
        CronYearField? year = null
    )
    {
        _originalExpression = originalExpression;
        _format = format;
        _second = second;
        _minute = minute;
        _hour = hour;
        _dayOfMonth = dayOfMonth;
        _month = month;
        _dayOfWeek = dayOfWeek;
        _year = year;
    }

    /// <summary>
    /// Parses a cron expression string.
    /// </summary>
    /// <param name="expression">The cron expression to parse.</param>
    /// <param name="format">The format of the expression (default: Standard 5-field).</param>
    /// <returns>A parsed <see cref="CronExpression"/>.</returns>
    /// <exception cref="ArgumentNullException">When expression is null.</exception>
    /// <exception cref="CronFormatException">When the expression format is invalid.</exception>
    public static CronExpression Parse(string expression, CronFormat format = CronFormat.Standard)
    {
        var parsed = CronParser.Parse(expression, format);
        return new CronExpression(
            expression,
            format,
            parsed.Second,
            parsed.Minute,
            parsed.Hour,
            parsed.DayOfMonth,
            parsed.Month,
            parsed.DayOfWeek,
            parsed.Year
        );
    }

    /// <summary>
    /// Attempts to parse a cron expression string.
    /// </summary>
    /// <param name="expression">The cron expression to parse.</param>
    /// <param name="result">When successful, contains the parsed expression.</param>
    /// <returns>True if parsing succeeded; otherwise, false.</returns>
    public static bool TryParse(string expression, [NotNullWhen(true)] out CronExpression? result)
    {
        return TryParse(expression, CronFormat.Standard, out result);
    }

    /// <summary>
    /// Attempts to parse a cron expression string with specified format.
    /// </summary>
    /// <param name="expression">The cron expression to parse.</param>
    /// <param name="format">The format of the expression.</param>
    /// <param name="result">When successful, contains the parsed expression.</param>
    /// <returns>True if parsing succeeded; otherwise, false.</returns>
    public static bool TryParse(
        string expression,
        CronFormat format,
        [NotNullWhen(true)] out CronExpression? result
    )
    {
        try
        {
            result = Parse(expression, format);
            return true;
        }
        catch (CronFormatException)
        {
            result = null;
            return false;
        }
    }

    /// <summary>
    /// Gets the next occurrence after the specified time.
    /// </summary>
    /// <param name="from">The time to search from (exclusive).</param>
    /// <returns>The next occurrence, or null if none found within search range.</returns>
    public DateTimeOffset? GetNextOccurrence(DateTimeOffset from)
    {
        return GetNextOccurrence(from, TimeZoneInfo.Utc);
    }

    /// <summary>
    /// Gets the next occurrence after the specified time in the specified time zone.
    /// </summary>
    /// <param name="from">The time to search from (exclusive).</param>
    /// <param name="zone">The time zone for calculations.</param>
    /// <returns>The next occurrence, or null if none found within search range.</returns>
    public DateTimeOffset? GetNextOccurrence(DateTimeOffset from, TimeZoneInfo zone)
    {
        ArgumentNullException.ThrowIfNull(zone);

        // Convert to the target timezone for calculations
        var localFrom = TimeZoneInfo.ConvertTime(from, zone);

        // Start from the next second
        var candidate = localFrom.AddSeconds(1);

        // Truncate to the second boundary
        candidate = new DateTimeOffset(
            candidate.Year,
            candidate.Month,
            candidate.Day,
            candidate.Hour,
            candidate.Minute,
            candidate.Second,
            zone.GetUtcOffset(candidate.DateTime)
        );

        var maxDate = localFrom.AddYears(MaxYearsToSearch);

        while (candidate <= maxDate)
        {
            // Check year constraint if present
            if (_year.HasValue && !_year.Value.IsAll && !_year.Value.Contains(candidate.Year))
            {
                var nextYear = _year.Value.GetNext(candidate.Year);
                if (nextYear < 0 || nextYear > 2099)
                {
                    return null;
                }
                candidate = new DateTimeOffset(
                    nextYear,
                    1,
                    1,
                    0,
                    0,
                    0,
                    zone.GetUtcOffset(new DateTime(nextYear, 1, 1))
                );
                continue;
            }

            // Find next valid month
            var monthResult = FindNextMonth(candidate, maxDate);
            if (monthResult is null)
            {
                return null;
            }
            candidate = monthResult.Value;

            // Find next valid day (considering both day-of-month and day-of-week)
            var dayResult = FindNextDay(candidate, zone);
            if (dayResult is null)
            {
                // No valid day in this month, move to next month
                candidate = MoveToNextMonth(candidate, zone);
                continue;
            }
            candidate = dayResult.Value;

            // Find next valid hour
            var hourResult = FindNextHour(candidate, zone);
            if (hourResult is null)
            {
                // No valid hour today, move to next day
                candidate = MoveToNextDay(candidate, zone);
                continue;
            }
            candidate = hourResult.Value;

            // Find next valid minute
            var minuteResult = FindNextMinute(candidate, zone);
            if (minuteResult is null)
            {
                // No valid minute this hour, move to next hour
                candidate = MoveToNextHour(candidate, zone);
                continue;
            }
            candidate = minuteResult.Value;

            // Find next valid second
            var secondResult = FindNextSecond(candidate, zone);
            if (secondResult is null)
            {
                // No valid second this minute, move to next minute
                candidate = MoveToNextMinute(candidate, zone);
                continue;
            }
            candidate = secondResult.Value;

            // Handle DST transitions
            var dstResult = HandleDstTransition(candidate, zone);
            if (dstResult is null)
            {
                // Time falls in DST gap, move forward
                candidate = MoveToNextMinute(candidate, zone);
                continue;
            }

            return dstResult;
        }

        return null;
    }

    /// <summary>
    /// Gets all occurrences between two times.
    /// </summary>
    /// <param name="from">The start time (exclusive).</param>
    /// <param name="to">The end time (inclusive).</param>
    /// <returns>An enumerable of occurrences.</returns>
    public IEnumerable<DateTimeOffset> GetOccurrences(DateTimeOffset from, DateTimeOffset to)
    {
        return GetOccurrences(from, to, TimeZoneInfo.Utc);
    }

    /// <summary>
    /// Gets all occurrences between two times in the specified time zone.
    /// </summary>
    /// <param name="from">The start time (exclusive).</param>
    /// <param name="to">The end time (inclusive).</param>
    /// <param name="zone">The time zone for calculations.</param>
    /// <returns>An enumerable of occurrences.</returns>
    public IEnumerable<DateTimeOffset> GetOccurrences(
        DateTimeOffset from,
        DateTimeOffset to,
        TimeZoneInfo zone
    )
    {
        var current = from;
        while (true)
        {
            var next = GetNextOccurrence(current, zone);
            if (next is null || next.Value > to)
            {
                yield break;
            }

            yield return next.Value;
            current = next.Value;
        }
    }

    /// <summary>
    /// Gets a human-readable description of the cron expression.
    /// </summary>
    /// <returns>A human-readable description.</returns>
    public string ToDescription()
    {
        var sb = new StringBuilder();

        // Handle special patterns first
        if (_originalExpression == "* * * * *" || _originalExpression == "0 * * * * *")
        {
            return "Every minute";
        }
        if (_originalExpression == "0 * * * *")
        {
            return "Every hour";
        }
        if (_originalExpression == "0 0 * * *")
        {
            return "Every day at midnight";
        }
        if (_originalExpression == "0 0 * * 0")
        {
            return "Every Sunday at midnight";
        }
        if (_originalExpression == "0 0 1 * *")
        {
            return "First day of every month at midnight";
        }
        if (_originalExpression == "0 0 1 1 *")
        {
            return "Every January 1st at midnight";
        }

        // Build description from components
        var parts = new List<string>();

        // Time description
        var timeDesc = DescribeTime();
        if (!string.IsNullOrEmpty(timeDesc))
        {
            parts.Add(timeDesc);
        }

        // Day description
        var dayDesc = DescribeDays();
        if (!string.IsNullOrEmpty(dayDesc))
        {
            parts.Add(dayDesc);
        }

        // Month description
        var monthDesc = DescribeMonths();
        if (!string.IsNullOrEmpty(monthDesc))
        {
            parts.Add(monthDesc);
        }

        // Year description
        if (_year.HasValue)
        {
            var yearDesc = DescribeYears();
            if (!string.IsNullOrEmpty(yearDesc))
            {
                parts.Add(yearDesc);
            }
        }

        if (parts.Count == 0)
        {
            return "Every minute";
        }

        return string.Join(" ", parts);
    }

    /// <summary>
    /// Returns the original cron expression string.
    /// </summary>
    public override string ToString()
    {
        return _originalExpression;
    }

    private DateTimeOffset? FindNextMonth(DateTimeOffset candidate, DateTimeOffset maxDate)
    {
        var year = candidate.Year;
        var month = candidate.Month;

        while (candidate <= maxDate)
        {
            var nextMonth = _month.GetNext(month);
            if (nextMonth >= 1 && nextMonth <= 12)
            {
                if (nextMonth > month)
                {
                    // Found a valid month in the current year
                    return new DateTimeOffset(year, nextMonth, 1, 0, 0, 0, candidate.Offset);
                }
                else if (nextMonth == month)
                {
                    // Current month is valid
                    return candidate;
                }
            }

            // No valid month in current year, try next year
            year++;
            month = 1;
            candidate = new DateTimeOffset(year, 1, 1, 0, 0, 0, candidate.Offset);

            if (candidate > maxDate)
            {
                return null;
            }

            // Check year constraint
            if (_year.HasValue && !_year.Value.IsAll && !_year.Value.Contains(year))
            {
                continue;
            }

            // Check if there's a valid month starting from January
            nextMonth = _month.GetNext(1);
            if (nextMonth >= 1 && nextMonth <= 12)
            {
                return new DateTimeOffset(year, nextMonth, 1, 0, 0, 0, candidate.Offset);
            }
        }

        return null;
    }

    private DateTimeOffset? FindNextDay(DateTimeOffset candidate, TimeZoneInfo zone)
    {
        var year = candidate.Year;
        var month = candidate.Month;
        var day = candidate.Day;
        var maxDay = DateTime.DaysInMonth(year, month);

        // Get valid days from both day-of-month and day-of-week fields
        var startDay =
            (candidate.Hour == 0 && candidate.Minute == 0 && candidate.Second == 0) ? day : day;

        for (var d = startDay; d <= maxDay; d++)
        {
            if (IsDayValid(year, month, d))
            {
                if (d == day)
                {
                    return candidate;
                }
                return new DateTimeOffset(
                    year,
                    month,
                    d,
                    0,
                    0,
                    0,
                    zone.GetUtcOffset(new DateTime(year, month, d))
                );
            }
        }

        return null;
    }

    private bool IsDayValid(int year, int month, int day)
    {
        // Check day-of-month constraint
        var domValid = _dayOfMonth.Contains(day, year, month);

        // Check day-of-week constraint
        var date = new DateOnly(year, month, day);
        var dayOfWeek = (int)date.DayOfWeek; // 0 = Sunday
        var dowValid = _dayOfWeek.Contains(dayOfWeek, day, year, month);

        // If both fields have special modifiers or specific values, treat as AND
        // If either is *, treat as OR (which means either must match)
        var domHasWildcard =
            !_dayOfMonth.HasSpecialModifiers && _dayOfMonth.Field.Bits == CronField.All(1, 31).Bits;
        var dowHasWildcard =
            !_dayOfWeek.HasSpecialModifiers && _dayOfWeek.Field.Bits == CronField.All(0, 6).Bits;

        if (domHasWildcard && dowHasWildcard)
        {
            return true; // Both are wildcards
        }
        if (domHasWildcard)
        {
            return dowValid; // Only check day-of-week
        }
        if (dowHasWildcard)
        {
            return domValid; // Only check day-of-month
        }

        // Both have specific values - use OR (standard cron behavior)
        return domValid || dowValid;
    }

    private DateTimeOffset? FindNextHour(DateTimeOffset candidate, TimeZoneInfo zone)
    {
        var hour = candidate.Hour;
        var nextHour = _hour.GetNext(hour);

        if (nextHour >= 0 && nextHour <= 23)
        {
            if (nextHour == hour)
            {
                return candidate;
            }
            return new DateTimeOffset(
                candidate.Year,
                candidate.Month,
                candidate.Day,
                nextHour,
                0,
                0,
                zone.GetUtcOffset(
                    new DateTime(candidate.Year, candidate.Month, candidate.Day, nextHour, 0, 0)
                )
            );
        }

        return null;
    }

    private DateTimeOffset? FindNextMinute(DateTimeOffset candidate, TimeZoneInfo zone)
    {
        var minute = candidate.Minute;
        var nextMinute = _minute.GetNext(minute);

        if (nextMinute >= 0 && nextMinute <= 59)
        {
            if (nextMinute == minute)
            {
                return candidate;
            }
            return new DateTimeOffset(
                candidate.Year,
                candidate.Month,
                candidate.Day,
                candidate.Hour,
                nextMinute,
                0,
                zone.GetUtcOffset(
                    new DateTime(
                        candidate.Year,
                        candidate.Month,
                        candidate.Day,
                        candidate.Hour,
                        nextMinute,
                        0
                    )
                )
            );
        }

        return null;
    }

    private DateTimeOffset? FindNextSecond(DateTimeOffset candidate, TimeZoneInfo zone)
    {
        var second = candidate.Second;
        var nextSecond = _second.GetNext(second);

        if (nextSecond >= 0 && nextSecond <= 59)
        {
            if (nextSecond == second)
            {
                return candidate;
            }
            return new DateTimeOffset(
                candidate.Year,
                candidate.Month,
                candidate.Day,
                candidate.Hour,
                candidate.Minute,
                nextSecond,
                zone.GetUtcOffset(
                    new DateTime(
                        candidate.Year,
                        candidate.Month,
                        candidate.Day,
                        candidate.Hour,
                        candidate.Minute,
                        nextSecond
                    )
                )
            );
        }

        return null;
    }

    private static DateTimeOffset? HandleDstTransition(DateTimeOffset candidate, TimeZoneInfo zone)
    {
        if (zone == TimeZoneInfo.Utc)
        {
            return candidate;
        }

        var localTime = candidate.DateTime;

        // Check for DST gap (spring forward - time doesn't exist)
        if (zone.IsInvalidTime(localTime))
        {
            return null; // Caller should skip this time
        }

        // Check for DST overlap (fall back - time exists twice)
        // In this case, we take the first occurrence (before DST ends)
        if (zone.IsAmbiguousTime(localTime))
        {
            var offsets = zone.GetAmbiguousTimeOffsets(localTime);
            // Take the larger offset (earlier clock time, before fall back)
            var offset = offsets.Max();
            return new DateTimeOffset(localTime, offset);
        }

        return candidate;
    }

    private static DateTimeOffset MoveToNextMonth(DateTimeOffset current, TimeZoneInfo zone)
    {
        var year = current.Year;
        var month = current.Month + 1;

        if (month > 12)
        {
            year++;
            month = 1;
        }

        return new DateTimeOffset(
            year,
            month,
            1,
            0,
            0,
            0,
            zone.GetUtcOffset(new DateTime(year, month, 1))
        );
    }

    private static DateTimeOffset MoveToNextDay(DateTimeOffset current, TimeZoneInfo zone)
    {
        var next = current.Date.AddDays(1);
        return new DateTimeOffset(
            next.Year,
            next.Month,
            next.Day,
            0,
            0,
            0,
            zone.GetUtcOffset(next)
        );
    }

    private static DateTimeOffset MoveToNextHour(DateTimeOffset current, TimeZoneInfo zone)
    {
        var next = new DateTime(
            current.Year,
            current.Month,
            current.Day,
            current.Hour,
            0,
            0
        ).AddHours(1);
        return new DateTimeOffset(
            next.Year,
            next.Month,
            next.Day,
            next.Hour,
            0,
            0,
            zone.GetUtcOffset(next)
        );
    }

    private static DateTimeOffset MoveToNextMinute(DateTimeOffset current, TimeZoneInfo zone)
    {
        var next = new DateTime(
            current.Year,
            current.Month,
            current.Day,
            current.Hour,
            current.Minute,
            0
        ).AddMinutes(1);
        return new DateTimeOffset(
            next.Year,
            next.Month,
            next.Day,
            next.Hour,
            next.Minute,
            0,
            zone.GetUtcOffset(next)
        );
    }

    private string DescribeTime()
    {
        var parts = new List<string>();

        // Check seconds
        if (_format is CronFormat.IncludeSeconds or CronFormat.IncludeSecondsAndYear)
        {
            var secDesc = DescribeField(_second, 0, 59, "second");
            if (secDesc != "every second")
            {
                parts.Add($"At second {secDesc}");
            }
        }

        // Check minutes
        var minDesc = DescribeField(_minute, 0, 59, "minute");
        if (minDesc == "every minute")
        {
            parts.Add("every minute");
        }
        else
        {
            parts.Add($"at minute {minDesc}");
        }

        // Check hours
        var hourDesc = DescribeField(_hour, 0, 23, "hour");
        if (hourDesc != "every hour")
        {
            parts.Add($"past hour {hourDesc}");
        }

        return string.Join(" ", parts);
    }

    private string DescribeDays()
    {
        var parts = new List<string>();

        // Day of month
        if (!_dayOfMonth.HasSpecialModifiers)
        {
            var domDesc = DescribeField(_dayOfMonth.Field, 1, 31, "day");
            if (domDesc != "every day")
            {
                parts.Add($"on day {domDesc}");
            }
        }
        else
        {
            if (_dayOfMonth.HasLast)
            {
                if (_dayOfMonth.LastOffset == 0)
                {
                    parts.Add("on the last day of the month");
                }
                else
                {
                    parts.Add($"on {_dayOfMonth.LastOffset} days before the last day of the month");
                }
            }
            if (_dayOfMonth.LastWeekday)
            {
                parts.Add("on the last weekday of the month");
            }
            if (_dayOfMonth.NearestWeekday > 0)
            {
                parts.Add($"on the nearest weekday to day {_dayOfMonth.NearestWeekday}");
            }
        }

        // Day of week
        if (!_dayOfWeek.HasSpecialModifiers)
        {
            var dowDesc = DescribeDayOfWeek();
            if (!string.IsNullOrEmpty(dowDesc))
            {
                parts.Add(dowDesc);
            }
        }
        else
        {
            if (_dayOfWeek.HasLastDayOfWeek)
            {
                parts.Add($"on the last {DayOfWeekName(_dayOfWeek.LastDayOfWeek)} of the month");
            }
            if (_dayOfWeek.HasNthOccurrence)
            {
                parts.Add(
                    $"on the {OrdinalSuffix(_dayOfWeek.NthOccurrence)} {DayOfWeekName(_dayOfWeek.NthDayOfWeek)} of the month"
                );
            }
        }

        return string.Join(" ", parts);
    }

    private string DescribeDayOfWeek()
    {
        if (_dayOfWeek.Field.Bits == CronField.All(0, 6).Bits)
        {
            return string.Empty; // Every day
        }

        var days = new List<string>();
        for (var i = 0; i <= 6; i++)
        {
            if (_dayOfWeek.Field.Contains(i))
            {
                days.Add(DayOfWeekName(i));
            }
        }

        if (days.Count == 0)
        {
            return string.Empty;
        }

        return "on " + string.Join(", ", days);
    }

    private string DescribeMonths()
    {
        if (_month.Bits == CronField.All(1, 12).Bits)
        {
            return string.Empty; // Every month
        }

        var months = new List<string>();
        for (var i = 1; i <= 12; i++)
        {
            if (_month.Contains(i))
            {
                months.Add(MonthName(i));
            }
        }

        if (months.Count == 0)
        {
            return string.Empty;
        }

        return "in " + string.Join(", ", months);
    }

    private string DescribeYears()
    {
        if (!_year.HasValue || _year.Value.IsAll)
        {
            return string.Empty;
        }

        var years = _year.Value.Years.ToList();

        if (years.Count == 0 || years.Count > 10)
        {
            return string.Empty;
        }

        return "in year "
            + string.Join(
                ", ",
                years.Order().Select(y => y.ToString(CultureInfo.InvariantCulture))
            );
    }

    private static string DescribeField(CronField field, int min, int max, string unit)
    {
        if (field.Bits == CronField.All(min, max).Bits)
        {
            return $"every {unit}";
        }

        var values = new List<int>();
        for (var i = min; i <= max; i++)
        {
            if (field.Contains(i))
            {
                values.Add(i);
            }
        }

        if (values.Count == 0)
        {
            return $"every {unit}";
        }

        if (values.Count == 1)
        {
            return values[0].ToString(CultureInfo.InvariantCulture);
        }

        return string.Join(", ", values);
    }

    private static string DayOfWeekName(int dow)
    {
        return dow switch
        {
            0 => "Sunday",
            1 => "Monday",
            2 => "Tuesday",
            3 => "Wednesday",
            4 => "Thursday",
            5 => "Friday",
            6 => "Saturday",
            _ => dow.ToString(CultureInfo.InvariantCulture),
        };
    }

    private static string MonthName(int month)
    {
        return month switch
        {
            1 => "January",
            2 => "February",
            3 => "March",
            4 => "April",
            5 => "May",
            6 => "June",
            7 => "July",
            8 => "August",
            9 => "September",
            10 => "October",
            11 => "November",
            12 => "December",
            _ => month.ToString(CultureInfo.InvariantCulture),
        };
    }

    private static string OrdinalSuffix(int n)
    {
        return n switch
        {
            1 => "1st",
            2 => "2nd",
            3 => "3rd",
            4 => "4th",
            5 => "5th",
            _ => $"{n}th",
        };
    }

    /// <summary>
    /// Gets a cron expression that fires every minute.
    /// </summary>
    public static CronExpression EveryMinute { get; } = Parse("* * * * *");

    /// <summary>
    /// Gets a cron expression that fires every hour at minute 0.
    /// </summary>
    public static CronExpression Hourly { get; } = Parse("0 * * * *");

    /// <summary>
    /// Gets a cron expression that fires every day at midnight.
    /// </summary>
    public static CronExpression Daily { get; } = Parse("0 0 * * *");

    /// <summary>
    /// Gets a cron expression that fires every week on Sunday at midnight.
    /// </summary>
    public static CronExpression Weekly { get; } = Parse("0 0 * * 0");

    /// <summary>
    /// Gets a cron expression that fires on the first day of every month at midnight.
    /// </summary>
    public static CronExpression Monthly { get; } = Parse("0 0 1 * *");

    /// <summary>
    /// Gets a cron expression that fires on January 1st at midnight.
    /// </summary>
    public static CronExpression Yearly { get; } = Parse("0 0 1 1 *");
}
