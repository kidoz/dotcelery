using System.Globalization;

namespace DotCelery.Cron;

/// <summary>
/// Parses cron expression strings into field bitsets.
/// </summary>
internal static class CronParser
{
    // Field ranges
    private const int SecondMin = 0,
        SecondMax = 59;
    private const int MinuteMin = 0,
        MinuteMax = 59;
    private const int HourMin = 0,
        HourMax = 23;
    private const int DayOfMonthMin = 1,
        DayOfMonthMax = 31;
    private const int MonthMin = 1,
        MonthMax = 12;
    private const int DayOfWeekMin = 0,
        DayOfWeekMax = 6;
    private const int YearMin = 1970,
        YearMax = 2099;

    // Month names (1-indexed in array for easy lookup)
    private static readonly string[] MonthNames =
    [
        "",
        "JAN",
        "FEB",
        "MAR",
        "APR",
        "MAY",
        "JUN",
        "JUL",
        "AUG",
        "SEP",
        "OCT",
        "NOV",
        "DEC",
    ];

    // Day of week names (0=Sunday)
    private static readonly string[] DayNames = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

    /// <summary>
    /// Parses a cron expression string.
    /// </summary>
    public static ParsedCronExpression Parse(string expression, CronFormat format)
    {
        ArgumentNullException.ThrowIfNull(expression);

        var parts = expression.Split(' ', StringSplitOptions.RemoveEmptyEntries);

        var minFields = format switch
        {
            CronFormat.IncludeSeconds => 6,
            CronFormat.IncludeYear => 6,
            CronFormat.IncludeSecondsAndYear => 7,
            _ => 5,
        };

        var maxFields = format switch
        {
            CronFormat.IncludeSecondsAndYear => 7,
            CronFormat.IncludeYear => 7,
            CronFormat.IncludeSeconds => 6,
            _ => 5,
        };

        if (parts.Length < minFields || parts.Length > maxFields)
        {
            throw new CronFormatException(
                $"Invalid cron expression: expected {minFields}-{maxFields} fields, got {parts.Length}. "
                    + $"Expression: '{expression}'"
            );
        }

        var index = 0;
        var hasSeconds = format is CronFormat.IncludeSeconds or CronFormat.IncludeSecondsAndYear;

        CronField second;
        if (hasSeconds)
        {
            second = ParseField(parts[index++], SecondMin, SecondMax, "second");
        }
        else
        {
            // Default to second 0 for standard format
            second = CronField.FromValue(0);
        }

        var minute = ParseField(parts[index++], MinuteMin, MinuteMax, "minute");
        var hour = ParseField(parts[index++], HourMin, HourMax, "hour");
        var dayOfMonth = ParseDayOfMonthField(parts[index++]);
        var month = ParseMonthField(parts[index++]);
        var dayOfWeek = ParseDayOfWeekField(parts[index++]);

        // Parse year if present
        CronYearField? year = null;
        var hasYear = format is CronFormat.IncludeYear or CronFormat.IncludeSecondsAndYear;
        if (hasYear && index < parts.Length)
        {
            year = ParseYearField(parts[index]);
        }
        else if (hasYear)
        {
            // Default to all years if not specified
            year = CronYearField.All();
        }

        return new ParsedCronExpression(second, minute, hour, dayOfMonth, month, dayOfWeek, year);
    }

    /// <summary>
    /// Parses a generic numeric field.
    /// </summary>
    private static CronField ParseField(string field, int min, int max, string fieldName)
    {
        try
        {
            return ParseFieldCore(field, min, max, allowWrapping: false);
        }
        catch (CronFormatException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new CronFormatException($"Invalid {fieldName} field: '{field}'", ex);
        }
    }

    /// <summary>
    /// Core field parsing logic.
    /// </summary>
    private static CronField ParseFieldCore(string field, int min, int max, bool allowWrapping)
    {
        // Handle comma-separated list
        if (field.Contains(','))
        {
            var result = CronField.FromBits(0);
            foreach (var part in field.Split(','))
            {
                result = result.Or(ParseFieldPart(part.Trim(), min, max, allowWrapping));
            }
            return result;
        }

        return ParseFieldPart(field, min, max, allowWrapping);
    }

    /// <summary>
    /// Parses a single part of a field (no commas).
    /// Handles: *, */n, n, n-m, n-m/s, n/s
    /// </summary>
    private static CronField ParseFieldPart(string part, int min, int max, bool allowWrapping)
    {
        if (string.IsNullOrWhiteSpace(part))
        {
            throw new CronFormatException("Empty field part");
        }

        // Handle wildcard: * or */n
        if (part.StartsWith('*'))
        {
            if (part == "*")
            {
                return CronField.All(min, max);
            }

            if (part.StartsWith("*/", StringComparison.Ordinal))
            {
                var stepStr = part[2..];
                if (
                    !int.TryParse(
                        stepStr,
                        NumberStyles.None,
                        CultureInfo.InvariantCulture,
                        out var step
                    )
                    || step <= 0
                )
                {
                    throw new CronFormatException($"Invalid step value: '{stepStr}'");
                }
                return CronField.FromStep(min, max, step);
            }

            throw new CronFormatException($"Invalid wildcard expression: '{part}'");
        }

        // Handle question mark (treat as wildcard)
        if (part == "?")
        {
            return CronField.All(min, max);
        }

        // Handle range: n-m or n-m/s
        if (part.Contains('-'))
        {
            return ParseRange(part, min, max, allowWrapping);
        }

        // Handle step from value: n/s
        if (part.Contains('/'))
        {
            return ParseStepFromValue(part, min, max);
        }

        // Single value
        var value = ParseInt(part, min, max);
        return CronField.FromValue(value);
    }

    /// <summary>
    /// Parses a range expression: n-m or n-m/s, supporting reverse ranges.
    /// </summary>
    private static CronField ParseRange(string part, int min, int max, bool allowWrapping)
    {
        var slashIndex = part.IndexOf('/');
        string rangePart;
        var step = 1;

        if (slashIndex >= 0)
        {
            rangePart = part[..slashIndex];
            var stepStr = part[(slashIndex + 1)..];
            if (
                !int.TryParse(stepStr, NumberStyles.None, CultureInfo.InvariantCulture, out step)
                || step <= 0
            )
            {
                throw new CronFormatException($"Invalid step value: '{stepStr}'");
            }
        }
        else
        {
            rangePart = part;
        }

        var dashIndex = rangePart.IndexOf('-');
        if (dashIndex < 0)
        {
            throw new CronFormatException($"Invalid range expression: '{part}'");
        }

        var startStr = rangePart[..dashIndex];
        var endStr = rangePart[(dashIndex + 1)..];

        var start = ParseInt(startStr, min, max);
        var end = ParseInt(endStr, min, max);

        if (start > end)
        {
            if (allowWrapping)
            {
                // Reverse range: e.g., SAT-MON (6-1) or 22-2
                return CronField.FromWrappedRange(start, end, min, max, step);
            }
            throw new CronFormatException(
                $"Invalid range: {start}-{end}. Start must be less than or equal to end."
            );
        }

        if (step == 1)
        {
            return CronField.FromRange(start, end);
        }

        // Range with step
        ulong bits = 0;
        for (var i = start; i <= end; i += step)
        {
            bits |= 1UL << i;
        }
        return CronField.FromBits(bits);
    }

    /// <summary>
    /// Parses a step from value expression: n/s
    /// </summary>
    private static CronField ParseStepFromValue(string part, int min, int max)
    {
        var slashIndex = part.IndexOf('/');
        var startStr = part[..slashIndex];
        var stepStr = part[(slashIndex + 1)..];

        var start = ParseInt(startStr, min, max);
        if (
            !int.TryParse(stepStr, NumberStyles.None, CultureInfo.InvariantCulture, out var step)
            || step <= 0
        )
        {
            throw new CronFormatException($"Invalid step value: '{stepStr}'");
        }

        return CronField.FromStep(start, max, step);
    }

    /// <summary>
    /// Parses the day-of-month field, supporting L, W modifiers.
    /// </summary>
    private static CronDayOfMonthField ParseDayOfMonthField(string field)
    {
        try
        {
            // Handle comma-separated list
            if (field.Contains(','))
            {
                var result = CronDayOfMonthField.FromField(CronField.FromBits(0));
                foreach (var part in field.Split(','))
                {
                    result = result.Or(ParseDayOfMonthPart(part.Trim()));
                }
                return result;
            }

            return ParseDayOfMonthPart(field);
        }
        catch (CronFormatException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new CronFormatException($"Invalid day-of-month field: '{field}'", ex);
        }
    }

    /// <summary>
    /// Parses a single day-of-month part.
    /// </summary>
    private static CronDayOfMonthField ParseDayOfMonthPart(string part)
    {
        var upperPart = part.ToUpperInvariant();

        // LW - last weekday of month
        if (upperPart == "LW")
        {
            return CronDayOfMonthField.LastWeekdayOfMonth();
        }

        // L or L-n - last day of month (with optional offset)
        if (upperPart.StartsWith('L'))
        {
            if (upperPart == "L")
            {
                return CronDayOfMonthField.Last();
            }

            if (upperPart.StartsWith("L-", StringComparison.Ordinal))
            {
                var offsetStr = upperPart[2..];
                if (
                    !int.TryParse(
                        offsetStr,
                        NumberStyles.None,
                        CultureInfo.InvariantCulture,
                        out var offset
                    )
                    || offset < 0
                )
                {
                    throw new CronFormatException($"Invalid L offset: '{offsetStr}'");
                }
                return CronDayOfMonthField.Last(offset);
            }

            throw new CronFormatException($"Invalid L expression: '{part}'");
        }

        // nW - nearest weekday to day n
        if (upperPart.EndsWith('W'))
        {
            var dayStr = upperPart[..^1];
            if (
                !int.TryParse(dayStr, NumberStyles.None, CultureInfo.InvariantCulture, out var day)
                || day < DayOfMonthMin
                || day > DayOfMonthMax
            )
            {
                throw new CronFormatException($"Invalid W day: '{dayStr}'");
            }
            return CronDayOfMonthField.Weekday(day);
        }

        // Question mark
        if (upperPart == "?")
        {
            return CronDayOfMonthField.FromField(CronField.All(DayOfMonthMin, DayOfMonthMax));
        }

        // Regular field (no special modifiers)
        var field = ParseFieldCore(part, DayOfMonthMin, DayOfMonthMax, allowWrapping: false);
        return CronDayOfMonthField.FromField(field);
    }

    /// <summary>
    /// Parses the month field, supporting both numeric (1-12) and names (JAN-DEC).
    /// </summary>
    private static CronField ParseMonthField(string field)
    {
        try
        {
            // Replace month names with numbers
            var normalized = NormalizeMonthNames(field);
            return ParseFieldCore(normalized, MonthMin, MonthMax, allowWrapping: false);
        }
        catch (CronFormatException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new CronFormatException($"Invalid month field: '{field}'", ex);
        }
    }

    /// <summary>
    /// Parses the day-of-week field, supporting L, # modifiers and names.
    /// </summary>
    private static CronDayOfWeekField ParseDayOfWeekField(string field)
    {
        try
        {
            // First normalize day names to numbers
            var normalized = NormalizeDayNames(field);

            // Handle comma-separated list
            if (normalized.Contains(','))
            {
                var result = CronDayOfWeekField.FromField(CronField.FromBits(0));
                foreach (var part in normalized.Split(','))
                {
                    result = result.Or(ParseDayOfWeekPart(part.Trim()));
                }
                return result;
            }

            return ParseDayOfWeekPart(normalized);
        }
        catch (CronFormatException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new CronFormatException($"Invalid day-of-week field: '{field}'", ex);
        }
    }

    /// <summary>
    /// Parses a single day-of-week part.
    /// </summary>
    private static CronDayOfWeekField ParseDayOfWeekPart(string part)
    {
        var upperPart = part.ToUpperInvariant();

        // nL - last occurrence of day n in month
        if (upperPart.EndsWith('L'))
        {
            var dayStr = upperPart[..^1];
            if (
                !int.TryParse(
                    dayStr,
                    NumberStyles.None,
                    CultureInfo.InvariantCulture,
                    out var dayOfWeek
                )
                || dayOfWeek < DayOfWeekMin
                || dayOfWeek > DayOfWeekMax
            )
            {
                throw new CronFormatException($"Invalid L day-of-week: '{dayStr}'");
            }
            return CronDayOfWeekField.LastOfMonth(dayOfWeek);
        }

        // n#m - mth occurrence of day n
        if (upperPart.Contains('#'))
        {
            var hashIndex = upperPart.IndexOf('#');
            var dayStr = upperPart[..hashIndex];
            var occurrenceStr = upperPart[(hashIndex + 1)..];

            if (
                !int.TryParse(
                    dayStr,
                    NumberStyles.None,
                    CultureInfo.InvariantCulture,
                    out var dayOfWeek
                )
                || dayOfWeek < DayOfWeekMin
                || dayOfWeek > DayOfWeekMax
            )
            {
                throw new CronFormatException($"Invalid # day-of-week: '{dayStr}'");
            }

            if (
                !int.TryParse(
                    occurrenceStr,
                    NumberStyles.None,
                    CultureInfo.InvariantCulture,
                    out var occurrence
                )
                || occurrence < 1
                || occurrence > 5
            )
            {
                throw new CronFormatException(
                    $"Invalid # occurrence: '{occurrenceStr}'. Must be 1-5."
                );
            }

            return CronDayOfWeekField.NthOfMonth(dayOfWeek, occurrence);
        }

        // Question mark
        if (upperPart == "?")
        {
            return CronDayOfWeekField.FromField(CronField.All(DayOfWeekMin, DayOfWeekMax));
        }

        // Regular field (allow wrapping for reverse ranges like SAT-MON)
        var field = ParseFieldCore(part, DayOfWeekMin, DayOfWeekMax, allowWrapping: true);
        return CronDayOfWeekField.FromField(field);
    }

    /// <summary>
    /// Replaces month names with their numeric equivalents.
    /// </summary>
    private static string NormalizeMonthNames(string field)
    {
        var result = field.ToUpperInvariant();
        for (var i = 1; i <= 12; i++)
        {
            result = result.Replace(MonthNames[i], i.ToString(CultureInfo.InvariantCulture));
        }
        return result;
    }

    /// <summary>
    /// Replaces day names with their numeric equivalents.
    /// </summary>
    private static string NormalizeDayNames(string field)
    {
        var result = field.ToUpperInvariant();
        for (var i = 0; i < DayNames.Length; i++)
        {
            result = result.Replace(DayNames[i], i.ToString(CultureInfo.InvariantCulture));
        }
        // Also handle 7 as Sunday (some systems use this)
        result = result.Replace("7", "0");
        return result;
    }

    /// <summary>
    /// Parses an integer value and validates it's within range.
    /// </summary>
    private static int ParseInt(string value, int min, int max)
    {
        if (!int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var result))
        {
            throw new CronFormatException($"Invalid numeric value: '{value}'");
        }

        if (result < min || result > max)
        {
            throw new CronFormatException($"Value {result} is out of range [{min}-{max}]");
        }

        return result;
    }

    /// <summary>
    /// Parses the year field.
    /// </summary>
    private static CronYearField ParseYearField(string field)
    {
        try
        {
            return ParseYearFieldCore(field);
        }
        catch (CronFormatException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new CronFormatException($"Invalid year field: '{field}'", ex);
        }
    }

    /// <summary>
    /// Core year field parsing logic.
    /// </summary>
    private static CronYearField ParseYearFieldCore(string field)
    {
        // Handle wildcard
        if (field == "*")
        {
            return CronYearField.All();
        }

        // Handle step from all: */n
        if (field.StartsWith("*/", StringComparison.Ordinal))
        {
            var stepStr = field[2..];
            if (
                !int.TryParse(
                    stepStr,
                    NumberStyles.None,
                    CultureInfo.InvariantCulture,
                    out var step
                )
                || step <= 0
            )
            {
                throw new CronFormatException($"Invalid step value: '{stepStr}'");
            }
            return CronYearField.FromStep(YearMin, YearMax, step);
        }

        // Handle comma-separated list
        if (field.Contains(','))
        {
            var result = CronYearField.FromValue(0);
            var first = true;
            foreach (var part in field.Split(','))
            {
                var parsed = ParseYearFieldPart(part.Trim());
                if (first)
                {
                    result = parsed;
                    first = false;
                }
                else
                {
                    result = result.Or(parsed);
                }
            }
            return result;
        }

        return ParseYearFieldPart(field);
    }

    /// <summary>
    /// Parses a single year field part.
    /// </summary>
    private static CronYearField ParseYearFieldPart(string part)
    {
        // Handle range: n-m or n-m/s
        if (part.Contains('-'))
        {
            var slashIndex = part.IndexOf('/');
            string rangePart;
            var step = 1;

            if (slashIndex >= 0)
            {
                rangePart = part[..slashIndex];
                var stepStr = part[(slashIndex + 1)..];
                if (
                    !int.TryParse(
                        stepStr,
                        NumberStyles.None,
                        CultureInfo.InvariantCulture,
                        out step
                    )
                    || step <= 0
                )
                {
                    throw new CronFormatException($"Invalid step value: '{stepStr}'");
                }
            }
            else
            {
                rangePart = part;
            }

            var dashIndex = rangePart.IndexOf('-');
            var startStr = rangePart[..dashIndex];
            var endStr = rangePart[(dashIndex + 1)..];

            var start = ParseInt(startStr, YearMin, YearMax);
            var end = ParseInt(endStr, YearMin, YearMax);

            if (start > end)
            {
                throw new CronFormatException($"Invalid year range: {start}-{end}");
            }

            if (step == 1)
            {
                return CronYearField.FromRange(start, end);
            }

            return CronYearField.FromStep(start, end, step);
        }

        // Handle step from value: n/s
        if (part.Contains('/'))
        {
            var slashIndex = part.IndexOf('/');
            var startStr = part[..slashIndex];
            var stepStr = part[(slashIndex + 1)..];

            var start = ParseInt(startStr, YearMin, YearMax);
            if (
                !int.TryParse(
                    stepStr,
                    NumberStyles.None,
                    CultureInfo.InvariantCulture,
                    out var step
                )
                || step <= 0
            )
            {
                throw new CronFormatException($"Invalid step value: '{stepStr}'");
            }

            return CronYearField.FromStep(start, YearMax, step);
        }

        // Single value
        var year = ParseInt(part, YearMin, YearMax);
        return CronYearField.FromValue(year);
    }
}

/// <summary>
/// Holds the parsed cron expression fields.
/// </summary>
internal readonly record struct ParsedCronExpression(
    CronField Second,
    CronField Minute,
    CronField Hour,
    CronDayOfMonthField DayOfMonth,
    CronField Month,
    CronDayOfWeekField DayOfWeek,
    CronYearField? Year = null
);
