namespace DotCelery.Cron;

/// <summary>
/// Represents a day-of-month field with support for special modifiers like L and W.
/// </summary>
internal readonly struct CronDayOfMonthField
{
    private readonly CronField _field;
    private readonly bool _hasLast;
    private readonly int _lastOffset;
    private readonly int _nearestWeekday;
    private readonly bool _lastWeekday;

    /// <summary>
    /// Gets the base bitfield for regular day values.
    /// </summary>
    public CronField Field => _field;

    /// <summary>
    /// Gets whether this field includes the last day modifier (L).
    /// </summary>
    public bool HasLast => _hasLast;

    /// <summary>
    /// Gets the offset from last day (e.g., L-3 means 3 days before last day).
    /// </summary>
    public int LastOffset => _lastOffset;

    /// <summary>
    /// Gets the day number for nearest weekday calculation (0 = not set).
    /// </summary>
    public int NearestWeekday => _nearestWeekday;

    /// <summary>
    /// Gets whether this field specifies LW (last weekday of month).
    /// </summary>
    public bool LastWeekday => _lastWeekday;

    /// <summary>
    /// Gets whether this field has any special modifiers.
    /// </summary>
    public bool HasSpecialModifiers => _hasLast || _nearestWeekday > 0 || _lastWeekday;

    private CronDayOfMonthField(
        CronField field,
        bool hasLast = false,
        int lastOffset = 0,
        int nearestWeekday = 0,
        bool lastWeekday = false
    )
    {
        _field = field;
        _hasLast = hasLast;
        _lastOffset = lastOffset;
        _nearestWeekday = nearestWeekday;
        _lastWeekday = lastWeekday;
    }

    /// <summary>
    /// Creates a simple field with no special modifiers.
    /// </summary>
    public static CronDayOfMonthField FromField(CronField field)
    {
        return new CronDayOfMonthField(field);
    }

    /// <summary>
    /// Creates a field with the last day modifier.
    /// </summary>
    public static CronDayOfMonthField Last(int offset = 0)
    {
        return new CronDayOfMonthField(CronField.FromBits(0), hasLast: true, lastOffset: offset);
    }

    /// <summary>
    /// Creates a field with the nearest weekday modifier.
    /// </summary>
    public static CronDayOfMonthField Weekday(int day)
    {
        return new CronDayOfMonthField(CronField.FromBits(0), nearestWeekday: day);
    }

    /// <summary>
    /// Creates a field with the last weekday modifier (LW).
    /// </summary>
    public static CronDayOfMonthField LastWeekdayOfMonth()
    {
        return new CronDayOfMonthField(CronField.FromBits(0), lastWeekday: true);
    }

    /// <summary>
    /// Combines this field with another using OR.
    /// </summary>
    public CronDayOfMonthField Or(CronDayOfMonthField other)
    {
        return new CronDayOfMonthField(
            _field.Or(other._field),
            _hasLast || other._hasLast,
            _hasLast ? _lastOffset : other._lastOffset,
            _nearestWeekday > 0 ? _nearestWeekday : other._nearestWeekday,
            _lastWeekday || other._lastWeekday
        );
    }

    /// <summary>
    /// Checks if the specified day matches this field for the given year/month.
    /// </summary>
    public bool Contains(int day, int year, int month)
    {
        // Check regular bitfield first
        if (_field.Contains(day))
        {
            return true;
        }

        var daysInMonth = GetDaysInMonth(year, month);

        // Check last day modifier
        if (_hasLast)
        {
            var lastDay = daysInMonth - _lastOffset;
            if (day == lastDay)
            {
                return true;
            }
        }

        // Check last weekday
        if (_lastWeekday)
        {
            var lwDay = GetLastWeekday(year, month);
            if (day == lwDay)
            {
                return true;
            }
        }

        // Check nearest weekday
        if (_nearestWeekday > 0)
        {
            var nwDay = GetNearestWeekday(year, month, _nearestWeekday);
            if (day == nwDay)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Gets all valid days for this field in the specified year/month.
    /// </summary>
    public IEnumerable<int> GetValidDays(int year, int month)
    {
        var daysInMonth = GetDaysInMonth(year, month);
        var validDays = new HashSet<int>();

        // Add days from bitfield
        for (var day = 1; day <= daysInMonth; day++)
        {
            if (_field.Contains(day))
            {
                validDays.Add(day);
            }
        }

        // Add last day
        if (_hasLast)
        {
            var lastDay = daysInMonth - _lastOffset;
            if (lastDay >= 1)
            {
                validDays.Add(lastDay);
            }
        }

        // Add last weekday
        if (_lastWeekday)
        {
            validDays.Add(GetLastWeekday(year, month));
        }

        // Add nearest weekday
        if (_nearestWeekday > 0)
        {
            var nwDay = GetNearestWeekday(year, month, _nearestWeekday);
            validDays.Add(nwDay);
        }

        return validDays.Order();
    }

    private static int GetDaysInMonth(int year, int month)
    {
        return DateTime.DaysInMonth(year, month);
    }

    private static int GetLastWeekday(int year, int month)
    {
        var daysInMonth = GetDaysInMonth(year, month);
        var date = new DateOnly(year, month, daysInMonth);

        while (date.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday)
        {
            date = date.AddDays(-1);
        }

        return date.Day;
    }

    private static int GetNearestWeekday(int year, int month, int targetDay)
    {
        var daysInMonth = GetDaysInMonth(year, month);

        // Clamp target day to valid range
        if (targetDay > daysInMonth)
        {
            targetDay = daysInMonth;
        }

        var date = new DateOnly(year, month, targetDay);

        if (date.DayOfWeek == DayOfWeek.Saturday)
        {
            // Move to Friday, unless it's the 1st (then move to Monday)
            if (targetDay == 1)
            {
                return 3; // Monday
            }
            return targetDay - 1; // Friday
        }

        if (date.DayOfWeek == DayOfWeek.Sunday)
        {
            // Move to Monday, unless it would exceed month (then move to Friday)
            if (targetDay == daysInMonth)
            {
                return targetDay - 2; // Friday
            }
            return targetDay + 1; // Monday
        }

        return targetDay;
    }
}
