namespace DotCelery.Cron;

/// <summary>
/// Represents a day-of-week field with support for special modifiers like L and #.
/// </summary>
internal readonly struct CronDayOfWeekField
{
    private readonly CronField _field;
    private readonly int _lastDayOfWeek;
    private readonly int _nthDayOfWeek;
    private readonly int _nthOccurrence;

    /// <summary>
    /// Gets the base bitfield for regular day values.
    /// </summary>
    public CronField Field => _field;

    /// <summary>
    /// Gets the day of week for the last occurrence modifier (0-6, -1 = not set).
    /// Example: 5L means last Friday.
    /// </summary>
    public int LastDayOfWeek => _lastDayOfWeek;

    /// <summary>
    /// Gets whether this field has a last day of week modifier.
    /// </summary>
    public bool HasLastDayOfWeek => _lastDayOfWeek >= 0;

    /// <summary>
    /// Gets the day of week for Nth occurrence (0-6, -1 = not set).
    /// </summary>
    public int NthDayOfWeek => _nthDayOfWeek;

    /// <summary>
    /// Gets which occurrence of the day (1-5).
    /// Example: 5#3 means third Friday.
    /// </summary>
    public int NthOccurrence => _nthOccurrence;

    /// <summary>
    /// Gets whether this field has an Nth occurrence modifier.
    /// </summary>
    public bool HasNthOccurrence => _nthDayOfWeek >= 0 && _nthOccurrence > 0;

    /// <summary>
    /// Gets whether this field has any special modifiers.
    /// </summary>
    public bool HasSpecialModifiers => HasLastDayOfWeek || HasNthOccurrence;

    private CronDayOfWeekField(
        CronField field,
        int lastDayOfWeek = -1,
        int nthDayOfWeek = -1,
        int nthOccurrence = 0
    )
    {
        _field = field;
        _lastDayOfWeek = lastDayOfWeek;
        _nthDayOfWeek = nthDayOfWeek;
        _nthOccurrence = nthOccurrence;
    }

    /// <summary>
    /// Creates a simple field with no special modifiers.
    /// </summary>
    public static CronDayOfWeekField FromField(CronField field)
    {
        return new CronDayOfWeekField(field);
    }

    /// <summary>
    /// Creates a field for the last occurrence of a day of week.
    /// </summary>
    /// <param name="dayOfWeek">Day of week (0=Sunday, 6=Saturday)</param>
    public static CronDayOfWeekField LastOfMonth(int dayOfWeek)
    {
        return new CronDayOfWeekField(CronField.FromBits(0), lastDayOfWeek: dayOfWeek);
    }

    /// <summary>
    /// Creates a field for the Nth occurrence of a day of week.
    /// </summary>
    /// <param name="dayOfWeek">Day of week (0=Sunday, 6=Saturday)</param>
    /// <param name="occurrence">Which occurrence (1-5)</param>
    public static CronDayOfWeekField NthOfMonth(int dayOfWeek, int occurrence)
    {
        if (occurrence < 1 || occurrence > 5)
        {
            throw new CronFormatException($"Occurrence must be between 1 and 5, got: {occurrence}");
        }
        return new CronDayOfWeekField(
            CronField.FromBits(0),
            nthDayOfWeek: dayOfWeek,
            nthOccurrence: occurrence
        );
    }

    /// <summary>
    /// Combines this field with another using OR.
    /// </summary>
    public CronDayOfWeekField Or(CronDayOfWeekField other)
    {
        return new CronDayOfWeekField(
            _field.Or(other._field),
            _lastDayOfWeek >= 0 ? _lastDayOfWeek : other._lastDayOfWeek,
            _nthDayOfWeek >= 0 ? _nthDayOfWeek : other._nthDayOfWeek,
            _nthOccurrence > 0 ? _nthOccurrence : other._nthOccurrence
        );
    }

    /// <summary>
    /// Checks if the specified day matches this field for the given year/month.
    /// </summary>
    public bool Contains(int dayOfWeek, int day, int year, int month)
    {
        // Check regular bitfield first
        if (_field.Contains(dayOfWeek))
        {
            return true;
        }

        // Check last day of week modifier
        if (HasLastDayOfWeek && dayOfWeek == _lastDayOfWeek)
        {
            var lastDate = GetLastDayOfWeekInMonth(year, month, _lastDayOfWeek);
            if (day == lastDate)
            {
                return true;
            }
        }

        // Check Nth occurrence
        if (HasNthOccurrence && dayOfWeek == _nthDayOfWeek)
        {
            var nthDate = GetNthDayOfWeekInMonth(year, month, _nthDayOfWeek, _nthOccurrence);
            if (nthDate.HasValue && day == nthDate.Value)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Gets all valid days (as day-of-month) for this field in the specified year/month.
    /// </summary>
    public IEnumerable<int> GetValidDays(int year, int month)
    {
        var validDays = new HashSet<int>();
        var daysInMonth = DateTime.DaysInMonth(year, month);

        // Add days from bitfield
        for (var day = 1; day <= daysInMonth; day++)
        {
            var date = new DateOnly(year, month, day);
            var dow = (int)date.DayOfWeek;

            if (_field.Contains(dow))
            {
                validDays.Add(day);
            }
        }

        // Add last day of week
        if (HasLastDayOfWeek)
        {
            validDays.Add(GetLastDayOfWeekInMonth(year, month, _lastDayOfWeek));
        }

        // Add Nth occurrence
        if (HasNthOccurrence)
        {
            var nthDate = GetNthDayOfWeekInMonth(year, month, _nthDayOfWeek, _nthOccurrence);
            if (nthDate.HasValue)
            {
                validDays.Add(nthDate.Value);
            }
        }

        return validDays.Order();
    }

    private static int GetLastDayOfWeekInMonth(int year, int month, int targetDayOfWeek)
    {
        var daysInMonth = DateTime.DaysInMonth(year, month);
        var date = new DateOnly(year, month, daysInMonth);

        while ((int)date.DayOfWeek != targetDayOfWeek)
        {
            date = date.AddDays(-1);
        }

        return date.Day;
    }

    private static int? GetNthDayOfWeekInMonth(int year, int month, int targetDayOfWeek, int n)
    {
        var daysInMonth = DateTime.DaysInMonth(year, month);
        var count = 0;

        for (var day = 1; day <= daysInMonth; day++)
        {
            var date = new DateOnly(year, month, day);
            if ((int)date.DayOfWeek == targetDayOfWeek)
            {
                count++;
                if (count == n)
                {
                    return day;
                }
            }
        }

        // Nth occurrence doesn't exist this month
        return null;
    }
}
