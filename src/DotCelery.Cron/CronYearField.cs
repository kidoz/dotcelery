namespace DotCelery.Cron;

/// <summary>
/// Represents a year field that can store year values (1970-2099).
/// Uses a HashSet instead of bitfield since year range exceeds 64 bits.
/// </summary>
internal readonly struct CronYearField
{
    private readonly HashSet<int> _years;
    private readonly bool _isAll;

    private CronYearField(HashSet<int> years, bool isAll = false)
    {
        _years = years;
        _isAll = isAll;
    }

    /// <summary>
    /// Creates a field that matches all years.
    /// </summary>
    public static CronYearField All()
    {
        return new CronYearField([], isAll: true);
    }

    /// <summary>
    /// Creates a field with a single year.
    /// </summary>3
    public static CronYearField FromValue(int year)
    {
        return new CronYearField([year]);
    }

    /// <summary>
    /// Creates a field with a range of years.
    /// </summary>
    public static CronYearField FromRange(int start, int end)
    {
        var years = new HashSet<int>();
        for (var y = start; y <= end; y++)
        {
            years.Add(y);
        }
        return new CronYearField(years);
    }

    /// <summary>
    /// Creates a field with years at a specific step.
    /// </summary>
    public static CronYearField FromStep(int start, int max, int step)
    {
        var years = new HashSet<int>();
        for (var y = start; y <= max; y += step)
        {
            years.Add(y);
        }
        return new CronYearField(years);
    }

    /// <summary>
    /// Combines this field with another.
    /// </summary>
    public CronYearField Or(CronYearField other)
    {
        if (_isAll || other._isAll)
        {
            return All();
        }

        var combined = new HashSet<int>(_years);
        foreach (var year in other._years)
        {
            combined.Add(year);
        }
        return new CronYearField(combined);
    }

    /// <summary>
    /// Checks if the specified year matches.
    /// </summary>
    public bool Contains(int year)
    {
        return _isAll || _years.Contains(year);
    }

    /// <summary>
    /// Gets the next valid year >= the specified year.
    /// Returns -1 if no such year exists.
    /// </summary>
    public int GetNext(int fromYear)
    {
        if (_isAll)
        {
            return fromYear;
        }

        if (_years.Count == 0)
        {
            return -1;
        }

        // Find the smallest year >= fromYear
        var next = -1;
        foreach (var year in _years)
        {
            if (year >= fromYear && (next == -1 || year < next))
            {
                next = year;
            }
        }

        return next;
    }

    /// <summary>
    /// Gets whether this field matches all years.
    /// </summary>
    public bool IsAll => _isAll;

    /// <summary>
    /// Gets all years in this field.
    /// </summary>
    public IEnumerable<int> Years => _isAll ? [] : _years;
}
