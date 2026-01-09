using System.Collections;

namespace DotCelery.Beat;

/// <summary>
/// A collection of schedule entries.
/// </summary>
public sealed class Schedule : IEnumerable<ScheduleEntry>
{
    private readonly Dictionary<string, ScheduleEntry> _entries = [];

    /// <summary>
    /// Gets the number of entries in the schedule.
    /// </summary>
    public int Count => _entries.Count;

    /// <summary>
    /// Adds an entry to the schedule.
    /// </summary>
    /// <param name="entry">The entry to add.</param>
    public void Add(ScheduleEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        _entries[entry.Name] = entry;
    }

    /// <summary>
    /// Removes an entry from the schedule.
    /// </summary>
    /// <param name="name">The entry name.</param>
    /// <returns>True if removed.</returns>
    public bool Remove(string name) => _entries.Remove(name);

    /// <summary>
    /// Gets an entry by name.
    /// </summary>
    /// <param name="name">The entry name.</param>
    /// <returns>The entry, or null if not found.</returns>
    public ScheduleEntry? Get(string name)
    {
        _entries.TryGetValue(name, out var entry);
        return entry;
    }

    /// <summary>
    /// Checks if an entry exists.
    /// </summary>
    /// <param name="name">The entry name.</param>
    /// <returns>True if exists.</returns>
    public bool Contains(string name) => _entries.ContainsKey(name);

    /// <summary>
    /// Gets all entries that should run at the given time.
    /// </summary>
    /// <param name="now">The current time.</param>
    /// <returns>Entries that should run.</returns>
    public IEnumerable<ScheduleEntry> GetDueEntries(DateTimeOffset now)
    {
        return _entries.Values.Where(e => e.ShouldRun(now));
    }

    /// <inheritdoc />
    public IEnumerator<ScheduleEntry> GetEnumerator() => _entries.Values.GetEnumerator();

    /// <inheritdoc />
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
