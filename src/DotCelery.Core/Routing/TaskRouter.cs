using System.Collections.Frozen;
using System.Text.RegularExpressions;
using DotCelery.Core.Abstractions;

namespace DotCelery.Core.Routing;

/// <summary>
/// Default implementation of <see cref="ITaskRouter"/> with pattern matching support.
/// </summary>
public sealed class TaskRouter : ITaskRouter
{
    private readonly Dictionary<string, string> _exactRoutes = new(StringComparer.Ordinal);
    private readonly List<(Regex Pattern, string Queue, int Priority)> _patternRoutes = [];
    private readonly object _lock = new();

    private FrozenDictionary<string, string>? _frozenExactRoutes;
    private IReadOnlyList<(Regex Pattern, string Queue, int Priority)>? _frozenPatternRoutes;

    /// <inheritdoc />
    public string GetQueue(string taskName, string defaultQueue = "celery")
    {
        ArgumentException.ThrowIfNullOrEmpty(taskName);

        // Check exact routes first (most specific)
        var exactRoutes = GetFrozenExactRoutes();
        if (exactRoutes.TryGetValue(taskName, out var queue))
        {
            return queue;
        }

        // Check pattern routes (in priority order)
        var patternRoutes = GetFrozenPatternRoutes();
        foreach (var (pattern, routeQueue, _) in patternRoutes)
        {
            if (pattern.IsMatch(taskName))
            {
                return routeQueue;
            }
        }

        return defaultQueue;
    }

    /// <inheritdoc />
    public void AddRoute<TTask>(string queue)
        where TTask : ITask
    {
        AddExactRoute(TTask.TaskName, queue);
    }

    /// <inheritdoc />
    public void AddRoute(string pattern, string queue)
    {
        ArgumentException.ThrowIfNullOrEmpty(pattern);
        ArgumentException.ThrowIfNullOrEmpty(queue);

        // Convert glob pattern to regex
        var regex = ConvertPatternToRegex(pattern);
        var priority = CalculatePriority(pattern);

        lock (_lock)
        {
            _patternRoutes.Add((regex, queue, priority));
            // Sort by priority (higher = more specific = checked first)
            _patternRoutes.Sort((a, b) => b.Priority.CompareTo(a.Priority));
            _frozenPatternRoutes = null;
        }
    }

    /// <inheritdoc />
    public void AddExactRoute(string taskName, string queue)
    {
        ArgumentException.ThrowIfNullOrEmpty(taskName);
        ArgumentException.ThrowIfNullOrEmpty(queue);

        lock (_lock)
        {
            _exactRoutes[taskName] = queue;
            _frozenExactRoutes = null;
        }
    }

    private FrozenDictionary<string, string> GetFrozenExactRoutes()
    {
        var frozen = _frozenExactRoutes;
        if (frozen is not null)
        {
            return frozen;
        }

        lock (_lock)
        {
            frozen = _frozenExactRoutes;
            if (frozen is not null)
            {
                return frozen;
            }

            frozen = _exactRoutes.ToFrozenDictionary(StringComparer.Ordinal);
            _frozenExactRoutes = frozen;
            return frozen;
        }
    }

    private IReadOnlyList<(Regex Pattern, string Queue, int Priority)> GetFrozenPatternRoutes()
    {
        var frozen = _frozenPatternRoutes;
        if (frozen is not null)
        {
            return frozen;
        }

        lock (_lock)
        {
            frozen = _frozenPatternRoutes;
            if (frozen is not null)
            {
                return frozen;
            }

            frozen = _patternRoutes.ToArray();
            _frozenPatternRoutes = frozen;
            return frozen;
        }
    }

    private static Regex ConvertPatternToRegex(string pattern)
    {
        // Escape regex special characters first
        var escaped = Regex.Escape(pattern);

        // Replace escaped ** with regex for multi-segment match
        // \*\* becomes .+ (one or more characters)
        escaped = escaped.Replace(@"\*\*", ".+");

        // Replace escaped * with regex for single-segment match
        // \* becomes [^.]+ (one or more non-dot characters)
        escaped = escaped.Replace(@"\*", @"[^.]+");

        // Anchor the pattern
        return new Regex($"^{escaped}$", RegexOptions.Compiled | RegexOptions.Singleline);
    }

    private static int CalculatePriority(string pattern)
    {
        // Higher priority = more specific
        // Exact matches (no wildcards) have highest priority
        // Single wildcards (*) have higher priority than double (**)
        // More segments = higher priority

        var priority = 0;

        // Count segments
        var segments = pattern.Split('.');
        priority += segments.Length * 10;

        // Count wildcards (wildcards reduce priority)
        var singleWildcards = pattern.Split("*", StringSplitOptions.None).Length - 1;
        var doubleWildcards = pattern.Split("**", StringSplitOptions.None).Length - 1;

        // Double wildcards count as 2 single wildcards
        singleWildcards -= doubleWildcards;

        priority -= singleWildcards * 5;
        priority -= doubleWildcards * 10;

        return priority;
    }
}
