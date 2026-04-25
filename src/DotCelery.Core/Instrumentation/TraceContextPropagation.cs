using System.Diagnostics;

namespace DotCelery.Core.Instrumentation;

/// <summary>
/// Injects and extracts W3C trace context through task message headers
/// so distributed traces stay connected across the broker boundary.
/// </summary>
public static class TraceContextPropagation
{
    /// <summary>
    /// Header name used for the W3C traceparent value.
    /// </summary>
    public const string TraceParentHeader = "traceparent";

    /// <summary>
    /// Header name used for the W3C tracestate value.
    /// </summary>
    public const string TraceStateHeader = "tracestate";

    /// <summary>
    /// Writes the current <see cref="Activity"/>'s trace context into a copy of
    /// <paramref name="source"/> (creating a new dictionary when it is null). Returns
    /// the original dictionary unchanged when there is no ambient activity.
    /// </summary>
    /// <param name="source">Existing headers, or null.</param>
    /// <returns>Headers dictionary with trace-context keys populated, or the original when no
    /// activity is in flight.</returns>
    public static IReadOnlyDictionary<string, string>? InjectCurrent(
        IReadOnlyDictionary<string, string>? source
    )
    {
        var activity = Activity.Current;
        if (activity is null || activity.IdFormat != ActivityIdFormat.W3C)
        {
            return source;
        }

        var headers = source is null
            ? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, string>(source, StringComparer.OrdinalIgnoreCase);

        if (activity.Id is { } traceparent)
        {
            headers[TraceParentHeader] = traceparent;
        }

        if (!string.IsNullOrEmpty(activity.TraceStateString))
        {
            headers[TraceStateHeader] = activity.TraceStateString!;
        }

        return headers;
    }

    /// <summary>
    /// Extracts a W3C <see cref="ActivityContext"/> from the given headers, if present.
    /// </summary>
    /// <param name="headers">Headers received with a task message.</param>
    /// <param name="context">The extracted context when the return value is true.</param>
    /// <returns>True when a valid traceparent was found and parsed.</returns>
    public static bool TryExtract(
        IReadOnlyDictionary<string, string>? headers,
        out ActivityContext context
    )
    {
        context = default;

        if (headers is null)
        {
            return false;
        }

        if (!headers.TryGetValue(TraceParentHeader, out var traceparent))
        {
            return false;
        }

        headers.TryGetValue(TraceStateHeader, out var tracestate);

        return ActivityContext.TryParse(traceparent, tracestate, isRemote: true, out context);
    }
}
