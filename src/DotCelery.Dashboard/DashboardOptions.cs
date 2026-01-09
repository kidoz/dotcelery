namespace DotCelery.Dashboard;

/// <summary>
/// Configuration options for the DotCelery dashboard.
/// </summary>
public sealed class DashboardOptions
{
    /// <summary>
    /// Gets or sets the URL path prefix for the dashboard.
    /// Default is "/celery".
    /// </summary>
    public string PathPrefix { get; set; } = "/celery";

    /// <summary>
    /// Gets or sets the dashboard title.
    /// </summary>
    public string Title { get; set; } = "DotCelery Dashboard";

    /// <summary>
    /// Gets or sets whether to enable real-time updates via SignalR.
    /// </summary>
    public bool EnableRealTimeUpdates { get; set; } = true;

    /// <summary>
    /// Gets or sets the refresh interval for dashboard data in seconds.
    /// </summary>
    public int RefreshIntervalSeconds { get; set; } = 5;

    /// <summary>
    /// Gets or sets whether to allow task operations (retry, revoke) from the dashboard.
    /// </summary>
    public bool AllowTaskOperations { get; set; } = true;

    /// <summary>
    /// Gets or sets whether the dashboard is read-only.
    /// </summary>
    public bool ReadOnly { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of tasks to display per page.
    /// </summary>
    public int PageSize { get; set; } = 50;

    /// <summary>
    /// Gets or sets the time window for metrics display.
    /// </summary>
    public TimeSpan MetricsWindow { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Gets or sets a custom authorization check.
    /// Return true to allow access, false to deny.
    /// </summary>
    public Func<HttpContext, Task<bool>>? AuthorizationCallback { get; set; }

    /// <summary>
    /// Gets or sets whether authorization is required to access the dashboard.
    /// When true and no <see cref="AuthorizationCallback"/> is configured,
    /// all requests will be denied (fail-secure).
    /// Default is true for security.
    /// </summary>
    public bool RequireAuthorization { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to expose full exception details in the dashboard.
    /// When false (default), only the exception type and a generic message are shown.
    /// When true, full exception messages are exposed (use with caution in production).
    /// </summary>
    public bool ExposeExceptionDetails { get; set; }

    /// <summary>
    /// Gets or sets the maximum length of exception messages to display.
    /// Messages longer than this will be truncated.
    /// </summary>
    public int MaxExceptionMessageLength { get; set; } = 200;

    /// <summary>
    /// Gets the normalized path prefix (ensures leading slash, no trailing slash).
    /// </summary>
    internal string NormalizedPathPrefix
    {
        get
        {
            var prefix = PathPrefix.Trim();
            if (!prefix.StartsWith('/'))
            {
                prefix = "/" + prefix;
            }
            return prefix.TrimEnd('/');
        }
    }
}
