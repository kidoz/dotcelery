namespace DotCelery.Dashboard;

/// <summary>
/// Configuration options for the DotCelery dashboard.
/// </summary>
public sealed class DashboardOptions
{
    /// <summary>Gets or sets the authenticated principal claim that selects a tenant in isolation mode.</summary>
    public string TenantIdClaimType { get; set; } = "dotcelery:tenant";

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
    /// Gets or sets how long a task record the dashboard keeps lives. Records feed the task
    /// list, the state counts, and the live metrics window. Default is 24 hours.
    /// </summary>
    public TimeSpan TaskRecordRetention { get; set; } = TimeSpan.FromHours(24);

    /// <summary>
    /// Gets or sets how long a worker may go without a heartbeat before the dashboard marks it
    /// unresponsive and, after another interval, removes it. It should be longer than the
    /// worker's <c>WorkerOptions.WorkerHeartbeatInterval</c>. Default is 90 seconds.
    /// </summary>
    public TimeSpan WorkerOfflineTimeout { get; set; } = TimeSpan.FromSeconds(90);

    /// <summary>
    /// Gets or sets how often the dashboard records a metrics snapshot into the
    /// <c>IHistoricalDataStore</c>, when one is registered. Each snapshot holds the tasks
    /// completed since the previous one. Default is 1 minute.
    /// </summary>
    public TimeSpan MetricsSnapshotInterval { get; set; } = TimeSpan.FromMinutes(1);

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
    /// Gets or sets whether state-changing requests (POST, PUT, PATCH, DELETE) to the dashboard
    /// API must prove they come from a dashboard client. Such a request must carry the dashboard
    /// request header (a browser cannot set a custom header on a cross-site request without a
    /// preflight), and when it carries an Origin header the origin must be the dashboard's own
    /// or one in <see cref="AllowedOrigins"/>. Script clients call the API with the header.
    /// Default is true.
    /// </summary>
    public bool VerifyRequestOrigin { get; set; } = true;

    /// <summary>
    /// Gets or sets the origins besides the dashboard's own that may make state-changing
    /// requests, such as a dashboard UI hosted separately from the API. Default is empty.
    /// </summary>
    public IReadOnlyCollection<string> AllowedOrigins { get; set; } = [];

    /// <summary>
    /// Gets or sets the maximum number of tasks one bulk operation may affect. A request with
    /// more explicit task IDs is refused, and a task filter resolves at most this many tasks.
    /// Default is 1000.
    /// </summary>
    public int MaxBulkOperationSize { get; set; } = 1000;

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
