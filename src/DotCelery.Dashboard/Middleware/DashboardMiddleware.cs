using System.Reflection;
using System.Text;
using System.Web;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;

namespace DotCelery.Dashboard.Middleware;

/// <summary>
/// Middleware that serves the DotCelery dashboard.
/// </summary>
public sealed class DashboardMiddleware
{
    private readonly RequestDelegate _next;
    private readonly DashboardOptions _options;
    private readonly Dictionary<string, (string ContentType, byte[] Content)> _embeddedResources =
        new();

    /// <summary>
    /// Initializes a new instance of the <see cref="DashboardMiddleware"/> class.
    /// </summary>
    public DashboardMiddleware(RequestDelegate next, IOptions<DashboardOptions> options)
    {
        _next = next;
        _options = options.Value;

        LoadEmbeddedResources();
    }

    /// <summary>
    /// Processes the request.
    /// </summary>
    public async Task InvokeAsync(HttpContext context)
    {
        var path = context.Request.Path.Value ?? "";
        var prefix = _options.NormalizedPathPrefix;

        // Check if this is a dashboard request
        if (!path.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
        {
            await _next(context);
            return;
        }

        // Check authorization
        if (_options.RequireAuthorization)
        {
            if (_options.AuthorizationCallback is null)
            {
                // Fail-secure: deny access if authorization is required but no callback configured
                context.Response.StatusCode = StatusCodes.Status403Forbidden;
                context.Response.ContentType = "text/plain";
                await context.Response.WriteAsync(
                    "Dashboard access denied. Configure AuthorizationCallback or set RequireAuthorization = false."
                );
                return;
            }

            var isAuthorized = await _options.AuthorizationCallback(context);
            if (!isAuthorized)
            {
                context.Response.StatusCode = StatusCodes.Status403Forbidden;
                return;
            }
        }
        else if (_options.AuthorizationCallback is not null)
        {
            // Authorization is optional but callback is configured, still check it
            var isAuthorized = await _options.AuthorizationCallback(context);
            if (!isAuthorized)
            {
                context.Response.StatusCode = StatusCodes.Status403Forbidden;
                return;
            }
        }

        // Get the path after the prefix
        var subPath = path[prefix.Length..];
        if (string.IsNullOrEmpty(subPath) || subPath == "/")
        {
            subPath = "/index.html";
        }

        // Try to serve embedded resource
        if (_embeddedResources.TryGetValue(subPath.ToLowerInvariant(), out var resource))
        {
            context.Response.ContentType = resource.ContentType;
            context.Response.ContentLength = resource.Content.Length;
            await context.Response.Body.WriteAsync(resource.Content);
            return;
        }

        // For SPA routing, serve index.html for non-file paths
        if (
            !subPath.Contains('.')
            && _embeddedResources.TryGetValue("/index.html", out var indexResource)
        )
        {
            context.Response.ContentType = indexResource.ContentType;
            context.Response.ContentLength = indexResource.Content.Length;
            await context.Response.Body.WriteAsync(indexResource.Content);
            return;
        }

        // Not found
        context.Response.StatusCode = StatusCodes.Status404NotFound;
    }

    private void LoadEmbeddedResources()
    {
        var assembly = typeof(DashboardMiddleware).Assembly;
        var resourcePrefix = "DotCelery.Dashboard.wwwroot.";

        foreach (var resourceName in assembly.GetManifestResourceNames())
        {
            if (!resourceName.StartsWith(resourcePrefix, StringComparison.Ordinal))
            {
                continue;
            }

            // Extract the path after the prefix
            // Resource names look like: DotCelery.Dashboard.wwwroot.js.app.js
            // We need to convert to: /js/app.js
            var pathPart = resourceName[resourcePrefix.Length..];
            var relativePath = ConvertResourceNameToPath(pathPart);

            using var stream = assembly.GetManifestResourceStream(resourceName);
            if (stream is null)
            {
                continue;
            }

            using var ms = new MemoryStream();
            stream.CopyTo(ms);
            var content = ms.ToArray();

            var contentType = GetContentType(relativePath);
            _embeddedResources[relativePath] = (contentType, content);
        }

        // If no resources found, generate default index.html
        if (!_embeddedResources.ContainsKey("/index.html"))
        {
            var defaultHtml = GenerateDefaultIndexHtml();
            _embeddedResources["/index.html"] = (
                "text/html; charset=utf-8",
                Encoding.UTF8.GetBytes(defaultHtml)
            );
        }
    }

    /// <summary>
    /// Converts a resource name path part to a URL path.
    /// Resource names use dots as separators, but the last segment before the extension
    /// contains the actual filename with its extension.
    /// Example: "js.app.js" -> "/js/app.js", "index.html" -> "/index.html"
    /// </summary>
    private static string ConvertResourceNameToPath(string resourcePath)
    {
        // Split by dots
        var parts = resourcePath.Split('.');

        if (parts.Length < 2)
        {
            // No extension, just return as-is
            return "/" + resourcePath.ToLowerInvariant();
        }

        // The last part is the extension (e.g., "js", "html", "css")
        var extension = parts[^1];

        // The second-to-last part is the filename without extension
        var fileName = parts[^2];

        // Everything before that forms the directory path
        var directoryParts = parts[..^2];

        var path =
            directoryParts.Length > 0
                ? "/" + string.Join("/", directoryParts) + "/" + fileName + "." + extension
                : "/" + fileName + "." + extension;

        return path.ToLowerInvariant();
    }

    private static string GetContentType(string path) =>
        Path.GetExtension(path).ToLowerInvariant() switch
        {
            ".html" => "text/html; charset=utf-8",
            ".css" => "text/css; charset=utf-8",
            ".js" => "application/javascript; charset=utf-8",
            ".json" => "application/json; charset=utf-8",
            ".png" => "image/png",
            ".jpg" or ".jpeg" => "image/jpeg",
            ".gif" => "image/gif",
            ".svg" => "image/svg+xml",
            ".ico" => "image/x-icon",
            ".woff" => "font/woff",
            ".woff2" => "font/woff2",
            _ => "application/octet-stream",
        };

    /// <summary>
    /// Escapes a string for safe use in HTML content.
    /// </summary>
    private static string EscapeForHtml(string value)
    {
        return HttpUtility.HtmlEncode(value);
    }

    /// <summary>
    /// Escapes a string for safe use in JavaScript strings.
    /// </summary>
    private static string EscapeForJavaScript(string value)
    {
        return value
            .Replace("\\", "\\\\")
            .Replace("'", "\\'")
            .Replace("\"", "\\\"")
            .Replace("\n", "\\n")
            .Replace("\r", "\\r")
            .Replace("<", "\\x3c")
            .Replace(">", "\\x3e");
    }

    private string GenerateDefaultIndexHtml()
    {
        var safeTitle = EscapeForHtml(_options.Title);
        var safePathPrefix = EscapeForJavaScript(_options.NormalizedPathPrefix);

        return $$"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{safeTitle}}</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; color: #333; }
        .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
        header { background: #2563eb; color: white; padding: 20px; margin-bottom: 20px; border-radius: 8px; }
        header h1 { font-size: 24px; font-weight: 600; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 20px; }
        .stat-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
        .stat-card h3 { color: #666; font-size: 14px; font-weight: 500; margin-bottom: 8px; }
        .stat-card .value { font-size: 32px; font-weight: 700; color: #2563eb; }
        .stat-card.success .value { color: #16a34a; }
        .stat-card.warning .value { color: #ca8a04; }
        .stat-card.error .value { color: #dc2626; }
        .panel { background: white; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 20px; }
        .panel-header { padding: 16px 20px; border-bottom: 1px solid #eee; display: flex; justify-content: space-between; align-items: center; }
        .panel-header h2 { font-size: 18px; font-weight: 600; }
        .panel-body { padding: 20px; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #eee; }
        th { font-weight: 600; color: #666; font-size: 14px; }
        .badge { display: inline-block; padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: 500; }
        .badge-success { background: #dcfce7; color: #166534; }
        .badge-pending { background: #fef3c7; color: #92400e; }
        .badge-failure { background: #fee2e2; color: #991b1b; }
        .badge-started { background: #dbeafe; color: #1e40af; }
        .badge-online { background: #dcfce7; color: #166534; }
        .badge-offline { background: #f3f4f6; color: #6b7280; }
        .btn { padding: 8px 16px; border-radius: 6px; border: none; cursor: pointer; font-size: 14px; }
        .btn-primary { background: #2563eb; color: white; }
        .btn-primary:hover { background: #1d4ed8; }
        .btn-danger { background: #dc2626; color: white; }
        .btn-danger:hover { background: #b91c1c; }
        .loading { text-align: center; padding: 40px; color: #666; }
        .error { color: #dc2626; padding: 20px; text-align: center; }
        .tabs { display: flex; gap: 8px; margin-bottom: 16px; }
        .tab { padding: 8px 16px; border: none; background: #f3f4f6; border-radius: 6px; cursor: pointer; }
        .tab.active { background: #2563eb; color: white; }
        #refresh-btn { background: #f3f4f6; color: #333; }
        #refresh-btn:hover { background: #e5e7eb; }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>{{safeTitle}}</h1>
        </header>

        <div class="stats-grid" id="stats-grid">
            <div class="loading">Loading...</div>
        </div>

        <div class="panel">
            <div class="panel-header">
                <h2>Workers</h2>
                <button class="btn" id="refresh-btn" onclick="loadData()">Refresh</button>
            </div>
            <div class="panel-body">
                <table>
                    <thead>
                        <tr>
                            <th>Worker ID</th>
                            <th>Hostname</th>
                            <th>Queues</th>
                            <th>Active Tasks</th>
                            <th>Processed</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody id="workers-table">
                        <tr><td colspan="6" class="loading">Loading...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>

        <div class="panel">
            <div class="panel-header">
                <h2>Recent Tasks</h2>
                <div class="tabs">
                    <button class="tab active" data-state="">All</button>
                    <button class="tab" data-state="Success">Success</button>
                    <button class="tab" data-state="Failure">Failed</button>
                    <button class="tab" data-state="Pending">Pending</button>
                    <button class="tab" data-state="Started">Running</button>
                </div>
            </div>
            <div class="panel-body">
                <table>
                    <thead>
                        <tr>
                            <th>Task ID</th>
                            <th>Task Name</th>
                            <th>State</th>
                            <th>Worker</th>
                            <th>Duration</th>
                            <th>Completed</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody id="tasks-table">
                        <tr><td colspan="7" class="loading">Loading...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <script>
        const API_BASE = '{{safePathPrefix}}/api';
        let currentState = '';

        // HTML escape function to prevent XSS
        function escapeHtml(text) {
            if (text == null) return '';
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }

        async function loadData() {
            try {
                const [overview, workers, tasks] = await Promise.all([
                    fetch(`${API_BASE}/overview`).then(r => r.json()),
                    fetch(`${API_BASE}/workers`).then(r => r.json()),
                    fetch(`${API_BASE}/tasks${currentState ? `?state=${currentState}` : ''}`).then(r => r.json())
                ]);

                renderStats(overview);
                renderWorkers(workers);
                renderTasks(tasks.tasks || []);
            } catch (err) {
                console.error('Failed to load data:', err);
            }
        }

        function renderStats(data) {
            const counts = data.stateCounts || {};
            const metrics = data.metrics || {};

            document.getElementById('stats-grid').innerHTML = `
                <div class="stat-card">
                    <h3>Pending</h3>
                    <div class="value">${counts.Pending || 0}</div>
                </div>
                <div class="stat-card success">
                    <h3>Succeeded</h3>
                    <div class="value">${counts.Success || 0}</div>
                </div>
                <div class="stat-card error">
                    <h3>Failed</h3>
                    <div class="value">${counts.Failure || 0}</div>
                </div>
                <div class="stat-card">
                    <h3>Active Workers</h3>
                    <div class="value">${data.activeWorkers || 0}</div>
                </div>
                <div class="stat-card warning">
                    <h3>Delayed</h3>
                    <div class="value">${data.delayedTaskCount || 0}</div>
                </div>
                <div class="stat-card">
                    <h3>Tasks/sec</h3>
                    <div class="value">${(metrics.tasksPerSecond || 0).toFixed(2)}</div>
                </div>
            `;
        }

        function renderWorkers(workers) {
            if (!workers.length) {
                document.getElementById('workers-table').innerHTML = '<tr><td colspan="6">No workers</td></tr>';
                return;
            }

            document.getElementById('workers-table').innerHTML = workers.map(w => `
                <tr>
                    <td>${escapeHtml(w.workerId)}</td>
                    <td>${escapeHtml(w.hostname)}</td>
                    <td>${escapeHtml((w.queues || []).join(', '))}</td>
                    <td>${escapeHtml(String(w.activeTasks))}</td>
                    <td>${escapeHtml(String(w.processedCount))}</td>
                    <td><span class="badge badge-${w.status === 'Online' ? 'online' : 'offline'}">${escapeHtml(w.status)}</span></td>
                </tr>
            `).join('');
        }

        function renderTasks(tasks) {
            if (!tasks.length) {
                document.getElementById('tasks-table').innerHTML = '<tr><td colspan="7">No tasks</td></tr>';
                return;
            }

            document.getElementById('tasks-table').innerHTML = tasks.map(t => {
                const safeTaskId = escapeHtml(t.taskId);
                const safeTaskName = escapeHtml(t.taskName);
                const safeState = escapeHtml(t.state);
                const safeWorker = escapeHtml(t.worker || '-');
                const safeDuration = t.duration ? escapeHtml(formatDuration(t.duration)) : '-';
                const safeCompleted = t.completedAt ? escapeHtml(new Date(t.completedAt).toLocaleString()) : '-';
                // For onclick, we use a data attribute to avoid inline JavaScript with user data
                const revokeBtn = (t.state === 'Pending' || t.state === 'Started')
                    ? `<button class="btn btn-danger" data-task-id="${safeTaskId}" onclick="revokeTask(this.dataset.taskId)">Revoke</button>`
                    : '';
                return `
                    <tr>
                        <td title="${safeTaskId}">${safeTaskId.substring(0, 8)}...</td>
                        <td>${safeTaskName}</td>
                        <td><span class="badge badge-${t.state.toLowerCase()}">${safeState}</span></td>
                        <td>${safeWorker}</td>
                        <td>${safeDuration}</td>
                        <td>${safeCompleted}</td>
                        <td>${revokeBtn}</td>
                    </tr>
                `;
            }).join('');
        }

        function formatDuration(duration) {
            const match = duration.match(/(\d+):(\d+):(\d+)/);
            if (match) {
                const [, h, m, s] = match;
                if (h !== '00') return `${parseInt(h)}h ${parseInt(m)}m`;
                if (m !== '00') return `${parseInt(m)}m ${parseInt(s)}s`;
                return `${parseInt(s)}s`;
            }
            return duration;
        }

        async function revokeTask(taskId) {
            if (!confirm('Are you sure you want to revoke this task?')) return;

            try {
                await fetch(`${API_BASE}/tasks/${taskId}/revoke`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ terminate: true })
                });
                loadData();
            } catch (err) {
                alert('Failed to revoke task');
            }
        }

        document.querySelectorAll('.tab').forEach(tab => {
            tab.addEventListener('click', () => {
                document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                tab.classList.add('active');
                currentState = tab.dataset.state;
                loadData();
            });
        });

        loadData();
        setInterval(loadData, {{_options.RefreshIntervalSeconds * 1000}});
    </script>
</body>
</html>
""";
    }
}
