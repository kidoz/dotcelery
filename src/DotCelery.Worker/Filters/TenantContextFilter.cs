using DotCelery.Core.Filters;
using DotCelery.Core.MultiTenancy;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Filters;

/// <summary>
/// Filter that sets the tenant context for task execution based on message tenant ID.
/// </summary>
public sealed class TenantContextFilter : ITaskFilterWithExceptionHandling
{
    private readonly MultiTenancyOptions _options;
    private readonly ILogger<TenantContextFilter> _logger;

    private const string TenantScopeProperty = "TenantScope";

    /// <summary>
    /// Initializes a new instance of the <see cref="TenantContextFilter"/> class.
    /// </summary>
    public TenantContextFilter(
        IOptions<MultiTenancyOptions> options,
        ILogger<TenantContextFilter> logger
    )
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public int Order => -2000; // Run very early to set tenant before other filters

    /// <inheritdoc />
    public ValueTask OnExecutingAsync(
        TaskExecutingContext context,
        CancellationToken cancellationToken
    )
    {
        if (!_options.Enabled)
        {
            return ValueTask.CompletedTask;
        }

        // Get tenant ID from task context (direct property or headers)
        var tenantId =
            context.TaskContext.TenantId
            ?? context.TaskContext.Headers?.GetTenantId(_options.TenantIdHeader)
            ?? _options.DefaultTenantId;

        // Validate tenant if enabled
        if (_options.ValidateTenants && _options.ValidTenants.Count > 0)
        {
            if (!_options.ValidTenants.Contains(tenantId))
            {
                _logger.LogWarning(
                    "Invalid tenant {TenantId} for task {TaskId}, using default",
                    tenantId,
                    context.TaskId
                );
                tenantId = _options.DefaultTenantId;
            }
        }

        // Set tenant context
        var scope = TenantContext.SetTenant(tenantId);
        context.Properties[TenantScopeProperty] = scope;

        _logger.LogDebug(
            "Set tenant context to {TenantId} for task {TaskId}",
            tenantId,
            context.TaskId
        );

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask OnExecutedAsync(
        TaskExecutedContext context,
        CancellationToken cancellationToken
    )
    {
        DisposeTenantScope(context.Properties);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<bool> OnExceptionAsync(
        TaskExceptionContext context,
        CancellationToken cancellationToken
    )
    {
        DisposeTenantScope(context.Properties);
        return ValueTask.FromResult(false);
    }

    private static void DisposeTenantScope(IDictionary<string, object?> properties)
    {
        if (
            properties.TryGetValue(TenantScopeProperty, out var scopeObj)
            && scopeObj is IDisposable scope
        )
        {
            scope.Dispose();
            properties.Remove(TenantScopeProperty);
        }
    }
}
