using DotCelery.Core.Filters;
using DotCelery.Core.Models;
using DotCelery.Core.MultiTenancy;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Filters;

/// <summary>
/// Filter that validates the tenant a task belongs to, rejecting tasks whose tenant is not in
/// the configured tenant list.
/// </summary>
/// <remarks>
/// The tenant context itself (<see cref="TenantContext.Current"/>) is set by the executor
/// around the task, not by this filter: a value written to an <c>AsyncLocal</c> inside an
/// awaited async method (the filter pipeline) does not flow back to its caller, so a filter
/// that sets it would never be seen by the task.
/// </remarks>
public sealed class TenantContextFilter : ITaskFilter
{
    private readonly MultiTenancyOptions _options;
    private readonly ILogger<TenantContextFilter> _logger;

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
    public int Order => -2000; // Run very early, before filters that depend on the tenant

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

        // Validate the tenant if enabled
        if (
            (_options.ValidateTenants || _options.ValidTenants.Count > 0)
            && _options.ValidTenants.Count > 0
        )
        {
            var tenantId = _options.ResolveTenantId(context.Message);
            if (!_options.ValidTenants.Contains(tenantId))
            {
                _logger.LogWarning(
                    "Invalid tenant {TenantId} for task {TaskId}, rejecting task",
                    tenantId,
                    context.TaskId
                );
                context.SkipExecution = true;
                context.SkipResult = new TaskResult
                {
                    TaskId = context.TaskId,
                    State = TaskState.Rejected,
                    CompletedAt = DateTimeOffset.UtcNow,
                    Duration = TimeSpan.Zero,
                    Exception = new TaskExceptionInfo
                    {
                        Type = "InvalidTenant",
                        Message = $"Tenant '{tenantId}' is not valid",
                    },
                };
            }
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask OnExecutedAsync(
        TaskExecutedContext context,
        CancellationToken cancellationToken
    ) => ValueTask.CompletedTask;
}
