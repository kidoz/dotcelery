using DotCelery.Core.Filters;
using DotCelery.Worker.Registry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Filters;

/// <summary>
/// Manages and executes the task filter pipeline.
/// </summary>
public sealed class TaskFilterPipeline
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<TaskFilterPipeline> _logger;
    private readonly TaskFilterOptions _options;

    /// <summary>
    /// Initializes a new instance of the <see cref="TaskFilterPipeline"/> class.
    /// </summary>
    public TaskFilterPipeline(
        IServiceProvider serviceProvider,
        IOptions<TaskFilterOptions> options,
        ILogger<TaskFilterPipeline> logger
    )
    {
        _serviceProvider = serviceProvider;
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>
    /// Adds a global filter type that applies to all tasks.
    /// </summary>
    /// <typeparam name="TFilter">The filter type.</typeparam>
    public void AddGlobalFilter<TFilter>()
        where TFilter : class
    {
        AddGlobalFilter(typeof(TFilter));
    }

    /// <summary>
    /// Adds a global filter type that applies to all tasks.
    /// </summary>
    /// <param name="filterType">The filter type.</param>
    public void AddGlobalFilter(Type filterType)
    {
        if (
            !typeof(ITaskFilter).IsAssignableFrom(filterType)
            && !typeof(ITaskExceptionFilter).IsAssignableFrom(filterType)
        )
        {
            throw new ArgumentException(
                $"Filter type must implement {nameof(ITaskFilter)} or {nameof(ITaskExceptionFilter)}",
                nameof(filterType)
            );
        }

        _options.GlobalFilterTypes.Add(filterType);
    }

    /// <summary>
    /// Resolves and returns all filters for a task, sorted by order.
    /// </summary>
    /// <param name="registration">The task registration.</param>
    /// <param name="scope">The service scope for resolving filters.</param>
    /// <returns>The resolved filters in execution order.</returns>
    public IReadOnlyList<ResolvedFilter> GetFilters(
        TaskRegistration registration,
        IServiceScope scope
    )
    {
        var filters = new List<ResolvedFilter>();

        // Add global filters
        foreach (var filterType in _options.GlobalFilterTypes)
        {
            var filter = ResolveFilter(filterType, scope);
            if (filter is not null)
            {
                filters.Add(filter);
            }
        }

        // Add task-specific filters
        if (registration.FilterTypes is not null)
        {
            foreach (var filterType in registration.FilterTypes)
            {
                var filter = ResolveFilter(filterType, scope);
                if (filter is not null)
                {
                    filters.Add(filter);
                }
            }
        }

        // Sort by order (ascending for executing, will be reversed for executed)
        return filters.OrderBy(f => f.Order).ToList();
    }

    private ResolvedFilter? ResolveFilter(Type filterType, IServiceScope scope)
    {
        try
        {
            var instance =
                scope.ServiceProvider.GetService(filterType)
                ?? ActivatorUtilities.CreateInstance(scope.ServiceProvider, filterType);

            var taskFilter = instance as ITaskFilter;
            var exceptionFilter = instance as ITaskExceptionFilter;

            if (taskFilter is null && exceptionFilter is null)
            {
                _logger.LogWarning(
                    "Filter type {FilterType} does not implement required interfaces",
                    filterType.Name
                );
                return null;
            }

            var order = taskFilter?.Order ?? 0;

            return new ResolvedFilter(taskFilter, exceptionFilter, order);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to resolve filter {FilterType}", filterType.Name);
            return null;
        }
    }

    /// <summary>
    /// Executes the OnExecuting phase of all filters.
    /// </summary>
    /// <param name="filters">The filters to execute.</param>
    /// <param name="context">The executing context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if execution should continue, false if a filter requested to skip execution.</returns>
    public async ValueTask<bool> ExecuteOnExecutingAsync(
        IReadOnlyList<ResolvedFilter> filters,
        TaskExecutingContext context,
        CancellationToken cancellationToken
    )
    {
        foreach (var filter in filters)
        {
            if (filter.TaskFilter is null)
                continue;

            try
            {
                await filter
                    .TaskFilter.OnExecutingAsync(context, cancellationToken)
                    .ConfigureAwait(false);

                if (context.SkipExecution)
                {
                    _logger.LogDebug(
                        "Filter {FilterType} requested to skip execution for task {TaskId}",
                        filter.TaskFilter.GetType().Name,
                        context.TaskId
                    );
                    return false;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Filter {FilterType} threw exception in OnExecutingAsync for task {TaskId}",
                    filter.TaskFilter.GetType().Name,
                    context.TaskId
                );
                throw;
            }
        }

        return true;
    }

    /// <summary>
    /// Executes the OnExecuted phase of all filters (in reverse order).
    /// </summary>
    /// <param name="filters">The filters to execute.</param>
    /// <param name="context">The executed context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async ValueTask ExecuteOnExecutedAsync(
        IReadOnlyList<ResolvedFilter> filters,
        TaskExecutedContext context,
        CancellationToken cancellationToken
    )
    {
        // Execute in reverse order (LIFO)
        for (var i = filters.Count - 1; i >= 0; i--)
        {
            var filter = filters[i];
            if (filter.TaskFilter is null)
                continue;

            try
            {
                await filter
                    .TaskFilter.OnExecutedAsync(context, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Filter {FilterType} threw exception in OnExecutedAsync for task {TaskId}",
                    filter.TaskFilter.GetType().Name,
                    context.TaskId
                );
                // Continue executing remaining filters
            }
        }
    }

    /// <summary>
    /// Executes exception filters.
    /// </summary>
    /// <param name="filters">The filters to execute.</param>
    /// <param name="context">The exception context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the exception was handled, false otherwise.</returns>
    public async ValueTask<bool> ExecuteOnExceptionAsync(
        IReadOnlyList<ResolvedFilter> filters,
        TaskExceptionContext context,
        CancellationToken cancellationToken
    )
    {
        // Execute exception filters in reverse order
        for (var i = filters.Count - 1; i >= 0; i--)
        {
            var filter = filters[i];
            if (filter.ExceptionFilter is null)
                continue;

            try
            {
                var handled = await filter
                    .ExceptionFilter.OnExceptionAsync(context, cancellationToken)
                    .ConfigureAwait(false);

                if (handled || context.ExceptionHandled)
                {
                    _logger.LogDebug(
                        "Filter {FilterType} handled exception for task {TaskId}",
                        filter.ExceptionFilter.GetType().Name,
                        context.TaskId
                    );
                    return true;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Filter {FilterType} threw exception in OnExceptionAsync for task {TaskId}",
                    filter.ExceptionFilter.GetType().Name,
                    context.TaskId
                );
                // Continue executing remaining filters
            }
        }

        return false;
    }
}

/// <summary>
/// A resolved filter instance with its execution order.
/// </summary>
public sealed record ResolvedFilter(
    ITaskFilter? TaskFilter,
    ITaskExceptionFilter? ExceptionFilter,
    int Order
);
