using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DotCelery.Core.Signals;

/// <summary>
/// Default implementation of <see cref="ITaskSignalDispatcher"/>.
/// Dispatches signals to all registered handlers via DI.
/// </summary>
public sealed class TaskSignalDispatcher : ITaskSignalDispatcher
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<TaskSignalDispatcher> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="TaskSignalDispatcher"/> class.
    /// </summary>
    public TaskSignalDispatcher(
        IServiceProvider serviceProvider,
        ILogger<TaskSignalDispatcher> logger
    )
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    /// <inheritdoc />
    public async ValueTask DispatchAsync<TSignal>(
        TSignal signal,
        CancellationToken cancellationToken = default
    )
        where TSignal : ITaskSignal
    {
        var handlers = _serviceProvider.GetServices<ITaskSignalHandler<TSignal>>();

        foreach (var handler in handlers)
        {
            try
            {
                await handler.HandleAsync(signal, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Log but don't propagate - signals should not affect task execution
                _logger.LogError(
                    ex,
                    "Signal handler {HandlerType} threw exception for signal {SignalType} on task {TaskId}",
                    handler.GetType().Name,
                    typeof(TSignal).Name,
                    signal.TaskId
                );
            }
        }
    }
}

/// <summary>
/// Null implementation of <see cref="ITaskSignalDispatcher"/> that does nothing.
/// Used when signals are disabled.
/// </summary>
public sealed class NullTaskSignalDispatcher : ITaskSignalDispatcher
{
    /// <summary>
    /// Singleton instance.
    /// </summary>
    public static readonly NullTaskSignalDispatcher Instance = new();

    private NullTaskSignalDispatcher() { }

    /// <inheritdoc />
    public ValueTask DispatchAsync<TSignal>(
        TSignal signal,
        CancellationToken cancellationToken = default
    )
        where TSignal : ITaskSignal
    {
        return ValueTask.CompletedTask;
    }
}
