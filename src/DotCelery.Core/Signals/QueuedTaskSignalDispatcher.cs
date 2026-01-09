using System.Text.Json;
using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Logging;

namespace DotCelery.Core.Signals;

/// <summary>
/// Signal dispatcher that queues signals for asynchronous processing
/// instead of handling them inline.
/// </summary>
public sealed class QueuedTaskSignalDispatcher : ITaskSignalDispatcher
{
    private readonly ISignalStore _signalQueue;
    private readonly ILogger<QueuedTaskSignalDispatcher> _logger;
    private readonly JsonSerializerOptions _jsonOptions;

    /// <summary>
    /// Initializes a new instance of the <see cref="QueuedTaskSignalDispatcher"/> class.
    /// </summary>
    /// <param name="signalQueue">The signal queue.</param>
    /// <param name="logger">The logger.</param>
    public QueuedTaskSignalDispatcher(
        ISignalStore signalQueue,
        ILogger<QueuedTaskSignalDispatcher> logger
    )
    {
        _signalQueue = signalQueue;
        _logger = logger;
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };
    }

    /// <inheritdoc />
    public async ValueTask DispatchAsync<TSignal>(
        TSignal signal,
        CancellationToken cancellationToken = default
    )
        where TSignal : ITaskSignal
    {
        ArgumentNullException.ThrowIfNull(signal);

        try
        {
            var message = SignalMessage.Create(signal, _jsonOptions);

            await _signalQueue.EnqueueAsync(message, cancellationToken).ConfigureAwait(false);

            _logger.LogDebug(
                "Queued signal {SignalType} for task {TaskId}",
                typeof(TSignal).Name,
                signal.TaskId
            );
        }
        catch (Exception ex)
        {
            // Log but don't propagate - signals should not affect task execution
            _logger.LogError(
                ex,
                "Failed to queue signal {SignalType} for task {TaskId}",
                typeof(TSignal).Name,
                signal.TaskId
            );
        }
    }
}
