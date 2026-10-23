using System.Diagnostics.CodeAnalysis;
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
    /// <remarks>
    /// Queued signals serialize their payload polymorphically, which a Native AOT application
    /// cannot do; it dispatches signals inline instead.
    /// </remarks>
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026",
        Justification = "Queued signal payloads are serialized polymorphically; a Native AOT application dispatches signals inline."
    )]
    [UnconditionalSuppressMessage(
        "AOT",
        "IL3050",
        Justification = "Queued signal payloads are serialized polymorphically; a Native AOT application dispatches signals inline."
    )]
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
            // Terminal task signals drive workflows; their enqueue must precede completion.
            _logger.LogError(
                ex,
                "Failed to queue signal {SignalType} for task {TaskId}",
                typeof(TSignal).Name,
                signal.TaskId
            );
            if (
                signal
                is TaskSuccessSignal
                    or TaskFailureSignal
                    or TaskRevokedSignal
                    or TaskRejectedSignal
            )
                throw;
        }
    }
}
