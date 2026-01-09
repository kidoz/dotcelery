using DotCelery.Core.Abstractions;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.DeadLetter;

/// <summary>
/// Default implementation of <see cref="IDeadLetterHandler"/>.
/// </summary>
public sealed class DeadLetterHandler : IDeadLetterHandler
{
    private readonly IDeadLetterStore? _store;
    private readonly IMessageSerializer _serializer;
    private readonly DeadLetterOptions _options;
    private readonly ILogger<DeadLetterHandler> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="DeadLetterHandler"/> class.
    /// </summary>
    public DeadLetterHandler(
        IMessageSerializer serializer,
        IOptions<DeadLetterOptions> options,
        ILogger<DeadLetterHandler> logger,
        IDeadLetterStore? store = null
    )
    {
        _store = store;
        _serializer = serializer;
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async ValueTask HandleAsync(
        TaskMessage message,
        DeadLetterReason reason,
        Exception? exception = null,
        string? worker = null,
        CancellationToken cancellationToken = default
    )
    {
        if (!_options.Enabled)
        {
            _logger.LogDebug(
                "Dead letter queue is disabled, not storing failed task {TaskId}",
                message.Id
            );
            return;
        }

        if (!_options.Reasons.Contains(reason))
        {
            _logger.LogDebug(
                "Dead letter reason {Reason} not configured for storage, skipping task {TaskId}",
                reason,
                message.Id
            );
            return;
        }

        if (_store is null)
        {
            _logger.LogWarning(
                "No dead letter store configured, failed task {TaskId} will not be stored",
                message.Id
            );
            return;
        }

        var deadLetterMessage = new DeadLetterMessage
        {
            Id = Guid.NewGuid().ToString("N"),
            TaskId = message.Id,
            TaskName = message.Task,
            Queue = message.Queue,
            Reason = reason,
            OriginalMessage = SerializeMessage(message),
            ExceptionMessage = exception?.Message,
            ExceptionType = exception?.GetType().FullName,
            StackTrace = _options.IncludeStackTrace ? exception?.StackTrace : null,
            RetryCount = message.Retries,
            Timestamp = DateTimeOffset.UtcNow,
            ExpiresAt = DateTimeOffset.UtcNow.Add(_options.RetentionPeriod),
            Worker = worker,
        };

        try
        {
            await _store.StoreAsync(deadLetterMessage, cancellationToken).ConfigureAwait(false);

            _logger.LogInformation(
                "Task {TaskId} ({TaskName}) stored in dead letter queue. Reason: {Reason}",
                message.Id,
                message.Task,
                reason
            );
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to store task {TaskId} in dead letter queue", message.Id);
        }
    }

    private byte[] SerializeMessage(TaskMessage message)
    {
        try
        {
            return _serializer.Serialize(message);
        }
        catch
        {
            // If serialization fails, return empty array
            return [];
        }
    }
}
