using DotCelery.Core.Filters;
using DotCelery.Core.Models;
using DotCelery.Core.Security;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Filters;

/// <summary>
/// Filter that validates messages against security policies before execution.
/// </summary>
/// <remarks>
/// This filter should be registered with a very low order (e.g., -2000) to ensure
/// it runs before all other filters, rejecting invalid messages early.
/// </remarks>
public sealed class SecurityValidationFilter : ITaskFilter
{
    private readonly IMessageSecurityValidator? _validator;
    private readonly MessageSecurityOptions _options;
    private readonly ILogger<SecurityValidationFilter> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="SecurityValidationFilter"/> class.
    /// </summary>
    public SecurityValidationFilter(
        IOptions<MessageSecurityOptions> options,
        ILogger<SecurityValidationFilter> logger,
        IMessageSecurityValidator? validator = null
    )
    {
        _options = options.Value;
        _logger = logger;
        _validator = validator;
    }

    /// <summary>
    /// Gets the execution order. Runs first to reject invalid messages early.
    /// </summary>
    public int Order => -2000;

    /// <inheritdoc />
    public ValueTask OnExecutingAsync(
        TaskExecutingContext context,
        CancellationToken cancellationToken
    )
    {
        // Check schema version
        if (context.Message.SchemaVersion > _options.MaxAllowedSchemaVersion)
        {
            _logger.LogWarning(
                "Message {MessageId} has unsupported schema version {Version} (max: {MaxVersion})",
                context.TaskId,
                context.Message.SchemaVersion,
                _options.MaxAllowedSchemaVersion
            );

            context.SkipExecution = true;
            context.SkipResult = CreateRejectedResult(
                context.TaskId,
                MessageValidationError.UnsupportedSchemaVersion,
                $"Schema version {context.Message.SchemaVersion} is not supported"
            );
            return ValueTask.CompletedTask;
        }

        // Check payload size
        if (
            _options.MaxPayloadSizeBytes > 0
            && context.Message.Args.Length > _options.MaxPayloadSizeBytes
        )
        {
            _logger.LogWarning(
                "Message {MessageId} payload size {Size} exceeds limit {Limit}",
                context.TaskId,
                context.Message.Args.Length,
                _options.MaxPayloadSizeBytes
            );

            context.SkipExecution = true;
            context.SkipResult = CreateRejectedResult(
                context.TaskId,
                MessageValidationError.PayloadTooLarge,
                $"Payload size {context.Message.Args.Length} bytes exceeds maximum of {_options.MaxPayloadSizeBytes} bytes"
            );
            return ValueTask.CompletedTask;
        }

        // Check task allowlist
        if (_options.EnforceTaskAllowlist && !_options.AllowedTaskNames.Contains(context.TaskName))
        {
            _logger.LogWarning(
                "Message {MessageId} has disallowed task name {TaskName}",
                context.TaskId,
                context.TaskName
            );

            context.SkipExecution = true;
            context.SkipResult = CreateRejectedResult(
                context.TaskId,
                MessageValidationError.TaskNotAllowed,
                $"Task '{context.TaskName}' is not in the allowed task list"
            );
            return ValueTask.CompletedTask;
        }

        // Signature verification would happen in the broker/serializer layer
        // as it requires the raw serialized bytes before deserialization

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask OnExecutedAsync(
        TaskExecutedContext context,
        CancellationToken cancellationToken
    )
    {
        // No post-execution validation needed
        return ValueTask.CompletedTask;
    }

    private static TaskResult CreateRejectedResult(
        string taskId,
        MessageValidationError errorCode,
        string errorMessage
    )
    {
        return new TaskResult
        {
            TaskId = taskId,
            State = TaskState.Rejected,
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = TimeSpan.Zero,
            Exception = new TaskExceptionInfo
            {
                Type = nameof(MessageSecurityException),
                Message = errorMessage,
            },
            Metadata = new Dictionary<string, object>
            {
                ["securityError"] = errorCode.ToString(),
                ["securityMessage"] = errorMessage,
            },
        };
    }
}
