using DotCelery.Core.Abstractions;
using DotCelery.Core.Exceptions;
using DotCelery.Core.Models;
using DotCelery.Core.Progress;
using DotCelery.Core.Signals;
using DotCelery.Worker.Progress;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DotCelery.Worker.Execution;

/// <summary>
/// Implementation of <see cref="ITaskContext"/> for task execution.
/// </summary>
public sealed class TaskExecutionContext : ITaskContext
{
    private readonly IServiceProvider _restrictedServiceProvider;
    private readonly IServiceProvider _scopedServiceProvider;
    private readonly IResultBackend _resultBackend;

    /// <summary>
    /// Initializes a new instance of the <see cref="TaskExecutionContext"/> class.
    /// </summary>
    public TaskExecutionContext(
        TaskMessage message,
        IServiceProvider serviceProvider,
        IResultBackend resultBackend,
        ITaskSignalDispatcher? signalDispatcher = null,
        string? workerName = null,
        IEnumerable<Type>? blockedServiceTypes = null
    )
    {
        Message = message;
        TaskId = message.Id;
        TaskName = message.Task;
        RetryCount = message.Retries;
        MaxRetries = message.MaxRetries;
        Queue = message.Queue;
        SentAt = message.Timestamp;
        Eta = message.Eta;
        Expires = message.Expires;
        ParentId = message.ParentId;
        RootId = message.RootId;
        CorrelationId = message.CorrelationId;
        TenantId = message.TenantId;
        PartitionKey = message.PartitionKey;
        Headers = message.Headers;
        _resultBackend = resultBackend;
        _scopedServiceProvider = serviceProvider;

        // Wrap service provider with restrictions to prevent access to sensitive services
        var logger = serviceProvider
            .GetService<ILoggerFactory>()
            ?.CreateLogger<RestrictedServiceProvider>();
        _restrictedServiceProvider = new RestrictedServiceProvider(
            serviceProvider,
            blockedServiceTypes,
            logger
        );

        // Create progress reporter for this task
        Progress = new ProgressReporter(
            TaskId,
            TaskName,
            resultBackend,
            signalDispatcher,
            workerName
        );
    }

    /// <summary>
    /// Gets the original task message.
    /// </summary>
    public TaskMessage Message { get; }

    /// <inheritdoc />
    public string TaskId { get; }

    /// <inheritdoc />
    public string TaskName { get; }

    /// <inheritdoc />
    public int RetryCount { get; }

    /// <inheritdoc />
    public int MaxRetries { get; }

    /// <inheritdoc />
    public string Queue { get; }

    /// <inheritdoc />
    public DateTimeOffset SentAt { get; }

    /// <inheritdoc />
    public DateTimeOffset? Eta { get; }

    /// <inheritdoc />
    public DateTimeOffset? Expires { get; }

    /// <inheritdoc />
    public string? ParentId { get; }

    /// <inheritdoc />
    public string? RootId { get; }

    /// <inheritdoc />
    public string? CorrelationId { get; }

    /// <inheritdoc />
    public string? TenantId { get; }

    /// <inheritdoc />
    public string? PartitionKey { get; }

    /// <inheritdoc />
    public IReadOnlyDictionary<string, string>? Headers { get; }

    /// <inheritdoc />
    public IProgressReporter Progress { get; }

    /// <summary>
    /// Gets the scoped service provider for filter contexts.
    /// Internal to allow infrastructure code (filters) full access while restricting task code.
    /// </summary>
    internal IServiceProvider ScopedServiceProvider => _scopedServiceProvider;

    /// <inheritdoc />
    public void Retry(TimeSpan? countdown = null, Exception? exception = null)
    {
        if (RetryCount >= MaxRetries)
        {
            throw new RejectException($"Max retries ({MaxRetries}) exceeded", exception);
        }

        throw new RetryException(countdown, exception);
    }

    /// <inheritdoc />
    public Task UpdateStateAsync(TaskState state, object? metadata = null)
    {
        return _resultBackend.UpdateStateAsync(TaskId, state, metadata).AsTask();
    }

    /// <inheritdoc />
    public T GetRequiredService<T>()
        where T : notnull
    {
        return _restrictedServiceProvider.GetRequiredService<T>();
    }
}
