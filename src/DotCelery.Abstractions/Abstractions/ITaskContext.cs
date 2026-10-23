using DotCelery.Core.Models;
using DotCelery.Core.Progress;

namespace DotCelery.Core.Abstractions;

/// <summary>
/// Provides execution context and utilities for running tasks.
/// </summary>
public interface ITaskContext
{
    /// <summary>
    /// Gets the unique identifier for this task invocation.
    /// </summary>
    string TaskId { get; }

    /// <summary>
    /// Gets the name of the task being executed.
    /// </summary>
    string TaskName { get; }

    /// <summary>
    /// Gets the current retry attempt number (0 for first execution).
    /// </summary>
    int RetryCount { get; }

    /// <summary>
    /// Gets the maximum retries configured for this task.
    /// </summary>
    int MaxRetries { get; }

    /// <summary>
    /// Gets the queue this task was received from.
    /// </summary>
    string Queue { get; }

    /// <summary>
    /// Gets when the task was originally sent.
    /// </summary>
    DateTimeOffset SentAt { get; }

    /// <summary>
    /// Gets the scheduled execution time (if any).
    /// </summary>
    DateTimeOffset? Eta { get; }

    /// <summary>
    /// Gets the task expiration time (if any).
    /// </summary>
    DateTimeOffset? Expires { get; }

    /// <summary>
    /// Gets the parent task ID (in workflow chains).
    /// </summary>
    string? ParentId { get; }

    /// <summary>
    /// Gets the root task ID (in workflow chains).
    /// </summary>
    string? RootId { get; }

    /// <summary>
    /// Gets the correlation ID for tracing.
    /// </summary>
    string? CorrelationId { get; }

    /// <summary>
    /// Gets the tenant ID for multi-tenancy support.
    /// </summary>
    string? TenantId { get; }

    /// <summary>
    /// Gets the partition key for ordered processing.
    /// </summary>
    string? PartitionKey { get; }

    /// <summary>
    /// Gets the message headers.
    /// </summary>
    IReadOnlyDictionary<string, string>? Headers { get; }

    /// <summary>
    /// Gets the progress reporter for this task.
    /// </summary>
    IProgressReporter Progress { get; }

    /// <summary>
    /// Requests a task retry with optional delay.
    /// Throws <see cref="Exceptions.RetryException"/> to signal retry.
    /// </summary>
    /// <param name="countdown">Delay before retry.</param>
    /// <param name="exception">Original exception to include.</param>
    void Retry(TimeSpan? countdown = null, Exception? exception = null);

    /// <summary>
    /// Updates task state with custom metadata.
    /// </summary>
    /// <param name="state">New task state.</param>
    /// <param name="metadata">Optional metadata.</param>
    /// <returns>A task representing the async operation.</returns>
    Task UpdateStateAsync(TaskState state, object? metadata = null);

    /// <summary>
    /// Resolves a required service from the DI container.
    /// </summary>
    /// <typeparam name="T">Service type.</typeparam>
    /// <returns>The resolved service.</returns>
    T GetRequiredService<T>()
        where T : notnull;
}
