using DotCelery.Core.Abstractions;
using DotCelery.Core.Canvas;

namespace DotCelery.Client.Batches;

/// <summary>
/// Builder for creating batches of tasks.
/// </summary>
public sealed class BatchBuilder
{
    private readonly List<BatchTask> _tasks = [];
    private BatchTask? _callback;
    private string? _name;

    /// <summary>
    /// Sets the batch name.
    /// </summary>
    /// <param name="name">The batch name.</param>
    /// <returns>This builder.</returns>
    public BatchBuilder WithName(string name)
    {
        _name = name;
        return this;
    }

    /// <summary>
    /// Enqueues a task to the batch.
    /// </summary>
    /// <typeparam name="TTask">The task type.</typeparam>
    /// <typeparam name="TInput">The input type.</typeparam>
    /// <param name="input">The task input.</param>
    /// <param name="options">Optional send options.</param>
    /// <returns>This builder.</returns>
    public BatchBuilder Enqueue<TTask, TInput>(TInput input, SendOptions? options = null)
        where TTask : ITask<TInput>
        where TInput : class
    {
        var task = new BatchTask
        {
            TaskName = TTask.TaskName,
            Input = input,
            TaskId = options?.TaskId,
            Queue = options?.Queue,
            Priority = options?.Priority,
            MaxRetries = options?.MaxRetries,
            Headers = options?.Headers,
        };
        _tasks.Add(task);
        return this;
    }

    /// <summary>
    /// Enqueues a task to the batch with output.
    /// </summary>
    /// <typeparam name="TTask">The task type.</typeparam>
    /// <typeparam name="TInput">The input type.</typeparam>
    /// <typeparam name="TOutput">The output type.</typeparam>
    /// <param name="input">The task input.</param>
    /// <param name="options">Optional send options.</param>
    /// <returns>This builder.</returns>
    public BatchBuilder Enqueue<TTask, TInput, TOutput>(TInput input, SendOptions? options = null)
        where TTask : ITask<TInput, TOutput>
        where TInput : class
        where TOutput : class
    {
        var task = new BatchTask
        {
            TaskName = TTask.TaskName,
            Input = input,
            TaskId = options?.TaskId,
            Queue = options?.Queue,
            Priority = options?.Priority,
            MaxRetries = options?.MaxRetries,
            Headers = options?.Headers,
        };
        _tasks.Add(task);
        return this;
    }

    /// <summary>
    /// Sets a callback task to execute when all batch tasks complete.
    /// </summary>
    /// <typeparam name="TTask">The callback task type.</typeparam>
    /// <typeparam name="TInput">The callback input type.</typeparam>
    /// <param name="input">The callback input.</param>
    /// <returns>This builder.</returns>
    public BatchBuilder OnComplete<TTask, TInput>(TInput input)
        where TTask : ITask<TInput>
        where TInput : class
    {
        _callback = new BatchTask { TaskName = TTask.TaskName, Input = input };
        return this;
    }

    /// <summary>
    /// Gets the configured tasks.
    /// </summary>
    internal IReadOnlyList<BatchTask> Tasks => _tasks;

    /// <summary>
    /// Gets the callback (if any).
    /// </summary>
    internal BatchTask? Callback => _callback;

    /// <summary>
    /// Gets the batch name.
    /// </summary>
    internal string? Name => _name;
}

/// <summary>
/// Internal representation of a task in a batch.
/// </summary>
internal sealed record BatchTask
{
    public required string TaskName { get; init; }
    public object? Input { get; init; }
    public string? TaskId { get; init; }
    public string? Queue { get; init; }
    public int? Priority { get; init; }
    public int? MaxRetries { get; init; }
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
}
