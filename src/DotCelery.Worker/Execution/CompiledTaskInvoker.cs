using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using DotCelery.Core.Abstractions;
using DotCelery.Worker.Registry;

namespace DotCelery.Worker.Execution;

/// <summary>
/// Provides compiled delegate-based task invocation for improved performance and type safety.
/// Caches compiled delegates to avoid repeated reflection overhead.
/// </summary>
public sealed class CompiledTaskInvoker
{
    private readonly ConcurrentDictionary<Type, CachedInvoker> _delegateCache = new();

    /// <summary>
    /// Delegate for executing a task with input.
    /// </summary>
    /// <param name="task">The task instance.</param>
    /// <param name="input">The task input.</param>
    /// <param name="context">The task context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that resolves to the result.</returns>
    public delegate Task<object?> TaskWithInputInvoker(
        object task,
        object input,
        ITaskContext context,
        CancellationToken cancellationToken
    );

    /// <summary>
    /// Delegate for executing a task without input.
    /// </summary>
    /// <param name="task">The task instance.</param>
    /// <param name="context">The task context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that resolves to the result.</returns>
    public delegate Task<object?> TaskWithoutInputInvoker(
        object task,
        ITaskContext context,
        CancellationToken cancellationToken
    );

    /// <summary>
    /// Invokes a task using a compiled delegate.
    /// </summary>
    /// <param name="task">The task instance.</param>
    /// <param name="input">The task input (null for tasks without input).</param>
    /// <param name="context">The task context.</param>
    /// <param name="registration">The task registration info.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The task result.</returns>
    public async Task<object?> InvokeAsync(
        object task,
        object? input,
        ITaskContext context,
        TaskRegistration registration,
        CancellationToken cancellationToken
    )
    {
        var compiledDelegate = _delegateCache.GetOrAdd(
            registration.TaskType,
            type => CompileDelegate(type, registration.InputType, registration.OutputType)
        );

        // Validate delegate matches expected signature
        if (compiledDelegate.HasInput != (input is not null))
        {
            if (compiledDelegate.HasInput)
            {
                throw new InvalidOperationException(
                    $"Task {registration.TaskType.Name} requires input but none was provided"
                );
            }
            else
            {
                throw new InvalidOperationException(
                    $"Task {registration.TaskType.Name} does not accept input but input was provided"
                );
            }
        }

        if (compiledDelegate.HasInput)
        {
            return await compiledDelegate
                .WithInput!(task, input!, context, cancellationToken)
                .ConfigureAwait(false);
        }
        else
        {
            return await compiledDelegate
                .WithoutInput!(task, context, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Pre-compiles the delegate for a task type during registration.
    /// </summary>
    /// <param name="registration">The task registration.</param>
    public void PreCompile(TaskRegistration registration)
    {
        _delegateCache.GetOrAdd(
            registration.TaskType,
            type => CompileDelegate(type, registration.InputType, registration.OutputType)
        );
    }

    private static CachedInvoker CompileDelegate(Type taskType, Type? inputType, Type? outputType)
    {
        var method = FindExecuteAsyncMethod(taskType, inputType);

        if (inputType is not null)
        {
            var del = CompileWithInputDelegate(taskType, method, inputType, outputType);
            return new CachedInvoker { HasInput = true, WithInput = del };
        }
        else
        {
            var del = CompileWithoutInputDelegate(taskType, method, outputType);
            return new CachedInvoker { HasInput = false, WithoutInput = del };
        }
    }

    private static MethodInfo FindExecuteAsyncMethod(Type taskType, Type? inputType)
    {
        const string methodName = "ExecuteAsync";

        // Find the ExecuteAsync method with the correct signature
        var methods = taskType
            .GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => m.Name == methodName)
            .ToList();

        if (methods.Count == 0)
        {
            throw new InvalidOperationException(
                $"Task {taskType.Name} does not have a public ExecuteAsync method"
            );
        }

        // Find method with matching parameter types
        foreach (var method in methods)
        {
            var parameters = method.GetParameters();

            if (inputType is not null)
            {
                // Task with input: ExecuteAsync(TInput, ITaskContext, CancellationToken)
                if (
                    parameters.Length == 3
                    && parameters[0].ParameterType == inputType
                    && parameters[1].ParameterType == typeof(ITaskContext)
                    && parameters[2].ParameterType == typeof(CancellationToken)
                )
                {
                    return method;
                }
            }
            else
            {
                // Task without input: ExecuteAsync(ITaskContext, CancellationToken)
                if (
                    parameters.Length == 2
                    && parameters[0].ParameterType == typeof(ITaskContext)
                    && parameters[1].ParameterType == typeof(CancellationToken)
                )
                {
                    return method;
                }
            }
        }

        throw new InvalidOperationException(
            $"Task {taskType.Name} does not have an ExecuteAsync method with the expected signature. "
                + $"Expected: ExecuteAsync({(inputType is not null ? $"{inputType.Name}, " : "")}ITaskContext, CancellationToken)"
        );
    }

    private static TaskWithInputInvoker CompileWithInputDelegate(
        Type taskType,
        MethodInfo method,
        Type inputType,
        Type? outputType
    )
    {
        // Parameters: (object task, object input, ITaskContext context, CancellationToken ct)
        var taskParam = Expression.Parameter(typeof(object), "task");
        var inputParam = Expression.Parameter(typeof(object), "input");
        var contextParam = Expression.Parameter(typeof(ITaskContext), "context");
        var ctParam = Expression.Parameter(typeof(CancellationToken), "ct");

        // Cast task to actual type
        var castTask = Expression.Convert(taskParam, taskType);

        // Cast input to actual type
        var castInput = Expression.Convert(inputParam, inputType);

        // Call method: ((TTask)task).ExecuteAsync((TInput)input, context, ct)
        var call = Expression.Call(castTask, method, castInput, contextParam, ctParam);

        if (outputType is not null)
        {
            // Method returns Task<TOutput> - compile a strongly-typed delegate and wrap it
            var taskResultType = typeof(Task<>).MakeGenericType(outputType);
            var funcType = typeof(Func<,,,,>).MakeGenericType(
                typeof(object),
                typeof(object),
                typeof(ITaskContext),
                typeof(CancellationToken),
                taskResultType
            );

            var lambda = Expression.Lambda(
                funcType,
                call,
                taskParam,
                inputParam,
                contextParam,
                ctParam
            );
            var compiled = lambda.Compile();

            // Create a strongly-typed wrapper that knows how to extract the result
            var wrapperType = typeof(AsyncResultExtractorWithInput<>).MakeGenericType(outputType);
            var wrapper = (IAsyncResultExtractorWithInput)
                Activator.CreateInstance(wrapperType, compiled)!;

            return wrapper.InvokeAsync;
        }
        else
        {
            // Method returns Task (void), no result to extract
            var lambda = Expression.Lambda<
                Func<object, object, ITaskContext, CancellationToken, Task>
            >(call, taskParam, inputParam, contextParam, ctParam);
            var compiled = lambda.Compile();

            return async (task, input, context, ct) =>
            {
                await compiled(task, input, context, ct).ConfigureAwait(false);
                return null;
            };
        }
    }

    private static TaskWithoutInputInvoker CompileWithoutInputDelegate(
        Type taskType,
        MethodInfo method,
        Type? outputType
    )
    {
        // Parameters: (object task, ITaskContext context, CancellationToken ct)
        var taskParam = Expression.Parameter(typeof(object), "task");
        var contextParam = Expression.Parameter(typeof(ITaskContext), "context");
        var ctParam = Expression.Parameter(typeof(CancellationToken), "ct");

        // Cast task to actual type
        var castTask = Expression.Convert(taskParam, taskType);

        // Call method: ((TTask)task).ExecuteAsync(context, ct)
        var call = Expression.Call(castTask, method, contextParam, ctParam);

        if (outputType is not null)
        {
            // Method returns Task<TOutput> - compile a strongly-typed delegate and wrap it
            var taskResultType = typeof(Task<>).MakeGenericType(outputType);
            var funcType = typeof(Func<,,,>).MakeGenericType(
                typeof(object),
                typeof(ITaskContext),
                typeof(CancellationToken),
                taskResultType
            );

            var lambda = Expression.Lambda(funcType, call, taskParam, contextParam, ctParam);
            var compiled = lambda.Compile();

            // Create a strongly-typed wrapper that knows how to extract the result
            var wrapperType = typeof(AsyncResultExtractorNoInput<>).MakeGenericType(outputType);
            var wrapper = (IAsyncResultExtractorNoInput)
                Activator.CreateInstance(wrapperType, compiled)!;

            return wrapper.InvokeAsync;
        }
        else
        {
            var lambda = Expression.Lambda<Func<object, ITaskContext, CancellationToken, Task>>(
                call,
                taskParam,
                contextParam,
                ctParam
            );
            var compiled = lambda.Compile();

            return async (task, context, ct) =>
            {
                await compiled(task, context, ct).ConfigureAwait(false);
                return null;
            };
        }
    }

    private sealed class CachedInvoker
    {
        public bool HasInput { get; init; }
        public TaskWithInputInvoker? WithInput { get; init; }
        public TaskWithoutInputInvoker? WithoutInput { get; init; }
    }

    #region Async Result Extractors

    /// <summary>
    /// Interface for the async result extractor with input (allows non-generic reference).
    /// </summary>
    private interface IAsyncResultExtractorWithInput
    {
        Task<object?> InvokeAsync(
            object task,
            object input,
            ITaskContext context,
            CancellationToken cancellationToken
        );
    }

    /// <summary>
    /// Interface for the async result extractor without input (allows non-generic reference).
    /// </summary>
    private interface IAsyncResultExtractorNoInput
    {
        Task<object?> InvokeAsync(
            object task,
            ITaskContext context,
            CancellationToken cancellationToken
        );
    }

    /// <summary>
    /// Strongly-typed wrapper that extracts results without per-invocation reflection.
    /// </summary>
    /// <typeparam name="TOutput">The output type.</typeparam>
    private sealed class AsyncResultExtractorWithInput<TOutput>(Delegate inner)
        : IAsyncResultExtractorWithInput
        where TOutput : class
    {
        private readonly Func<
            object,
            object,
            ITaskContext,
            CancellationToken,
            Task<TOutput>
        > _func = (Func<object, object, ITaskContext, CancellationToken, Task<TOutput>>)inner;

        public async Task<object?> InvokeAsync(
            object task,
            object input,
            ITaskContext context,
            CancellationToken cancellationToken
        )
        {
            return await _func(task, input, context, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Strongly-typed wrapper that extracts results without per-invocation reflection.
    /// </summary>
    /// <typeparam name="TOutput">The output type.</typeparam>
    private sealed class AsyncResultExtractorNoInput<TOutput>(Delegate inner)
        : IAsyncResultExtractorNoInput
        where TOutput : class
    {
        private readonly Func<object, ITaskContext, CancellationToken, Task<TOutput>> _func =
            (Func<object, ITaskContext, CancellationToken, Task<TOutput>>)inner;

        public async Task<object?> InvokeAsync(
            object task,
            ITaskContext context,
            CancellationToken cancellationToken
        )
        {
            return await _func(task, context, cancellationToken).ConfigureAwait(false);
        }
    }

    #endregion
}
