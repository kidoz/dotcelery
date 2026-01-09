using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Signals;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Services;

/// <summary>
/// Background service that processes signals from the signal queue
/// and dispatches them to registered handlers.
/// </summary>
public sealed class SignalQueueProcessor : BackgroundService
{
    private readonly ISignalStore _signalQueue;
    private readonly IServiceProvider _serviceProvider;
    private readonly SignalQueueProcessorOptions _options;
    private readonly ILogger<SignalQueueProcessor> _logger;
    private readonly JsonSerializerOptions _jsonOptions;

    private static readonly ConcurrentDictionary<Type, MethodInfo> _dispatchMethods = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="SignalQueueProcessor"/> class.
    /// </summary>
    public SignalQueueProcessor(
        ISignalStore signalQueue,
        IServiceProvider serviceProvider,
        IOptions<SignalQueueProcessorOptions> options,
        ILogger<SignalQueueProcessor> logger
    )
    {
        _signalQueue = signalQueue;
        _serviceProvider = serviceProvider;
        _options = options.Value;
        _logger = logger;
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("Signal queue processor is disabled");
            return;
        }

        _logger.LogInformation(
            "Signal queue processor started. Poll interval: {Interval}, Batch size: {BatchSize}",
            _options.PollInterval,
            _options.BatchSize
        );

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessBatchAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Error processing signal queue");
            }

            try
            {
                await Task.Delay(_options.PollInterval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        _logger.LogInformation("Signal queue processor stopped");
    }

    private async Task ProcessBatchAsync(CancellationToken cancellationToken)
    {
        var messages = new List<SignalMessage>();

        await foreach (
            var message in _signalQueue
                .DequeueAsync(_options.BatchSize, cancellationToken)
                .ConfigureAwait(false)
        )
        {
            messages.Add(message);
        }

        if (messages.Count == 0)
        {
            return;
        }

        _logger.LogDebug("Processing {Count} signals from queue", messages.Count);

        if (_options.ParallelProcessing && messages.Count > 1)
        {
            var parallelOptions = new ParallelOptions
            {
                MaxDegreeOfParallelism = _options.MaxDegreeOfParallelism,
                CancellationToken = cancellationToken,
            };

            await Parallel
                .ForEachAsync(
                    messages,
                    parallelOptions,
                    async (message, ct) =>
                    {
                        await ProcessMessageAsync(message, ct).ConfigureAwait(false);
                    }
                )
                .ConfigureAwait(false);
        }
        else
        {
            foreach (var message in messages)
            {
                await ProcessMessageAsync(message, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task ProcessMessageAsync(
        SignalMessage message,
        CancellationToken cancellationToken
    )
    {
        try
        {
            var signal = message.Deserialize(_jsonOptions);
            await DispatchToHandlersAsync(signal, cancellationToken).ConfigureAwait(false);

            await _signalQueue
                .AcknowledgeAsync(message.Id, cancellationToken)
                .ConfigureAwait(false);

            _logger.LogDebug(
                "Processed signal {SignalType} for task {TaskId}",
                signal.GetType().Name,
                signal.TaskId
            );
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to process signal message {MessageId}", message.Id);

            await _signalQueue
                .RejectAsync(message.Id, requeue: true, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private async Task DispatchToHandlersAsync(
        ITaskSignal signal,
        CancellationToken cancellationToken
    )
    {
        var signalType = signal.GetType();

        // Get or create the dispatch method for this signal type
        var dispatchMethod = _dispatchMethods.GetOrAdd(
            signalType,
            type =>
            {
                var method = typeof(SignalQueueProcessor).GetMethod(
                    nameof(DispatchTypedSignalAsync),
                    BindingFlags.NonPublic | BindingFlags.Instance
                )!;
                return method.MakeGenericMethod(type);
            }
        );

        await ((ValueTask)dispatchMethod.Invoke(this, [signal, cancellationToken])!).ConfigureAwait(
            false
        );
    }

    private async ValueTask DispatchTypedSignalAsync<TSignal>(
        TSignal signal,
        CancellationToken cancellationToken
    )
        where TSignal : ITaskSignal
    {
        using var scope = _serviceProvider.CreateScope();
        var handlers = scope.ServiceProvider.GetServices<ITaskSignalHandler<TSignal>>();

        foreach (var handler in handlers)
        {
            try
            {
                await handler.HandleAsync(signal, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Log but don't propagate - signals should not affect other handlers
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
