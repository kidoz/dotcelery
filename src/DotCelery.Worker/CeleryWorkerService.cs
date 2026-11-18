using System.Runtime.ExceptionServices;
using System.Text.Json;
using System.Threading.Channels;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Canvas;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Exceptions;
using DotCelery.Core.Instrumentation;
using DotCelery.Core.Models;
using DotCelery.Core.Security;
using DotCelery.Core.Serialization;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker;

/// <summary>
/// Background service that runs the Celery worker.
/// </summary>
/// <remarks>
/// <para>
/// A message is acknowledged only after its task outcome is stored, so delivery is
/// at-least-once: a task can run again if the worker stops or a dependency fails first.
/// </para>
/// <para>
/// Messages for unknown tasks are rejected without requeue. After an infrastructure failure,
/// such as an unavailable result backend, the message is returned to the broker once
/// <see cref="WorkerOptions.InfrastructureFailureRequeueDelay"/> has elapsed.
/// </para>
/// </remarks>
public sealed partial class CeleryWorkerService : BackgroundService
{
    private readonly IMessageBroker _broker;
    private readonly TaskExecutor _executor;
    private readonly IDelayedMessageStore? _delayedMessageStore;
    private readonly IGracefulShutdownHandler? _shutdownHandler;
    private readonly IKillSwitch? _killSwitch;
    private readonly IMessageSecurityValidator? _securityValidator;
    private readonly WorkerActivity? _activity;
    private readonly WorkerOptions _options;
    private readonly ILogger<CeleryWorkerService> _logger;
    private readonly Channel<BrokerMessage> _workChannel;
    private readonly string _workerName;
    private readonly TimeProvider _timeProvider;
    private readonly WorkerMessageSettler _settler;
    private readonly FollowUpPublisher _followUps;
    private readonly EtaWaiter _etaWaiter;
    private readonly CircuitBreakerMonitor _circuits;
    private readonly RetryScheduler _retries;
    private readonly CancellationTokenSource _stopIntakeCts = new();
    private readonly CancellationTokenSource _abortExecutionCts = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="CeleryWorkerService"/> class.
    /// </summary>
    public CeleryWorkerService(
        IMessageBroker broker,
        TaskExecutor executor,
        IOptions<WorkerOptions> options,
        ILogger<CeleryWorkerService> logger,
        IDelayedMessageStore? delayedMessageStore = null,
        IGracefulShutdownHandler? shutdownHandler = null,
        IKillSwitch? killSwitch = null,
        TimeProvider? timeProvider = null,
        IDeadLetterHandler? deadLetterHandler = null,
        IMessageSecurityValidator? securityValidator = null,
        ICircuitBreakerFactory? circuitBreakerFactory = null,
        WorkerActivity? activity = null
    )
    {
        _broker = broker;
        _executor = executor;
        _delayedMessageStore = delayedMessageStore;
        _shutdownHandler = shutdownHandler;
        _killSwitch = killSwitch;
        _securityValidator = securityValidator;
        _activity = activity;
        _options = options.Value;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _workerName = WorkerIdentity.Resolve(_options);
        _settler = new WorkerMessageSettler(
            broker,
            deadLetterHandler,
            logger,
            _workerName,
            _timeProvider
        );
        _followUps = new FollowUpPublisher(broker, _settler, logger, _timeProvider, _options);
        _etaWaiter = new EtaWaiter(_options, _timeProvider, logger, _settler);
        _circuits = new CircuitBreakerMonitor(circuitBreakerFactory);
        _retries = new RetryScheduler(broker, delayedMessageStore, _options, _timeProvider, logger);

        _workChannel = Channel.CreateBounded<BrokerMessage>(
            new BoundedChannelOptions(_options.PrefetchCount * _options.Concurrency)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = true,
            }
        );
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Starting DotCelery worker {WorkerName} with concurrency {Concurrency}, queues: {Queues}",
            _workerName,
            _options.Concurrency,
            string.Join(", ", _options.Queues)
        );

        using var intakeCts = CancellationTokenSource.CreateLinkedTokenSource(
            stoppingToken,
            _stopIntakeCts.Token
        );
        using var executionCts = CancellationTokenSource.CreateLinkedTokenSource(
            stoppingToken,
            _abortExecutionCts.Token
        );

        // Not linked to stoppingToken: the broker consumer is closed only after every worker
        // has settled its message, because brokers such as RabbitMQ requeue all unsettled
        // messages when a consumer closes, including messages whose tasks are still running.
        using var consumeCts = new CancellationTokenSource();

        // Start worker tasks
        var workers = Enumerable
            .Range(0, _options.Concurrency)
            .Select(i => ProcessMessagesAsync(i, intakeCts.Token, executionCts.Token))
            .ToList();

        var consumer = _broker
            .ConsumeAsync(_options.Queues.ToList(), consumeCts.Token)
            .GetAsyncEnumerator(CancellationToken.None);

        Task<bool>? pendingRead = null;
        Exception? consumeFailure = null;

        try
        {
            pendingRead = await PumpMessagesAsync(consumer, intakeCts.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            consumeFailure = ex;
        }
        finally
        {
            _workChannel.Writer.Complete();
        }

        await Task.WhenAll(workers).ConfigureAwait(false);
        await CloseConsumerAsync(consumer, consumeCts, pendingRead).ConfigureAwait(false);

        if (intakeCts.IsCancellationRequested)
        {
            if (consumeFailure is not null)
            {
                _logger.LogWarning(
                    consumeFailure,
                    "Worker {WorkerName} consumer failed while stopping",
                    _workerName
                );
            }

            _logger.LogInformation("Worker {WorkerName} stopped", _workerName);
            return;
        }

        // The broker stopped delivering while the worker was running. Fail the service so the
        // host stops (or its supervisor restarts it) instead of running without a consumer.
        if (consumeFailure is not null)
        {
            _logger.LogError(
                consumeFailure,
                "Worker {WorkerName} stopped because consuming from the broker failed",
                _workerName
            );
            ExceptionDispatchInfo.Capture(consumeFailure).Throw();
        }

        _logger.LogError(
            "Worker {WorkerName} stopped because the broker ended the message stream",
            _workerName
        );
        throw new InvalidOperationException(
            $"Worker {_workerName} stopped because the broker ended the message stream."
        );
    }

    private async Task CloseConsumerAsync(
        IAsyncEnumerator<BrokerMessage> consumer,
        CancellationTokenSource consumeCts,
        Task<bool>? pendingRead
    )
    {
        await consumeCts.CancelAsync().ConfigureAwait(false);

        if (pendingRead is not null)
        {
            try
            {
                if (await pendingRead.ConfigureAwait(false))
                {
                    // Delivered after intake stopped
                    await _settler.ReturnToBrokerAsync(consumer.Current).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected: the read was cancelled when the consumer closed
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Broker read failed while worker {WorkerName} was stopping",
                    _workerName
                );
            }
        }

        try
        {
            await consumer.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to close the broker consumer for worker {WorkerName}",
                _workerName
            );
        }
    }

    /// <inheritdoc />
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Stop taking messages first. Cancel synchronously so the linked intake token is
        // already cancelled when this method first yields, before a finishing task can free a
        // worker for a prefetched message. Prefetched messages are returned to the broker, and
        // the broker consumer stays open until every in-flight message is settled.
        _stopIntakeCts.Cancel();

        if (!_options.EnableGracefulShutdown || _shutdownHandler is null)
        {
            _logger.LogInformation(
                "Worker {WorkerName} stopping without graceful shutdown",
                _workerName
            );
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        _logger.LogInformation(
            "Worker {WorkerName} initiating graceful shutdown with timeout {Timeout}",
            _workerName,
            _options.ShutdownTimeout
        );

        GracefulShutdownResult? result = null;

        try
        {
            result = await _shutdownHandler
                .ShutdownAsync(_options.ShutdownTimeout, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The host's shutdown budget ran out before the worker's own timeout
        }

        if (result is { CompletedGracefully: true })
        {
            _logger.LogInformation(
                "Worker {WorkerName} graceful shutdown completed. All {Total} tasks finished in {Duration}",
                _workerName,
                result.TotalTasks,
                result.Duration
            );
        }
        else
        {
            _logger.LogWarning(
                "Worker {WorkerName} forced shutdown. Completed: {Completed}, Cancelled: {Cancelled}",
                _workerName,
                result?.CompletedTasks ?? 0,
                result?.CancelledTasks ?? _shutdownHandler.ActiveTaskCount
            );

            // Cancel the remaining tasks. Each worker settles its own message once its task
            // has stopped, so a message is never requeued while its task is still running.
            await _abortExecutionCts.CancelAsync().ConfigureAwait(false);
        }

        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        base.Dispose();
        _stopIntakeCts.Dispose();
        _abortExecutionCts.Dispose();
    }
}
