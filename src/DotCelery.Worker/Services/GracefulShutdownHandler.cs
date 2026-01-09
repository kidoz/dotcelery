using System.Collections.Concurrent;
using System.Diagnostics;
using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Logging;

namespace DotCelery.Worker.Services;

/// <summary>
/// Default implementation of <see cref="IGracefulShutdownHandler"/>.
/// </summary>
public sealed class GracefulShutdownHandler : IGracefulShutdownHandler
{
    private readonly ConcurrentDictionary<string, TaskRegistration> _activeTasks = new();
    private readonly ILogger<GracefulShutdownHandler> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly Lock _lock = new();

    private volatile bool _isShuttingDown;
    private TaskCompletionSource? _allTasksComplete;
    private int _initialTaskCount;

    /// <summary>
    /// Initializes a new instance of the <see cref="GracefulShutdownHandler"/> class.
    /// </summary>
    /// <param name="logger">The logger.</param>
    /// <param name="timeProvider">Optional time provider for testing.</param>
    public GracefulShutdownHandler(
        ILogger<GracefulShutdownHandler> logger,
        TimeProvider? timeProvider = null
    )
    {
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public int ActiveTaskCount => _activeTasks.Count;

    /// <inheritdoc />
    public bool IsShuttingDown => _isShuttingDown;

    /// <inheritdoc />
    public IDisposable RegisterTask(string taskId, BrokerMessage message)
    {
        var registration = new TaskRegistration(taskId, message, this);
        _activeTasks[taskId] = registration;

        _logger.LogDebug(
            "Task {TaskId} registered for graceful shutdown tracking. Active: {Count}",
            taskId,
            _activeTasks.Count
        );

        return registration;
    }

    /// <inheritdoc />
    public async Task<GracefulShutdownResult> ShutdownAsync(
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    )
    {
        var stopwatch = Stopwatch.StartNew();

        lock (_lock)
        {
            if (_isShuttingDown)
            {
                throw new InvalidOperationException("Shutdown already in progress");
            }

            _isShuttingDown = true;
            _initialTaskCount = _activeTasks.Count;

            if (_initialTaskCount == 0)
            {
                return new GracefulShutdownResult
                {
                    TotalTasks = 0,
                    CompletedTasks = 0,
                    CancelledTasks = 0,
                    NackedMessages = 0,
                    Duration = stopwatch.Elapsed,
                };
            }

            _allTasksComplete = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
        }

        _logger.LogInformation(
            "Graceful shutdown initiated. Waiting for {Count} active tasks to complete (timeout: {Timeout})",
            _initialTaskCount,
            timeout
        );

        var completedGracefully = false;

        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken
            );
            timeoutCts.CancelAfter(timeout);

            await _allTasksComplete.Task.WaitAsync(timeoutCts.Token).ConfigureAwait(false);
            completedGracefully = true;

            _logger.LogInformation(
                "All {Count} tasks completed gracefully in {Duration}",
                _initialTaskCount,
                stopwatch.Elapsed
            );
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning(
                "Graceful shutdown timeout after {Duration}. {Remaining} tasks still active",
                timeout,
                _activeTasks.Count
            );
        }

        stopwatch.Stop();

        var remainingTasks = _activeTasks.Count;
        var completedTasks = _initialTaskCount - remainingTasks;

        return new GracefulShutdownResult
        {
            TotalTasks = _initialTaskCount,
            CompletedTasks = completedTasks,
            CancelledTasks = completedGracefully ? 0 : remainingTasks,
            NackedMessages = completedGracefully ? 0 : remainingTasks,
            Duration = stopwatch.Elapsed,
        };
    }

    /// <inheritdoc />
    public IReadOnlyCollection<BrokerMessage> GetPendingMessages()
    {
        return _activeTasks.Values.Select(r => r.Message).ToList().AsReadOnly();
    }

    private void UnregisterTask(string taskId)
    {
        if (_activeTasks.TryRemove(taskId, out _))
        {
            _logger.LogDebug(
                "Task {TaskId} unregistered. Remaining: {Count}",
                taskId,
                _activeTasks.Count
            );

            if (_isShuttingDown && _activeTasks.IsEmpty)
            {
                _allTasksComplete?.TrySetResult();
            }
        }
    }

    private sealed class TaskRegistration : IDisposable
    {
        private readonly string _taskId;
        private readonly GracefulShutdownHandler _handler;
        private bool _disposed;

        public TaskRegistration(
            string taskId,
            BrokerMessage message,
            GracefulShutdownHandler handler
        )
        {
            _taskId = taskId;
            Message = message;
            _handler = handler;
        }

        public BrokerMessage Message { get; }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _handler.UnregisterTask(_taskId);
        }
    }
}
