using DotCelery.Backend.InMemory;
using DotCelery.Core.Models;
using DotCelery.Core.Progress;
using DotCelery.Core.Signals;
using DotCelery.Worker.Progress;

namespace DotCelery.Tests.Unit.Progress;

/// <summary>
/// Tests for <see cref="ProgressReporter"/>.
/// </summary>
public sealed class ProgressReporterTests : IAsyncDisposable
{
    private readonly InMemoryResultBackend _backend = new();
    private readonly TestSignalDispatcher _signalDispatcher = new();

    public async ValueTask DisposeAsync()
    {
        await _backend.DisposeAsync();
    }

    [Fact]
    public async Task ReportAsync_UpdatesBackendState()
    {
        // Arrange
        var reporter = new ProgressReporter("task-1", "test.task", _backend, _signalDispatcher);

        // Act
        await reporter.ReportAsync(50.0, "Halfway there");

        // Assert
        var state = await _backend.GetStateAsync("task-1");
        Assert.NotNull(state);
        Assert.Equal(TaskState.Progress, state);
    }

    [Fact]
    public async Task ReportAsync_ClampsPercentageToZero()
    {
        // Arrange
        var reporter = new ProgressReporter("task-1", "test.task", _backend, _signalDispatcher);

        // Act
        await reporter.ReportAsync(-10.0, "Invalid percentage");

        // Assert
        var signal = _signalDispatcher.ReceivedSignals.OfType<ProgressUpdatedSignal>().Single();
        Assert.Equal(0, signal.Progress.Percentage);
    }

    [Fact]
    public async Task ReportAsync_ClampsPercentageTo100()
    {
        // Arrange
        var reporter = new ProgressReporter("task-1", "test.task", _backend, _signalDispatcher);

        // Act
        await reporter.ReportAsync(150.0, "Over 100%");

        // Assert
        var signal = _signalDispatcher.ReceivedSignals.OfType<ProgressUpdatedSignal>().Single();
        Assert.Equal(100, signal.Progress.Percentage);
    }

    [Fact]
    public async Task ReportAsync_DispatchesProgressSignal()
    {
        // Arrange
        var reporter = new ProgressReporter(
            "task-1",
            "test.task",
            _backend,
            _signalDispatcher,
            "worker-1"
        );

        // Act
        await reporter.ReportAsync(75.0, "Almost done");

        // Assert
        var signal = Assert.Single(
            _signalDispatcher.ReceivedSignals.OfType<ProgressUpdatedSignal>()
        );
        Assert.Equal("task-1", signal.TaskId);
        Assert.Equal("test.task", signal.TaskName);
        Assert.Equal("worker-1", signal.Worker);
        Assert.Equal(75.0, signal.Progress.Percentage);
        Assert.Equal("Almost done", signal.Progress.Message);
    }

    [Fact]
    public async Task ReportAsync_WithProgressInfo_PreservesAllData()
    {
        // Arrange
        var reporter = new ProgressReporter("task-1", "test.task", _backend, _signalDispatcher);
        var data = new Dictionary<string, object> { ["key"] = "value" };

        var progressInfo = new ProgressInfo
        {
            Percentage = 60.0,
            Message = "Processing",
            Data = data,
            CurrentStep = "Step 2",
            EstimatedRemaining = TimeSpan.FromMinutes(5),
            Timestamp = DateTimeOffset.UtcNow,
        };

        // Act
        await reporter.ReportAsync(progressInfo);

        // Assert
        var signal = Assert.Single(
            _signalDispatcher.ReceivedSignals.OfType<ProgressUpdatedSignal>()
        );
        Assert.Equal(60.0, signal.Progress.Percentage);
        Assert.Equal("Processing", signal.Progress.Message);
        Assert.Equal("Step 2", signal.Progress.CurrentStep);
        Assert.Equal(TimeSpan.FromMinutes(5), signal.Progress.EstimatedRemaining);
        Assert.NotNull(signal.Progress.Data);
        Assert.Equal("value", signal.Progress.Data["key"]);
    }

    [Fact]
    public async Task ReportItemsAsync_CalculatesPercentage()
    {
        // Arrange
        var reporter = new ProgressReporter("task-1", "test.task", _backend, _signalDispatcher);

        // Act
        await reporter.ReportItemsAsync(3, 10, "Processing items");

        // Assert
        var signal = Assert.Single(
            _signalDispatcher.ReceivedSignals.OfType<ProgressUpdatedSignal>()
        );
        Assert.Equal(30.0, signal.Progress.Percentage);
        Assert.Equal(3, signal.Progress.ItemsProcessed);
        Assert.Equal(10, signal.Progress.TotalItems);
        Assert.Equal("Processing items", signal.Progress.Message);
    }

    [Fact]
    public async Task ReportItemsAsync_WithZeroTotal_ReturnsZeroPercentage()
    {
        // Arrange
        var reporter = new ProgressReporter("task-1", "test.task", _backend, _signalDispatcher);

        // Act
        await reporter.ReportItemsAsync(0, 0);

        // Assert
        var signal = Assert.Single(
            _signalDispatcher.ReceivedSignals.OfType<ProgressUpdatedSignal>()
        );
        Assert.Equal(0, signal.Progress.Percentage);
    }

    [Fact]
    public async Task ReportStepAsync_CalculatesPercentage()
    {
        // Arrange
        var reporter = new ProgressReporter("task-1", "test.task", _backend, _signalDispatcher);

        // Act
        await reporter.ReportStepAsync(2, 4, "Validation");

        // Assert
        var signal = Assert.Single(
            _signalDispatcher.ReceivedSignals.OfType<ProgressUpdatedSignal>()
        );
        Assert.Equal(50.0, signal.Progress.Percentage);
        Assert.Equal("Validation", signal.Progress.Message);
        Assert.Equal("2 of 4", signal.Progress.CurrentStep);
    }

    [Fact]
    public async Task ReportStepAsync_WithZeroTotalSteps_ReturnsZeroPercentage()
    {
        // Arrange
        var reporter = new ProgressReporter("task-1", "test.task", _backend, _signalDispatcher);

        // Act
        await reporter.ReportStepAsync(0, 0);

        // Assert
        var signal = Assert.Single(
            _signalDispatcher.ReceivedSignals.OfType<ProgressUpdatedSignal>()
        );
        Assert.Equal(0, signal.Progress.Percentage);
    }

    [Fact]
    public async Task ReportAsync_WithoutSignalDispatcher_DoesNotThrow()
    {
        // Arrange
        var reporter = new ProgressReporter("task-1", "test.task", _backend);

        // Act & Assert - should not throw
        await reporter.ReportAsync(50.0, "No dispatcher");

        // Verify backend state still updated
        var state = await _backend.GetStateAsync("task-1");
        Assert.NotNull(state);
        Assert.Equal(TaskState.Progress, state);
    }

    private sealed class TestSignalDispatcher : ITaskSignalDispatcher
    {
        public List<ITaskSignal> ReceivedSignals { get; } = [];

        public ValueTask DispatchAsync<TSignal>(
            TSignal signal,
            CancellationToken cancellationToken = default
        )
            where TSignal : ITaskSignal
        {
            ReceivedSignals.Add(signal);
            return ValueTask.CompletedTask;
        }
    }
}
