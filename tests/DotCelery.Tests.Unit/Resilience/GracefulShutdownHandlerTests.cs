using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Worker.Services;
using Microsoft.Extensions.Logging.Abstractions;

namespace DotCelery.Tests.Unit.Resilience;

/// <summary>
/// Tests for <see cref="GracefulShutdownHandler"/>.
/// </summary>
public sealed class GracefulShutdownHandlerTests
{
    private readonly GracefulShutdownHandler _handler;

    public GracefulShutdownHandlerTests()
    {
        _handler = new GracefulShutdownHandler(NullLogger<GracefulShutdownHandler>.Instance);
    }

    [Fact]
    public void ActiveTaskCount_Initially_ReturnsZero()
    {
        Assert.Equal(0, _handler.ActiveTaskCount);
    }

    [Fact]
    public void IsShuttingDown_Initially_ReturnsFalse()
    {
        Assert.False(_handler.IsShuttingDown);
    }

    [Fact]
    public void RegisterTask_IncrementsActiveTaskCount()
    {
        var message = CreateBrokerMessage("task-1");

        using var registration = _handler.RegisterTask("task-1", message);

        Assert.Equal(1, _handler.ActiveTaskCount);
    }

    [Fact]
    public void RegisterTask_WhenDisposed_DecrementsActiveTaskCount()
    {
        var message = CreateBrokerMessage("task-1");

        var registration = _handler.RegisterTask("task-1", message);
        Assert.Equal(1, _handler.ActiveTaskCount);

        registration.Dispose();
        Assert.Equal(0, _handler.ActiveTaskCount);
    }

    [Fact]
    public void RegisterTask_MultipleTasks_TracksCorrectly()
    {
        var message1 = CreateBrokerMessage("task-1");
        var message2 = CreateBrokerMessage("task-2");
        var message3 = CreateBrokerMessage("task-3");

        using var reg1 = _handler.RegisterTask("task-1", message1);
        using var reg2 = _handler.RegisterTask("task-2", message2);
        using var reg3 = _handler.RegisterTask("task-3", message3);

        Assert.Equal(3, _handler.ActiveTaskCount);
    }

    [Fact]
    public async Task ShutdownAsync_WithNoActiveTasks_CompletesImmediately()
    {
        var result = await _handler.ShutdownAsync(TimeSpan.FromSeconds(5));

        Assert.True(result.CompletedGracefully);
        Assert.Equal(0, result.TotalTasks);
        Assert.Equal(0, result.CompletedTasks);
        Assert.Equal(0, result.CancelledTasks);
    }

    [Fact]
    public async Task ShutdownAsync_WhenAlreadyShuttingDown_ThrowsInvalidOperationException()
    {
        var message = CreateBrokerMessage("task-1");
        using var registration = _handler.RegisterTask("task-1", message);

        // Start shutdown
        _ = _handler.ShutdownAsync(TimeSpan.FromSeconds(30));

        // Wait a bit for state to set
        await Task.Delay(10, CancellationToken.None);

        // Second call should throw
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _handler.ShutdownAsync(TimeSpan.FromSeconds(5))
        );
    }

    [Fact]
    public async Task ShutdownAsync_WithActiveTasks_WaitsForCompletion()
    {
        var message = CreateBrokerMessage("task-1");
        var registration = _handler.RegisterTask("task-1", message);

        // Start shutdown and complete task after delay
        var shutdownTask = _handler.ShutdownAsync(TimeSpan.FromSeconds(10));

        await Task.Delay(50, CancellationToken.None);
        registration.Dispose();

        var result = await shutdownTask;

        Assert.True(result.CompletedGracefully);
        Assert.Equal(1, result.TotalTasks);
        Assert.Equal(1, result.CompletedTasks);
        Assert.Equal(0, result.CancelledTasks);
    }

    [Fact]
    public async Task ShutdownAsync_WithTimeout_ReturnsCancelledTasks()
    {
        var message = CreateBrokerMessage("task-1");
        using var registration = _handler.RegisterTask("task-1", message);

        var result = await _handler.ShutdownAsync(TimeSpan.FromMilliseconds(50));

        Assert.False(result.CompletedGracefully);
        Assert.Equal(1, result.TotalTasks);
        Assert.Equal(0, result.CompletedTasks);
        Assert.Equal(1, result.CancelledTasks);
    }

    [Fact]
    public void GetPendingMessages_ReturnsAllActiveMessages()
    {
        var message1 = CreateBrokerMessage("task-1");
        var message2 = CreateBrokerMessage("task-2");

        using var reg1 = _handler.RegisterTask("task-1", message1);
        using var reg2 = _handler.RegisterTask("task-2", message2);

        var pendingMessages = _handler.GetPendingMessages();

        Assert.Equal(2, pendingMessages.Count);
    }

    [Fact]
    public void GetPendingMessages_AfterTaskComplete_ExcludesCompletedTask()
    {
        var message1 = CreateBrokerMessage("task-1");
        var message2 = CreateBrokerMessage("task-2");

        var reg1 = _handler.RegisterTask("task-1", message1);
        using var reg2 = _handler.RegisterTask("task-2", message2);

        reg1.Dispose(); // Complete task-1

        var pendingMessages = _handler.GetPendingMessages();

        Assert.Single(pendingMessages);
        Assert.Equal("task-2", pendingMessages.First().Message.Id);
    }

    private static BrokerMessage CreateBrokerMessage(string taskId)
    {
        return new BrokerMessage
        {
            Message = new TaskMessage
            {
                Id = taskId,
                Task = "test.task",
                Args = [],
                ContentType = "application/json",
                Timestamp = DateTimeOffset.UtcNow,
            },
            DeliveryTag = Guid.NewGuid(),
            Queue = "test-queue",
            ReceivedAt = DateTimeOffset.UtcNow,
        };
    }
}
