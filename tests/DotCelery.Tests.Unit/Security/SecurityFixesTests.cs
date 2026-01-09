using System.Threading.Channels;
using DotCelery.Broker.InMemory;
using DotCelery.Core.Models;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Unit.Security;

/// <summary>
/// Tests for security and architecture fixes.
/// </summary>
public class SecurityFixesTests
{
    #region TaskStateValidator Tests

    [Fact]
    public void TaskStateValidator_ValidTransition_ReturnsTrue()
    {
        // Valid transitions
        Assert.True(TaskStateValidator.IsValidTransition(TaskState.Pending, TaskState.Received));
        Assert.True(TaskStateValidator.IsValidTransition(TaskState.Received, TaskState.Started));
        Assert.True(TaskStateValidator.IsValidTransition(TaskState.Started, TaskState.Success));
        Assert.True(TaskStateValidator.IsValidTransition(TaskState.Started, TaskState.Failure));
        Assert.True(TaskStateValidator.IsValidTransition(TaskState.Started, TaskState.Retry));
        Assert.True(TaskStateValidator.IsValidTransition(TaskState.Retry, TaskState.Received));
    }

    [Fact]
    public void TaskStateValidator_InvalidTransition_ReturnsFalse()
    {
        // Invalid transitions
        Assert.False(TaskStateValidator.IsValidTransition(TaskState.Success, TaskState.Started));
        Assert.False(TaskStateValidator.IsValidTransition(TaskState.Failure, TaskState.Success));
        Assert.False(TaskStateValidator.IsValidTransition(TaskState.Pending, TaskState.Success));
        Assert.False(TaskStateValidator.IsValidTransition(TaskState.Received, TaskState.Failure));
    }

    [Fact]
    public void TaskStateValidator_SameState_ReturnsTrue()
    {
        // Same state transitions are always valid (idempotent)
        foreach (TaskState state in Enum.GetValues<TaskState>())
        {
            Assert.True(TaskStateValidator.IsValidTransition(state, state));
        }
    }

    [Fact]
    public void TaskStateValidator_TerminalStates_NoValidTransitions()
    {
        var terminalStates = new[]
        {
            TaskState.Success,
            TaskState.Failure,
            TaskState.Revoked,
            TaskState.Rejected,
        };

        foreach (var state in terminalStates)
        {
            Assert.True(TaskStateValidator.IsTerminal(state));
            var validTransitions = TaskStateValidator.GetValidTransitions(state);
            Assert.Empty(validTransitions);
        }
    }

    [Fact]
    public void TaskStateValidator_ValidateTransition_ThrowsOnInvalid()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            TaskStateValidator.ValidateTransition(TaskState.Success, TaskState.Started)
        );

        Assert.Contains("Invalid state transition", ex.Message);
        Assert.Contains("Success", ex.Message);
        Assert.Contains("Started", ex.Message);
    }

    [Fact]
    public void TaskStateValidator_NullCurrentState_OnlyPendingOrReceivedValid()
    {
        Assert.True(TaskStateValidator.IsValidTransition(null, TaskState.Pending));
        Assert.True(TaskStateValidator.IsValidTransition(null, TaskState.Received));
        Assert.False(TaskStateValidator.IsValidTransition(null, TaskState.Started));
        Assert.False(TaskStateValidator.IsValidTransition(null, TaskState.Success));
    }

    [Fact]
    public void TaskStateValidator_ProgressState_CanTransitionToTerminal()
    {
        Assert.True(TaskStateValidator.IsValidTransition(TaskState.Progress, TaskState.Success));
        Assert.True(TaskStateValidator.IsValidTransition(TaskState.Progress, TaskState.Failure));
        Assert.True(TaskStateValidator.IsValidTransition(TaskState.Progress, TaskState.Progress));
    }

    [Fact]
    public void TaskStateValidator_RequeueState_CanTransitionToReceived()
    {
        Assert.True(TaskStateValidator.IsValidTransition(TaskState.Requeued, TaskState.Received));
        Assert.True(TaskStateValidator.IsValidTransition(TaskState.Requeued, TaskState.Revoked));
        Assert.False(TaskStateValidator.IsValidTransition(TaskState.Requeued, TaskState.Started));
    }

    #endregion

    #region InMemoryBroker Bounded Queue Tests

    [Fact]
    public async Task InMemoryBroker_BoundedQueue_RespectsCapacity()
    {
        var options = Options.Create(
            new InMemoryBrokerOptions
            {
                MaxQueueCapacity = 5,
                FullMode = BoundedChannelFullMode.DropWrite,
            }
        );
        var broker = new InMemoryBroker(options);

        // Publish messages up to capacity
        for (int i = 0; i < 10; i++)
        {
            var message = new TaskMessage
            {
                Id = $"task-{i}",
                Task = "test.task",
                Queue = "test-queue",
                Timestamp = DateTimeOffset.UtcNow,
                Args = [],
                ContentType = "application/json",
            };
            await broker.PublishAsync(message);
        }

        // With DropWrite mode, extra messages should be dropped
        var queueLength = broker.GetQueueLength("test-queue");
        Assert.True(queueLength <= 5, $"Queue should have at most 5 messages, got {queueLength}");

        await broker.DisposeAsync();
    }

    [Fact]
    public async Task InMemoryBroker_UnboundedQueue_WhenCapacityNull()
    {
        var options = Options.Create(new InMemoryBrokerOptions { MaxQueueCapacity = null });
        var broker = new InMemoryBroker(options);

        // Should be able to publish many messages with unbounded queue
        for (int i = 0; i < 100; i++)
        {
            var message = new TaskMessage
            {
                Id = $"task-{i}",
                Task = "test.task",
                Queue = "test-queue",
                Timestamp = DateTimeOffset.UtcNow,
                Args = [],
                ContentType = "application/json",
            };
            await broker.PublishAsync(message);
        }

        var queueLength = broker.GetQueueLength("test-queue");
        Assert.Equal(100, queueLength);

        await broker.DisposeAsync();
    }

    [Fact]
    public void InMemoryBroker_DefaultOptions_HasBoundedCapacity()
    {
        var options = new InMemoryBrokerOptions();
        Assert.NotNull(options.MaxQueueCapacity);
        Assert.Equal(10000, options.MaxQueueCapacity);
    }

    [Fact]
    public void InMemoryBroker_DefaultOptions_UsesWaitFullMode()
    {
        var options = new InMemoryBrokerOptions();
        Assert.Equal(BoundedChannelFullMode.Wait, options.FullMode);
    }

    #endregion
}
