using System.Threading.Channels;
using DotCelery.Broker.InMemory;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Filters;
using DotCelery.Core.Models;
using DotCelery.Core.Progress;
using DotCelery.Core.Security;
using DotCelery.Worker.Filters;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
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

    [Fact]
    public async Task InMemoryBroker_WithMessageSigning_AddsSignatureAndRawBody()
    {
        var securityOptions = Options.Create(
            new MessageSecurityOptions
            {
                EnableMessageSigning = true,
                SigningKey = "0123456789abcdef0123456789abcdef"u8.ToArray(),
            }
        );
        using var validator = new MessageSecurityValidator(
            securityOptions,
            NullLogger<MessageSecurityValidator>.Instance
        );
        var broker = new InMemoryBroker(Options.Create(new InMemoryBrokerOptions()), validator);
        await broker.PublishAsync(CreateTestMessage());

        BrokerMessage? received = null;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        await foreach (var message in broker.ConsumeAsync(["test-queue"], cts.Token))
        {
            received = message;
            break;
        }

        Assert.NotNull(received);
        Assert.NotNull(received.RawBody);
        Assert.NotNull(received.Signature);
        Assert.True(validator.VerifySignature(received.RawBody, received.Signature));

        await broker.DisposeAsync();
    }

    [Fact]
    public async Task SecurityValidationFilter_WithSigningEnabled_RejectsUnsignedMessage()
    {
        var filter = new SecurityValidationFilter(
            Options.Create(
                new MessageSecurityOptions
                {
                    EnableMessageSigning = true,
                    SigningKey = "0123456789abcdef0123456789abcdef"u8.ToArray(),
                }
            ),
            NullLogger<SecurityValidationFilter>.Instance,
            new MessageSecurityValidator(
                Options.Create(
                    new MessageSecurityOptions
                    {
                        EnableMessageSigning = true,
                        SigningKey = "0123456789abcdef0123456789abcdef"u8.ToArray(),
                    }
                ),
                NullLogger<MessageSecurityValidator>.Instance
            )
        );
        var context = new TaskExecutingContext
        {
            TaskId = "task-1",
            TaskName = "test.task",
            Message = CreateTestMessage(),
            Input = null,
            TaskType = typeof(object),
            TaskContext = SubstituteTaskContext.Instance,
            ServiceProvider = new ServiceCollection().BuildServiceProvider(),
        };

        await filter.OnExecutingAsync(context, CancellationToken.None);

        Assert.True(context.SkipExecution);
        Assert.NotNull(context.SkipResult);
        Assert.Equal(TaskState.Rejected, context.SkipResult.State);
    }

    #endregion

    private static TaskMessage CreateTestMessage() =>
        new()
        {
            Id = "task-1",
            Task = "test.task",
            Args = "{}"u8.ToArray(),
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Queue = "test-queue",
        };

    private sealed class SubstituteTaskContext : ITaskContext
    {
        public static SubstituteTaskContext Instance { get; } = new();
        public string TaskId => "task-1";
        public string TaskName => "test.task";
        public int RetryCount => 0;
        public int MaxRetries => 3;
        public string Queue => "test-queue";
        public DateTimeOffset SentAt => DateTimeOffset.UtcNow;
        public DateTimeOffset? Eta => null;
        public DateTimeOffset? Expires => null;
        public string? ParentId => null;
        public string? RootId => null;
        public string? CorrelationId => null;
        public string? TenantId => null;
        public string? PartitionKey => null;
        public IReadOnlyDictionary<string, string>? Headers => null;
        public IProgressReporter Progress =>
            throw new NotSupportedException();

        public void Retry(TimeSpan? countdown = null, Exception? exception = null) =>
            throw new NotSupportedException();

        public Task UpdateStateAsync(TaskState state, object? metadata = null) =>
            Task.CompletedTask;

        public T GetRequiredService<T>()
            where T : notnull => throw new NotSupportedException();
    }
}
