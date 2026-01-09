using DotCelery.Core.Abstractions;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Models;
using DotCelery.Worker.DeadLetter;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace DotCelery.Tests.Unit.DeadLetter;

/// <summary>
/// Tests for <see cref="DeadLetterHandler"/>.
/// </summary>
public sealed class DeadLetterHandlerTests
{
    private readonly IDeadLetterStore _store = Substitute.For<IDeadLetterStore>();
    private readonly IMessageSerializer _serializer = Substitute.For<IMessageSerializer>();
    private readonly DeadLetterHandler _handler;

    public DeadLetterHandlerTests()
    {
        _serializer.Serialize(Arg.Any<TaskMessage>()).Returns([1, 2, 3]);
        var options = Options.Create(new DeadLetterOptions { Enabled = true });
        _handler = new DeadLetterHandler(
            _serializer,
            options,
            NullLogger<DeadLetterHandler>.Instance,
            _store
        );
    }

    [Fact]
    public async Task HandleAsync_WithMaxRetriesExceeded_StoresMessage()
    {
        // Arrange
        var message = CreateTaskMessage("task-1", retries: 3, maxRetries: 3);
        var exception = new InvalidOperationException("Task failed");

        // Act
        await _handler.HandleAsync(
            message,
            DeadLetterReason.MaxRetriesExceeded,
            exception,
            cancellationToken: CancellationToken.None
        );

        // Assert
        await _store
            .Received(1)
            .StoreAsync(
                Arg.Is<DeadLetterMessage>(m =>
                    m.TaskId == "task-1"
                    && m.Reason == DeadLetterReason.MaxRetriesExceeded
                    && m.ExceptionType == typeof(InvalidOperationException).FullName
                ),
                Arg.Any<CancellationToken>()
            );
    }

    [Fact]
    public async Task HandleAsync_WithRejectedReason_StoresMessage()
    {
        // Arrange
        var message = CreateTaskMessage("task-1");
        var exception = new InvalidOperationException("Task rejected");

        // Act
        await _handler.HandleAsync(
            message,
            DeadLetterReason.Rejected,
            exception,
            cancellationToken: CancellationToken.None
        );

        // Assert
        await _store
            .Received(1)
            .StoreAsync(
                Arg.Is<DeadLetterMessage>(m => m.Reason == DeadLetterReason.Rejected),
                Arg.Any<CancellationToken>()
            );
    }

    [Fact]
    public async Task HandleAsync_WithTimeLimitExceeded_StoresMessage()
    {
        // Arrange
        var message = CreateTaskMessage("task-1");
        var exception = new OperationCanceledException("Time limit exceeded");

        // Act
        await _handler.HandleAsync(
            message,
            DeadLetterReason.TimeLimitExceeded,
            exception,
            cancellationToken: CancellationToken.None
        );

        // Assert
        await _store
            .Received(1)
            .StoreAsync(
                Arg.Is<DeadLetterMessage>(m => m.Reason == DeadLetterReason.TimeLimitExceeded),
                Arg.Any<CancellationToken>()
            );
    }

    [Fact]
    public async Task HandleAsync_WithUnknownTask_StoresMessage()
    {
        // Arrange
        var message = CreateTaskMessage("unknown.task");

        // Act
        await _handler.HandleAsync(
            message,
            DeadLetterReason.UnknownTask,
            cancellationToken: CancellationToken.None
        );

        // Assert
        await _store
            .Received(1)
            .StoreAsync(
                Arg.Is<DeadLetterMessage>(m =>
                    m.Reason == DeadLetterReason.UnknownTask && m.ExceptionType == null
                ),
                Arg.Any<CancellationToken>()
            );
    }

    [Fact]
    public async Task HandleAsync_WithExpiredMessage_StoresMessage()
    {
        // Arrange
        var message = CreateTaskMessage("task-1", expires: DateTimeOffset.UtcNow.AddMinutes(-1));

        // Act
        await _handler.HandleAsync(
            message,
            DeadLetterReason.Expired,
            cancellationToken: CancellationToken.None
        );

        // Assert
        await _store
            .Received(1)
            .StoreAsync(
                Arg.Is<DeadLetterMessage>(m => m.Reason == DeadLetterReason.Expired),
                Arg.Any<CancellationToken>()
            );
    }

    [Fact]
    public async Task HandleAsync_IncludesAllMessageDetails()
    {
        // Arrange
        var message = new TaskMessage
        {
            Id = "task-123",
            Task = "test.task",
            Args = [1, 2, 3],
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Queue = "test-queue",
            Retries = 2,
            MaxRetries = 3,
        };
        var exception = new ArgumentException("Invalid argument", "param");

        // Act
        await _handler.HandleAsync(
            message,
            DeadLetterReason.MaxRetriesExceeded,
            exception,
            cancellationToken: CancellationToken.None
        );

        // Assert
        await _store
            .Received(1)
            .StoreAsync(
                Arg.Is<DeadLetterMessage>(m =>
                    m.TaskId == "task-123"
                    && m.TaskName == "test.task"
                    && m.Queue == "test-queue"
                    && m.ExceptionMessage == "Invalid argument (Parameter 'param')"
                ),
                Arg.Any<CancellationToken>()
            );
    }

    [Fact]
    public async Task HandleAsync_WhenDisabled_DoesNotStore()
    {
        // Arrange
        var options = Options.Create(new DeadLetterOptions { Enabled = false });
        var handler = new DeadLetterHandler(
            _serializer,
            options,
            NullLogger<DeadLetterHandler>.Instance,
            _store
        );
        var message = CreateTaskMessage("task-1");

        // Act
        await handler.HandleAsync(
            message,
            DeadLetterReason.MaxRetriesExceeded,
            new InvalidOperationException(),
            cancellationToken: CancellationToken.None
        );

        // Assert
        await _store
            .DidNotReceive()
            .StoreAsync(Arg.Any<DeadLetterMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleAsync_WhenNoStore_DoesNotThrow()
    {
        // Arrange
        var options = Options.Create(new DeadLetterOptions { Enabled = true });
        var handler = new DeadLetterHandler(
            _serializer,
            options,
            NullLogger<DeadLetterHandler>.Instance,
            store: null
        );
        var message = CreateTaskMessage("task-1");

        // Act & Assert - should not throw
        await handler.HandleAsync(
            message,
            DeadLetterReason.MaxRetriesExceeded,
            new InvalidOperationException(),
            cancellationToken: CancellationToken.None
        );
    }

    [Fact]
    public async Task HandleAsync_SetsTimestamp()
    {
        // Arrange
        var before = DateTimeOffset.UtcNow;
        var message = CreateTaskMessage("task-1");

        // Act
        await _handler.HandleAsync(
            message,
            DeadLetterReason.MaxRetriesExceeded,
            new InvalidOperationException(),
            cancellationToken: CancellationToken.None
        );
        var after = DateTimeOffset.UtcNow;

        // Assert
        await _store
            .Received(1)
            .StoreAsync(
                Arg.Is<DeadLetterMessage>(m => m.Timestamp >= before && m.Timestamp <= after),
                Arg.Any<CancellationToken>()
            );
    }

    [Fact]
    public async Task HandleAsync_WhenReasonNotConfigured_DoesNotStore()
    {
        // Arrange
        var options = Options.Create(
            new DeadLetterOptions
            {
                Enabled = true,
                Reasons = [DeadLetterReason.MaxRetriesExceeded], // Only MaxRetriesExceeded
            }
        );
        var handler = new DeadLetterHandler(
            _serializer,
            options,
            NullLogger<DeadLetterHandler>.Instance,
            _store
        );
        var message = CreateTaskMessage("task-1");

        // Act
        await handler.HandleAsync(
            message,
            DeadLetterReason.Rejected, // Not in configured reasons
            new InvalidOperationException(),
            cancellationToken: CancellationToken.None
        );

        // Assert
        await _store
            .DidNotReceive()
            .StoreAsync(Arg.Any<DeadLetterMessage>(), Arg.Any<CancellationToken>());
    }

    private static TaskMessage CreateTaskMessage(
        string taskId,
        int retries = 0,
        int maxRetries = 3,
        DateTimeOffset? expires = null
    ) =>
        new()
        {
            Id = taskId,
            Task = "test.task",
            Args = [],
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            Queue = "celery",
            Retries = retries,
            MaxRetries = maxRetries,
            Expires = expires,
        };
}
