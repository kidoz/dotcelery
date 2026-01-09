using DotCelery.Core.Signals;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace DotCelery.Tests.Unit.Signals;

/// <summary>
/// Tests for <see cref="TaskSignalDispatcher"/>.
/// </summary>
public sealed class TaskSignalDispatcherTests
{
    [Fact]
    public async Task DispatchAsync_WithNoHandlers_DoesNotThrow()
    {
        // Arrange
        var services = new ServiceCollection();
        var provider = services.BuildServiceProvider();
        var dispatcher = new TaskSignalDispatcher(
            provider,
            NullLogger<TaskSignalDispatcher>.Instance
        );

        var signal = new TaskSuccessSignal
        {
            TaskId = "task-1",
            TaskName = "test.task",
            Timestamp = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromSeconds(1),
        };

        // Act & Assert - should not throw
        await dispatcher.DispatchAsync(signal, CancellationToken.None);
    }

    [Fact]
    public async Task DispatchAsync_WithHandler_InvokesHandler()
    {
        // Arrange
        var services = new ServiceCollection();
        var testHandler = new TestSignalHandler();
        services.AddSingleton<ITaskSignalHandler<TaskSuccessSignal>>(testHandler);

        var provider = services.BuildServiceProvider();
        var dispatcher = new TaskSignalDispatcher(
            provider,
            NullLogger<TaskSignalDispatcher>.Instance
        );

        var signal = new TaskSuccessSignal
        {
            TaskId = "task-1",
            TaskName = "test.task",
            Timestamp = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromSeconds(1),
            Result = "test-result",
        };

        // Act
        await dispatcher.DispatchAsync(signal, CancellationToken.None);

        // Assert
        Assert.Single(testHandler.ReceivedSignals);
        Assert.Equal("task-1", testHandler.ReceivedSignals[0].TaskId);
    }

    [Fact]
    public async Task DispatchAsync_WithMultipleHandlers_InvokesAll()
    {
        // Arrange
        var services = new ServiceCollection();
        var handler1 = new TestSignalHandler();
        var handler2 = new TestSignalHandler();
        services.AddSingleton<ITaskSignalHandler<TaskSuccessSignal>>(handler1);
        services.AddSingleton<ITaskSignalHandler<TaskSuccessSignal>>(handler2);

        var provider = services.BuildServiceProvider();
        var dispatcher = new TaskSignalDispatcher(
            provider,
            NullLogger<TaskSignalDispatcher>.Instance
        );

        var signal = new TaskSuccessSignal
        {
            TaskId = "task-1",
            TaskName = "test.task",
            Timestamp = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromSeconds(1),
        };

        // Act
        await dispatcher.DispatchAsync(signal, CancellationToken.None);

        // Assert
        Assert.Single(handler1.ReceivedSignals);
        Assert.Single(handler2.ReceivedSignals);
    }

    [Fact]
    public async Task DispatchAsync_WithDifferentSignalTypes_RoutesCorrectly()
    {
        // Arrange
        var services = new ServiceCollection();
        var successHandler = new TestSignalHandler();
        var failureHandler = new TestFailureSignalHandler();
        services.AddSingleton<ITaskSignalHandler<TaskSuccessSignal>>(successHandler);
        services.AddSingleton<ITaskSignalHandler<TaskFailureSignal>>(failureHandler);

        var provider = services.BuildServiceProvider();
        var dispatcher = new TaskSignalDispatcher(
            provider,
            NullLogger<TaskSignalDispatcher>.Instance
        );

        var successSignal = new TaskSuccessSignal
        {
            TaskId = "task-1",
            TaskName = "test.task",
            Timestamp = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromSeconds(1),
        };

        var failureSignal = new TaskFailureSignal
        {
            TaskId = "task-2",
            TaskName = "test.task",
            Timestamp = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromSeconds(1),
            Exception = new InvalidOperationException("test"),
        };

        // Act
        await dispatcher.DispatchAsync(successSignal, CancellationToken.None);
        await dispatcher.DispatchAsync(failureSignal, CancellationToken.None);

        // Assert
        Assert.Single(successHandler.ReceivedSignals);
        Assert.Single(failureHandler.ReceivedSignals);
        Assert.Equal("task-1", successHandler.ReceivedSignals[0].TaskId);
        Assert.Equal("task-2", failureHandler.ReceivedSignals[0].TaskId);
    }

    [Fact]
    public async Task DispatchAsync_HandlerThrows_ContinuesWithOtherHandlers()
    {
        // Arrange
        var services = new ServiceCollection();
        var throwingHandler = new ThrowingSignalHandler();
        var normalHandler = new TestSignalHandler();
        services.AddSingleton<ITaskSignalHandler<TaskSuccessSignal>>(throwingHandler);
        services.AddSingleton<ITaskSignalHandler<TaskSuccessSignal>>(normalHandler);

        var provider = services.BuildServiceProvider();
        var dispatcher = new TaskSignalDispatcher(
            provider,
            NullLogger<TaskSignalDispatcher>.Instance
        );

        var signal = new TaskSuccessSignal
        {
            TaskId = "task-1",
            TaskName = "test.task",
            Timestamp = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromSeconds(1),
        };

        // Act - should not throw, errors are logged
        await dispatcher.DispatchAsync(signal, CancellationToken.None);

        // Assert - second handler still executed
        Assert.Single(normalHandler.ReceivedSignals);
    }

    [Fact]
    public async Task DispatchAsync_WithScopedHandler_ResolvesFromScope()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddScoped<ITaskSignalHandler<TaskSuccessSignal>, TestSignalHandler>();

        var provider = services.BuildServiceProvider();
        var dispatcher = new TaskSignalDispatcher(
            provider,
            NullLogger<TaskSignalDispatcher>.Instance
        );

        var signal = new TaskSuccessSignal
        {
            TaskId = "task-1",
            TaskName = "test.task",
            Timestamp = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromSeconds(1),
        };

        // Act & Assert - should work with scoped services
        await dispatcher.DispatchAsync(signal, CancellationToken.None);
    }

    [Fact]
    public void NullDispatcher_DoesNotThrow()
    {
        // Arrange
        var dispatcher = NullTaskSignalDispatcher.Instance;

        var signal = new TaskSuccessSignal
        {
            TaskId = "task-1",
            TaskName = "test.task",
            Timestamp = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromSeconds(1),
        };

        // Act & Assert - should not throw
        var task = dispatcher.DispatchAsync(signal, CancellationToken.None);
        Assert.True(task.IsCompletedSuccessfully);
    }

    private sealed class TestSignalHandler : ITaskSignalHandler<TaskSuccessSignal>
    {
        public List<TaskSuccessSignal> ReceivedSignals { get; } = [];

        public ValueTask HandleAsync(TaskSuccessSignal signal, CancellationToken cancellationToken)
        {
            ReceivedSignals.Add(signal);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class TestFailureSignalHandler : ITaskSignalHandler<TaskFailureSignal>
    {
        public List<TaskFailureSignal> ReceivedSignals { get; } = [];

        public ValueTask HandleAsync(TaskFailureSignal signal, CancellationToken cancellationToken)
        {
            ReceivedSignals.Add(signal);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ThrowingSignalHandler : ITaskSignalHandler<TaskSuccessSignal>
    {
        public ValueTask HandleAsync(TaskSuccessSignal signal, CancellationToken cancellationToken)
        {
            throw new InvalidOperationException("Handler error");
        }
    }
}
