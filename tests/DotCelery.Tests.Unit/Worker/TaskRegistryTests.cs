using DotCelery.Core.Abstractions;
using DotCelery.Worker.Registry;

namespace DotCelery.Tests.Unit.Worker;

public class TaskRegistryTests
{
    [Fact]
    public void Register_GenericVersion_AddsToRegistry()
    {
        var registry = new TaskRegistry();

        registry.Register<TestTask>();

        Assert.True(registry.IsRegistered(TestTask.TaskName));
        var registration = registry.GetTask(TestTask.TaskName);
        Assert.NotNull(registration);
        Assert.Equal(typeof(TestTask), registration.TaskType);
    }

    [Fact]
    public void Register_ByType_AddsToRegistry()
    {
        var registry = new TaskRegistry();

        registry.Register(typeof(TestTask), "custom.task.name");

        Assert.True(registry.IsRegistered("custom.task.name"));
        var registration = registry.GetTask("custom.task.name");
        Assert.NotNull(registration);
        Assert.Equal(typeof(TestTask), registration.TaskType);
    }

    [Fact]
    public void Register_DuplicateTask_OverwritesExisting()
    {
        var registry = new TaskRegistry();
        registry.Register<TestTask>();
        registry.Register<TestTask>(); // Should overwrite, not throw

        Assert.True(registry.IsRegistered(TestTask.TaskName));
    }

    [Fact]
    public void GetTask_NonExistentTask_ReturnsNull()
    {
        var registry = new TaskRegistry();

        var result = registry.GetTask("non.existent.task");

        Assert.Null(result);
    }

    [Fact]
    public void GetAllTasks_ReturnsAllRegistered()
    {
        var registry = new TaskRegistry();
        registry.Register<TestTask>();
        registry.Register<AnotherTestTask>();

        var tasks = registry.GetAllTasks();

        Assert.Equal(2, tasks.Count);
        Assert.Contains(TestTask.TaskName, tasks.Keys);
        Assert.Contains(AnotherTestTask.TaskName, tasks.Keys);
    }

    [Fact]
    public void GetAllTasks_EmptyRegistry_ReturnsEmpty()
    {
        var registry = new TaskRegistry();

        var tasks = registry.GetAllTasks();

        Assert.Empty(tasks);
    }

    [Fact]
    public void IsRegistered_RegisteredTask_ReturnsTrue()
    {
        var registry = new TaskRegistry();
        registry.Register<TestTask>();

        Assert.True(registry.IsRegistered(TestTask.TaskName));
    }

    [Fact]
    public void IsRegistered_UnregisteredTask_ReturnsFalse()
    {
        var registry = new TaskRegistry();

        Assert.False(registry.IsRegistered("non.existent.task"));
    }

    [Fact]
    public void Register_TaskWithInputOutput_RegistersCorrectTypes()
    {
        var registry = new TaskRegistry();

        registry.Register<TestTask>();

        var registration = registry.GetTask(TestTask.TaskName);
        Assert.NotNull(registration);
        Assert.Equal(typeof(TestInput), registration.InputType);
        Assert.Equal(typeof(TestOutput), registration.OutputType);
    }

    [Fact]
    public void Register_TaskWithInputOnly_RegistersCorrectTypes()
    {
        var registry = new TaskRegistry();

        registry.Register<TaskWithInputOnly>();

        var registration = registry.GetTask(TaskWithInputOnly.TaskName);
        Assert.NotNull(registration);
        Assert.Equal(typeof(TestInput), registration.InputType);
        Assert.Null(registration.OutputType);
    }

    private sealed class TestTask : ITask<TestInput, TestOutput>
    {
        public static string TaskName => "test.task";

        public Task<TestOutput> ExecuteAsync(
            TestInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            return Task.FromResult(new TestOutput { Result = input.Value * 2 });
        }
    }

    private sealed class AnotherTestTask : ITask<TestInput, TestOutput>
    {
        public static string TaskName => "another.test.task";

        public Task<TestOutput> ExecuteAsync(
            TestInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            return Task.FromResult(new TestOutput { Result = input.Value });
        }
    }

    private sealed class TaskWithInputOnly : ITask<TestInput>
    {
        public static string TaskName => "task.input.only";

        public Task ExecuteAsync(
            TestInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            return Task.CompletedTask;
        }
    }

    private sealed class TestInput
    {
        public int Value { get; set; }
    }

    private sealed class TestOutput
    {
        public int Result { get; set; }
    }
}
