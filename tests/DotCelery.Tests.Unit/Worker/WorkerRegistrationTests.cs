using DotCelery.Core.Abstractions;
using DotCelery.Core.Extensions;
using DotCelery.Core.Security;
using DotCelery.Worker.Extensions;
using DotCelery.Worker.Registry;
using Microsoft.Extensions.DependencyInjection;

namespace DotCelery.Tests.Unit.Worker;

/// <summary>
/// Tests for task registration through <see cref="WorkerServiceCollectionExtensions.AddWorker"/>.
/// </summary>
public sealed class WorkerRegistrationTests
{
    [Fact]
    public void AddWorker_TaskRegisteredBeforeAddWorker_IsRegistered()
    {
        var services = new ServiceCollection();

        services.AddDotCelery(builder =>
        {
            builder.AddTask<FirstTask>();
            builder.AddWorker();
        });

        using var provider = services.BuildServiceProvider();
        var registry = provider.GetRequiredService<TaskRegistry>();

        Assert.True(registry.IsRegistered(FirstTask.TaskName));
    }

    [Fact]
    public void AddWorker_TaskRegisteredAfterAddWorker_IsRegistered()
    {
        var services = new ServiceCollection();

        services.AddDotCelery(builder =>
        {
            builder.AddWorker();
            builder.AddTask<SecondTask>();
        });

        using var provider = services.BuildServiceProvider();
        var registry = provider.GetRequiredService<TaskRegistry>();

        Assert.True(registry.IsRegistered(SecondTask.TaskName));
    }

    [Fact]
    public void AddWorker_TasksRegisteredOnBothSidesOfAddWorker_AllRegistered()
    {
        var services = new ServiceCollection();

        services.AddDotCelery(builder =>
        {
            builder.AddTask<FirstTask>();
            builder.AddWorker();
            builder.AddTask<SecondTask>();
        });

        using var provider = services.BuildServiceProvider();
        var registry = provider.GetRequiredService<TaskRegistry>();

        Assert.True(registry.IsRegistered(FirstTask.TaskName));
        Assert.True(registry.IsRegistered(SecondTask.TaskName));
    }

    [Fact]
    public void AddWorker_RegistersDefaultMessageSecurityValidation()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDotCelery(builder => builder.AddWorker());

        using var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetService<IMessageSecurityValidator>());
    }

    [Fact]
    public void AddTask_TaskNameIsNull_ThrowsAtRegistration()
    {
        var services = new ServiceCollection();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            services.AddDotCelery(builder => builder.AddTask<NullTaskNameTask>())
        );

        Assert.Contains(nameof(NullTaskNameTask), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddTask_TaskNameIsEmpty_ThrowsAtRegistration()
    {
        var services = new ServiceCollection();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            services.AddDotCelery(builder => builder.AddTask<EmptyTaskNameTask>())
        );

        Assert.Contains(nameof(EmptyTaskNameTask), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddTasksFromAssembly_TaskNameIsNull_ThrowsAtRegistration()
    {
        var services = new ServiceCollection();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            services.AddDotCelery(builder =>
                builder.AddTasksFromAssembly(typeof(NullTaskNameTask).Assembly)
            )
        );

        Assert.Contains("TaskName", ex.Message, StringComparison.Ordinal);
    }

    private sealed class FirstTask : ITask
    {
        public static string TaskName => "test.registration.first";
    }

    private sealed class SecondTask : ITask
    {
        public static string TaskName => "test.registration.second";
    }

    private sealed class NullTaskNameTask : ITask
    {
        public static string TaskName => null!;
    }

    private sealed class EmptyTaskNameTask : ITask
    {
        public static string TaskName => string.Empty;
    }
}
