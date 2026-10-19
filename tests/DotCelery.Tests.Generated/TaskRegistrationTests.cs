using DotCelery.Core.Abstractions;
using DotCelery.Core.Extensions;
using DotCelery.Generated;
using DotCelery.Worker.Extensions;
using DotCelery.Worker.Registry;
using Microsoft.Extensions.DependencyInjection;

namespace DotCelery.Tests.Generated;

/// <summary>
/// Proves the source-generated registration through the real compiler: it registers the
/// accessible tasks under their static names, and the worker's registry is built from those
/// names without reflection.
/// </summary>
public sealed class TaskRegistrationTests
{
    [Fact]
    public void GeneratedRegistration_RegistersTheAccessibleTasks()
    {
        var builder = new DotCeleryBuilder(new ServiceCollection());

        builder.RegisterTasks();

        Assert.Equal(
            ["generated.first", "generated.nested", "generated.second"],
            builder.TaskRegistrations.Select(task => task.TaskName).Order(StringComparer.Ordinal)
        );
    }

    [Fact]
    public void GeneratedRegistration_SkipsATaskItCannotReference()
    {
        var builder = new DotCeleryBuilder(new ServiceCollection());

        builder.RegisterTasks();

        // The hidden task is private, so a generated reference to it would not compile
        Assert.DoesNotContain(
            builder.TaskRegistrations,
            task => task.TaskName == HiddenTask.TaskName
        );
    }

    [Fact]
    public void GeneratedRegistration_MatchesTheAssemblyScanForTheAccessibleTasks()
    {
        var generated = new DotCeleryBuilder(new ServiceCollection());
        generated.RegisterTasks();

        var scanned = new DotCeleryBuilder(new ServiceCollection());
        scanned.AddTasksFromAssembly(typeof(FirstTask).Assembly);

        // The scan sees private tasks the generated code cannot reference; everything else matches
        Assert.Equal(
            generated.TaskRegistrations.Select(task => task.TaskName).Order(StringComparer.Ordinal),
            scanned
                .TaskRegistrations.Select(task => task.TaskName)
                .Where(name => name != HiddenTask.TaskName)
                .Order(StringComparer.Ordinal)
        );
    }

    [Fact]
    public void AddWorker_BuildsTheRegistryFromTheGeneratedRegistrations()
    {
        var services = new ServiceCollection();

        services.AddDotCelery(builder => builder.RegisterTasks().AddWorker());

        using var provider = services.BuildServiceProvider();
        var registry = provider.GetRequiredService<TaskRegistry>();

        Assert.True(registry.IsRegistered("generated.first"));
        Assert.True(registry.IsRegistered("generated.nested"));
        Assert.True(registry.IsRegistered("generated.second"));
        Assert.False(registry.IsRegistered(HiddenTask.TaskName));
    }

    private sealed class HiddenTask : ITask<OrderInput>
    {
        public static string TaskName => "generated.hidden";

        public Task ExecuteAsync(
            OrderInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        ) => Task.CompletedTask;
    }
}
