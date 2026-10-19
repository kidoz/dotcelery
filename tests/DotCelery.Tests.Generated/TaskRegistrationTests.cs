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
            [
                "generated.echo",
                "generated.first",
                "generated.nested",
                "generated.noinput",
                "generated.second",
            ],
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

        Assert.True(registry.IsRegistered("generated.echo"));
        Assert.True(registry.IsRegistered("generated.first"));
        Assert.True(registry.IsRegistered("generated.nested"));
        Assert.True(registry.IsRegistered("generated.noinput"));
        Assert.True(registry.IsRegistered("generated.second"));
        Assert.False(registry.IsRegistered(HiddenTask.TaskName));
    }

    [Fact]
    public void GeneratedRegistration_AttachesATypedInvokerToEachTask()
    {
        var builder = new DotCeleryBuilder(new ServiceCollection());

        builder.RegisterTasks();

        Assert.All(builder.TaskRegistrations, task => Assert.NotNull(task.Invoker));
        Assert.All(
            builder.TaskRegistrations,
            task => Assert.Contains("DotCeleryInvoker", task.Invoker!.GetType().Name)
        );
    }

    [Fact]
    public void GeneratedRegistration_CarriesTheInputAndOutputTypes()
    {
        var registrations = Register();

        var echo = registrations.Single(task => task.TaskName == EchoTask.TaskName);
        Assert.Equal(typeof(OrderInput), echo.InputType);
        Assert.Equal(typeof(OrderInput), echo.OutputType);

        var first = registrations.Single(task => task.TaskName == FirstTask.TaskName);
        Assert.Equal(typeof(OrderInput), first.InputType);
        Assert.Null(first.OutputType);

        var noInput = registrations.Single(task => task.TaskName == NoInputTask.TaskName);
        Assert.Null(noInput.InputType);
        Assert.Equal(typeof(OrderInput), noInput.OutputType);
    }

    [Fact]
    public void AddWorker_BuildsTheRegistryWithTheGeneratedTypes()
    {
        var services = new ServiceCollection();

        services.AddDotCelery(builder => builder.RegisterTasks().AddWorker());

        using var provider = services.BuildServiceProvider();
        var registry = provider.GetRequiredService<TaskRegistry>();

        var echo = registry.GetTask(EchoTask.TaskName);
        Assert.NotNull(echo);
        Assert.Equal(typeof(OrderInput), echo.InputType);
        Assert.Equal(typeof(OrderInput), echo.OutputType);

        var noInput = registry.GetTask(NoInputTask.TaskName);
        Assert.NotNull(noInput);
        Assert.Null(noInput.InputType);
        Assert.Equal(typeof(OrderInput), noInput.OutputType);
    }

    [Fact]
    public void TheGeneratedHelpers_CreateTypedSignatures()
    {
        var input = new OrderInput { Value = 7 };

        var echo = DotCeleryTasks.Echo(input);
        Assert.Equal(EchoTask.TaskName, echo.TaskName);
        Assert.Same(input, echo.Input);

        var first = DotCeleryTasks.First(input);
        Assert.Equal(FirstTask.TaskName, first.TaskName);
        Assert.Same(input, first.Input);

        var noInput = DotCeleryTasks.NoInput();
        Assert.Equal(NoInputTask.TaskName, noInput.TaskName);
        Assert.Null(noInput.GetInput());
    }

    [Fact]
    public async Task TheGeneratedInvoker_InvokesATaskAndReturnsItsResult()
    {
        var registration = Register().Single(task => task.TaskName == EchoTask.TaskName);
        var input = new OrderInput { Value = 7 };

        var result = await registration.Invoker!.InvokeAsync(
            new EchoTask(),
            input,
            context: null!,
            CancellationToken.None
        );

        Assert.Same(input, result);
    }

    [Fact]
    public async Task TheGeneratedInvoker_InvokesATaskWithoutAResult()
    {
        var registration = Register().Single(task => task.TaskName == FirstTask.TaskName);

        var result = await registration.Invoker!.InvokeAsync(
            new FirstTask(),
            new OrderInput(),
            context: null!,
            CancellationToken.None
        );

        Assert.Null(result);
    }

    [Fact]
    public async Task TheGeneratedInvoker_InvokesATaskWithoutInput()
    {
        var registration = Register().Single(task => task.TaskName == NoInputTask.TaskName);

        var result = await registration.Invoker!.InvokeAsync(
            new NoInputTask(),
            input: null,
            context: null!,
            CancellationToken.None
        );

        Assert.Equal(42, Assert.IsType<OrderInput>(result).Value);
    }

    private static IReadOnlyList<TaskTypeRegistration> Register()
    {
        var builder = new DotCeleryBuilder(new ServiceCollection());
        builder.RegisterTasks();
        return builder.TaskRegistrations;
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
