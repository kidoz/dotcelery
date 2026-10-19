using DotCelery.Analyzers.Tests.Verifiers;

namespace DotCelery.Analyzers.Tests;

/// <summary>
/// Tests for the task registration generator, which registers the tasks defined in an assembly
/// by typed accesses to their names and generates the typed adapters that invoke them, without
/// reflection or expression compilation.
/// </summary>
public class TaskRegistrationGeneratorTests
{
    [Fact]
    public void Generator_RegistersEachTaskAndItsInvoker()
    {
        const string source = """
            using System.Threading;
            using System.Threading.Tasks;
            using DotCelery.Core.Abstractions;

            namespace Sample;

            public sealed class AlphaTask : ITask<AlphaInput, AlphaOutput>
            {
                public static string TaskName => "sample.alpha";

                public Task<AlphaOutput> ExecuteAsync(AlphaInput input, ITaskContext context, CancellationToken cancellationToken = default) => Task.FromResult(new AlphaOutput());
            }

            internal sealed class BetaTask : ITask<BetaInput>
            {
                public static string TaskName => "sample.beta";

                public Task ExecuteAsync(BetaInput input, ITaskContext context, CancellationToken cancellationToken = default) => Task.CompletedTask;
            }

            public sealed class AlphaInput { }

            public sealed class AlphaOutput { }

            public sealed class BetaInput { }
            """;

        var run = CSharpGeneratorVerifier<TaskRegistrationGenerator>.Run(source);

        Assert.Empty(run.Errors);
        var generated = Assert.Single(run.Sources);
        Assert.Contains(
            "builder.AddTask<global::Sample.AlphaTask>(global::Sample.AlphaTask.TaskName, new Sample_AlphaTask_DotCeleryInvoker(), typeof(global::Sample.AlphaInput), typeof(global::Sample.AlphaOutput));",
            generated,
            StringComparison.Ordinal
        );
        Assert.Contains(
            "builder.AddTask<global::Sample.BetaTask>(global::Sample.BetaTask.TaskName, new Sample_BetaTask_DotCeleryInvoker(), typeof(global::Sample.BetaInput), null);",
            generated,
            StringComparison.Ordinal
        );

        // The adapter of a task with a result returns it; one without a result returns null
        Assert.Contains(
            "return await ((global::DotCelery.Core.Abstractions.ITask<global::Sample.AlphaInput, global::Sample.AlphaOutput>)task).ExecuteAsync((global::Sample.AlphaInput)input!, context, cancellationToken).ConfigureAwait(false);",
            generated,
            StringComparison.Ordinal
        );
        Assert.Contains(
            "await ((global::DotCelery.Core.Abstractions.ITask<global::Sample.BetaInput>)task).ExecuteAsync((global::Sample.BetaInput)input!, context, cancellationToken).ConfigureAwait(false);",
            generated,
            StringComparison.Ordinal
        );
        Assert.Contains("namespace DotCelery.Generated;", generated, StringComparison.Ordinal);
        Assert.Contains("RegisterTasks(", generated, StringComparison.Ordinal);

        // A typed helper creates the signature from the input alone
        Assert.Contains(
            "global::DotCelery.Core.Canvas.Signature<global::Sample.AlphaTask, global::Sample.AlphaInput, global::Sample.AlphaOutput> Alpha(global::Sample.AlphaInput input) =>",
            generated,
            StringComparison.Ordinal
        );
        Assert.Contains(
            "global::DotCelery.Core.Canvas.Signature<global::Sample.BetaTask, global::Sample.BetaInput> Beta(global::Sample.BetaInput input) =>",
            generated,
            StringComparison.Ordinal
        );
    }

    [Fact]
    public void Generator_RegistersNestedTasks()
    {
        const string source = """
            using System.Threading;
            using System.Threading.Tasks;
            using DotCelery.Core.Abstractions;

            namespace Sample;

            public static class Host
            {
                public sealed class NestedTask : ITask<Input>
                {
                    public static string TaskName => "sample.nested";

                    public Task ExecuteAsync(Input input, ITaskContext context, CancellationToken cancellationToken = default) => Task.CompletedTask;
                }
            }

            public sealed class Input { }
            """;

        var run = CSharpGeneratorVerifier<TaskRegistrationGenerator>.Run(source);

        Assert.Empty(run.Errors);
        var generated = Assert.Single(run.Sources);
        Assert.Contains(
            "builder.AddTask<global::Sample.Host.NestedTask>(global::Sample.Host.NestedTask.TaskName, new Sample_Host_NestedTask_DotCeleryInvoker(), typeof(global::Sample.Input), null);",
            generated,
            StringComparison.Ordinal
        );
        Assert.Contains(
            "global::DotCelery.Core.Canvas.Signature<global::Sample.Host.NestedTask, global::Sample.Input> Nested(global::Sample.Input input) =>",
            generated,
            StringComparison.Ordinal
        );
        Assert.Contains(
            "internal sealed class Sample_Host_NestedTask_DotCeleryInvoker",
            generated,
            StringComparison.Ordinal
        );
    }

    [Fact]
    public void Generator_GeneratesAnInvokerForATaskWithTheInputlessConvention()
    {
        const string source = """
            using System.Threading;
            using System.Threading.Tasks;
            using DotCelery.Core.Abstractions;

            namespace Sample;

            public sealed class StartupTask : ITask
            {
                public static string TaskName => "sample.startup";

                public Task<Input> ExecuteAsync(ITaskContext context, CancellationToken cancellationToken = default) => Task.FromResult(new Input());
            }

            public sealed class Input { }
            """;

        var run = CSharpGeneratorVerifier<TaskRegistrationGenerator>.Run(source);

        Assert.Empty(run.Errors);
        var generated = Assert.Single(run.Sources);
        Assert.Contains(
            "builder.AddTask<global::Sample.StartupTask>(global::Sample.StartupTask.TaskName, new Sample_StartupTask_DotCeleryInvoker(), null, typeof(global::Sample.Input));",
            generated,
            StringComparison.Ordinal
        );
        Assert.Contains(
            "public static global::DotCelery.Core.Canvas.Signature Startup() =>",
            generated,
            StringComparison.Ordinal
        );
        Assert.Contains(
            "return await ((global::Sample.StartupTask)task).ExecuteAsync(context, cancellationToken).ConfigureAwait(false);",
            generated,
            StringComparison.Ordinal
        );
    }

    [Fact]
    public void Generator_RegistersATaskWithAmbiguousInterfacesWithoutAnInvoker()
    {
        const string source = """
            using System.Threading;
            using System.Threading.Tasks;
            using DotCelery.Core.Abstractions;

            namespace Sample;

            public sealed class TwoShapesTask : ITask<Input>, ITask<Input, Input>
            {
                public static string TaskName => "sample.two";

                public Task ExecuteAsync(Input input, ITaskContext context, CancellationToken cancellationToken = default) => Task.CompletedTask;

                Task<Input> ITask<Input, Input>.ExecuteAsync(Input input, ITaskContext context, CancellationToken cancellationToken) => Task.FromResult(input);
            }

            public sealed class Input { }
            """;

        var run = CSharpGeneratorVerifier<TaskRegistrationGenerator>.Run(source);

        Assert.Empty(run.Errors);
        var generated = Assert.Single(run.Sources);
        Assert.Contains(
            "builder.AddTask<global::Sample.TwoShapesTask>(global::Sample.TwoShapesTask.TaskName);",
            generated,
            StringComparison.Ordinal
        );
        Assert.DoesNotContain(
            "TwoShapesTask_DotCeleryInvoker",
            generated,
            StringComparison.Ordinal
        );

        // Without an adapter the generator cannot know the task's input, so it has no helper either
        Assert.DoesNotContain(" TwoShapes(", generated, StringComparison.Ordinal);
    }

    [Fact]
    public void Generator_SkipsTypesThatCannotBeRegistered()
    {
        const string source = """
            using System.Threading;
            using System.Threading.Tasks;
            using DotCelery.Core.Abstractions;

            namespace Sample;

            public abstract class AbstractTask : ITask<Input>
            {
                public static string TaskName => "sample.abstract";

                public abstract Task ExecuteAsync(Input input, ITaskContext context, CancellationToken cancellationToken = default);
            }

            public sealed class GenericTask<TInput> : ITask<TInput>
                where TInput : class
            {
                public static string TaskName => "sample.generic";

                public Task ExecuteAsync(TInput input, ITaskContext context, CancellationToken cancellationToken = default) => Task.CompletedTask;
            }

            public static class Host
            {
                private sealed class HiddenTask : ITask<Input>
                {
                    public static string TaskName => "sample.hidden";

                    public Task ExecuteAsync(Input input, ITaskContext context, CancellationToken cancellationToken = default) => Task.CompletedTask;
                }
            }

            file sealed class FileTask : ITask<Input>
            {
                public static string TaskName => "sample.file";

                public Task ExecuteAsync(Input input, ITaskContext context, CancellationToken cancellationToken = default) => Task.CompletedTask;
            }

            public sealed class VisibleTask : ITask<Input>
            {
                public static string TaskName => "sample.visible";

                public Task ExecuteAsync(Input input, ITaskContext context, CancellationToken cancellationToken = default) => Task.CompletedTask;
            }

            public sealed class NotATask { }

            public sealed class Input { }
            """;

        var run = CSharpGeneratorVerifier<TaskRegistrationGenerator>.Run(source);

        Assert.Empty(run.Errors);
        var generated = Assert.Single(run.Sources);
        Assert.Contains("global::Sample.VisibleTask", generated, StringComparison.Ordinal);
        Assert.DoesNotContain("AbstractTask", generated, StringComparison.Ordinal);
        Assert.DoesNotContain("GenericTask", generated, StringComparison.Ordinal);
        Assert.DoesNotContain("HiddenTask", generated, StringComparison.Ordinal);
        Assert.DoesNotContain("FileTask", generated, StringComparison.Ordinal);
        Assert.DoesNotContain("NotATask", generated, StringComparison.Ordinal);
    }

    [Fact]
    public void Generator_GivesCollidingHelperNamesTheFlattenedTypeName()
    {
        const string source = """
            using System.Threading;
            using System.Threading.Tasks;
            using DotCelery.Core.Abstractions;

            namespace One
            {
                public sealed class SendTask : ITask<Shared.Input>
                {
                    public static string TaskName => "one.send";

                    public Task ExecuteAsync(Shared.Input input, ITaskContext context, CancellationToken cancellationToken = default) => Task.CompletedTask;
                }
            }

            namespace Two
            {
                public sealed class SendTask : ITask<Shared.Input>
                {
                    public static string TaskName => "two.send";

                    public Task ExecuteAsync(Shared.Input input, ITaskContext context, CancellationToken cancellationToken = default) => Task.CompletedTask;
                }
            }

            namespace Shared
            {
                public sealed class Input { }
            }
            """;

        var run = CSharpGeneratorVerifier<TaskRegistrationGenerator>.Run(source);

        Assert.Empty(run.Errors);
        var generated = Assert.Single(run.Sources);
        Assert.Contains(
            "One_SendTask(global::Shared.Input input) =>",
            generated,
            StringComparison.Ordinal
        );
        Assert.Contains(
            "Two_SendTask(global::Shared.Input input) =>",
            generated,
            StringComparison.Ordinal
        );
    }

    [Fact]
    public void Generator_DoesNotNameAHelperLikeTheRegistrationMethod()
    {
        const string source = """
            using System.Threading;
            using System.Threading.Tasks;
            using DotCelery.Core.Abstractions;

            namespace Sample;

            public sealed class RegisterTasks : ITask<Input>
            {
                public static string TaskName => "sample.reserved";

                public Task ExecuteAsync(Input input, ITaskContext context, CancellationToken cancellationToken = default) => Task.CompletedTask;
            }

            public sealed class Input { }
            """;

        var run = CSharpGeneratorVerifier<TaskRegistrationGenerator>.Run(source);

        Assert.Empty(run.Errors);
        var generated = Assert.Single(run.Sources);
        Assert.Contains(
            "Sample_RegisterTasks(global::Sample.Input input) =>",
            generated,
            StringComparison.Ordinal
        );
    }

    [Fact]
    public void Generator_EmitsNothingWhenTheAssemblyHasNoTasks()
    {
        const string source = """
            namespace Sample;

            public sealed class NotATask { }
            """;

        var run = CSharpGeneratorVerifier<TaskRegistrationGenerator>.Run(source);

        Assert.Empty(run.Errors);
        Assert.Empty(run.Sources);
    }
}
