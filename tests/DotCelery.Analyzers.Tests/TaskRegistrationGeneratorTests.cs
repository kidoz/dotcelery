using DotCelery.Analyzers.Tests.Verifiers;

namespace DotCelery.Analyzers.Tests;

/// <summary>
/// Tests for the task registration generator, which registers the tasks defined in an assembly
/// by typed accesses to their names, without reflection.
/// </summary>
public class TaskRegistrationGeneratorTests
{
    [Fact]
    public void Generator_RegistersEachTaskByItsStaticName()
    {
        const string source = """
            using System.Threading;
            using System.Threading.Tasks;
            using DotCelery.Core.Abstractions;

            namespace Sample;

            public sealed class AlphaTask : ITask<AlphaInput>
            {
                public static string TaskName => "sample.alpha";

                public Task ExecuteAsync(AlphaInput input, ITaskContext context, CancellationToken cancellationToken = default) => Task.CompletedTask;
            }

            internal sealed class BetaTask : ITask<BetaInput>
            {
                public static string TaskName => "sample.beta";

                public Task ExecuteAsync(BetaInput input, ITaskContext context, CancellationToken cancellationToken = default) => Task.CompletedTask;
            }

            public sealed class AlphaInput { }

            public sealed class BetaInput { }
            """;

        var run = CSharpGeneratorVerifier<TaskRegistrationGenerator>.Run(source);

        Assert.Empty(run.Errors);
        var generated = Assert.Single(run.Sources);
        Assert.Contains(
            "builder.AddTask<global::Sample.AlphaTask>(global::Sample.AlphaTask.TaskName);",
            generated,
            StringComparison.Ordinal
        );
        Assert.Contains(
            "builder.AddTask<global::Sample.BetaTask>(global::Sample.BetaTask.TaskName);",
            generated,
            StringComparison.Ordinal
        );
        Assert.Contains("namespace DotCelery.Generated;", generated, StringComparison.Ordinal);
        Assert.Contains("RegisterTasks(", generated, StringComparison.Ordinal);
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
        Assert.Contains(
            "builder.AddTask<global::Sample.Host.NestedTask>(global::Sample.Host.NestedTask.TaskName);",
            Assert.Single(run.Sources),
            StringComparison.Ordinal
        );
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
