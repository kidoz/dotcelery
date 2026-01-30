using DotCelery.Analyzers.Tests.Verifiers;

namespace DotCelery.Analyzers.Tests;

/// <summary>
/// Unit tests for TaskNameAnalyzer (DCEL001, DCEL002, DCEL003).
/// </summary>
public class TaskNameAnalyzerTests
{
    [Fact]
    public async Task EmptyTaskName_ReportsDiagnostic()
    {
        const string test = """
            using System.Threading;
            using System.Threading.Tasks;
            using DotCelery.Core.Abstractions;

            public sealed class TestTask : ITask<TestInput, TestOutput>
            {
                public static string {|#0:TaskName|} => "";

                public Task<TestOutput> ExecuteAsync(TestInput input, ITaskContext context, CancellationToken cancellationToken = default)
                {
                    return Task.FromResult(new TestOutput());
                }
            }

            public class TestInput { }
            public class TestOutput { }
            """;

        await CSharpAnalyzerVerifier<TaskNameAnalyzer>.VerifyAnalyzerAsync(
            test,
            DiagnosticDescriptors.TaskNameCannotBeEmpty);
    }

    [Fact]
    public async Task WhitespaceTaskName_ReportsDiagnostic()
    {
        const string test = """
            using System.Threading;
            using System.Threading.Tasks;
            using DotCelery.Core.Abstractions;

            public sealed class TestTask : ITask<TestInput, TestOutput>
            {
                public static string {|#0:TaskName|} => "   ";

                public Task<TestOutput> ExecuteAsync(TestInput input, ITaskContext context, CancellationToken cancellationToken = default)
                {
                    return Task.FromResult(new TestOutput());
                }
            }

            public class TestInput { }
            public class TestOutput { }
            """;

        await CSharpAnalyzerVerifier<TaskNameAnalyzer>.VerifyAnalyzerAsync(
            test,
            DiagnosticDescriptors.TaskNameCannotBeEmpty);
    }

    [Fact]
    public async Task DuplicateTaskName_ReportsDiagnostic()
    {
        const string test = """
            using System.Threading;
            using System.Threading.Tasks;
            using DotCelery.Core.Abstractions;

            public sealed class TestTask1 : ITask<TestInput, TestOutput>
            {
                public static string TaskName => "duplicate.task";

                public Task<TestOutput> ExecuteAsync(TestInput input, ITaskContext context, CancellationToken cancellationToken = default)
                {
                    return Task.FromResult(new TestOutput());
                }
            }

            public sealed class TestTask2 : ITask<TestInput, TestOutput>
            {
                public static string {|#0:TaskName|} => "duplicate.task";

                public Task<TestOutput> ExecuteAsync(TestInput input, ITaskContext context, CancellationToken cancellationToken = default)
                {
                    return Task.FromResult(new TestOutput());
                }
            }

            public class TestInput { }
            public class TestOutput { }
            """;

        await CSharpAnalyzerVerifier<TaskNameAnalyzer>.VerifyAnalyzerAsync(
            test,
            DiagnosticDescriptors.DuplicateTaskName);
    }

    [Fact]
    public async Task UnsealedTask_ReportsDiagnostic()
    {
        const string test = """
            using System.Threading;
            using System.Threading.Tasks;
            using DotCelery.Core.Abstractions;

            public class {|#0:TestTask|} : ITask<TestInput, TestOutput>
            {
                public static string TaskName => "test.task";

                public Task<TestOutput> ExecuteAsync(TestInput input, ITaskContext context, CancellationToken cancellationToken = default)
                {
                    return Task.FromResult(new TestOutput());
                }
            }

            public class TestInput { }
            public class TestOutput { }
            """;

        await CSharpAnalyzerVerifier<TaskNameAnalyzer>.VerifyAnalyzerAsync(
            test,
            DiagnosticDescriptors.TaskMustBeSealed);
    }

    [Fact]
    public async Task ValidSealedTask_NoDiagnostic()
    {
        const string test = """
            using System.Threading;
            using System.Threading.Tasks;
            using DotCelery.Core.Abstractions;

            public sealed class TestTask : ITask<TestInput, TestOutput>
            {
                public static string TaskName => "test.task";

                public Task<TestOutput> ExecuteAsync(TestInput input, ITaskContext context, CancellationToken cancellationToken = default)
                {
                    return Task.FromResult(new TestOutput());
                }
            }

            public class TestInput { }
            public class TestOutput { }
            """;

        await CSharpAnalyzerVerifier<TaskNameAnalyzer>.VerifyAnalyzerAsync(test);
    }

    [Fact]
    public async Task MultipleValidTasks_NoDiagnostic()
    {
        const string test = """
            using System.Threading;
            using System.Threading.Tasks;
            using DotCelery.Core.Abstractions;

            public sealed class Task1 : ITask<TestInput, TestOutput>
            {
                public static string TaskName => "task.one";

                public Task<TestOutput> ExecuteAsync(TestInput input, ITaskContext context, CancellationToken cancellationToken = default)
                {
                    return Task.FromResult(new TestOutput());
                }
            }

            public sealed class Task2 : ITask<TestInput, TestOutput>
            {
                public static string TaskName => "task.two";

                public Task<TestOutput> ExecuteAsync(TestInput input, ITaskContext context, CancellationToken cancellationToken = default)
                {
                    return Task.FromResult(new TestOutput());
                }
            }

            public class TestInput { }
            public class TestOutput { }
            """;

        await CSharpAnalyzerVerifier<TaskNameAnalyzer>.VerifyAnalyzerAsync(test);
    }
}
