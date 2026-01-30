using DotCelery.Analyzers.Tests.Verifiers;

namespace DotCelery.Analyzers.Tests;

/// <summary>
/// Unit tests for BlockingCallAnalyzer (DCEL100).
/// </summary>
public class BlockingCallAnalyzerTests
{
    [Fact]
    public async Task TaskWait_ReportsDiagnostic()
    {
        const string test = """
            using System.Threading;
            using System.Threading.Tasks;
            using DotCelery.Core.Abstractions;

            public sealed class TestTask : ITask<TestInput, TestOutput>
            {
                public static string TaskName => "test.task";

                public async Task<TestOutput> ExecuteAsync(TestInput input, ITaskContext context, CancellationToken cancellationToken = default)
                {
                    var task = Task.Delay(100);
                    {|#0:task.Wait()|};
                    return new TestOutput();
                }
            }

            public class TestInput { }
            public class TestOutput { }
            """;

        await CSharpAnalyzerVerifier<BlockingCallAnalyzer>.VerifyAnalyzerAsync(
            test,
            DiagnosticDescriptors.AvoidBlockingCallsInTasks);
    }

    [Fact]
    public async Task TaskResult_ReportsDiagnostic()
    {
        const string test = """
            using System.Threading;
            using System.Threading.Tasks;
            using DotCelery.Core.Abstractions;

            public sealed class TestTask : ITask<TestInput, TestOutput>
            {
                public static string TaskName => "test.task";

                public async Task<TestOutput> ExecuteAsync(TestInput input, ITaskContext context, CancellationToken cancellationToken = default)
                {
                    var task = Task.FromResult(42);
                    var result = {|#0:task.Result|};
                    return new TestOutput();
                }
            }

            public class TestInput { }
            public class TestOutput { }
            """;

        await CSharpAnalyzerVerifier<BlockingCallAnalyzer>.VerifyAnalyzerAsync(
            test,
            DiagnosticDescriptors.AvoidBlockingCallsInTasks);
    }

    [Fact]
    public async Task GetAwaiterGetResult_ReportsDiagnostic()
    {
        const string test = """
            using System.Threading;
            using System.Threading.Tasks;
            using DotCelery.Core.Abstractions;

            public sealed class TestTask : ITask<TestInput, TestOutput>
            {
                public static string TaskName => "test.task";

                public async Task<TestOutput> ExecuteAsync(TestInput input, ITaskContext context, CancellationToken cancellationToken = default)
                {
                    var task = Task.FromResult(42);
                    var result = {|#0:task.GetAwaiter().GetResult()|};
                    return new TestOutput();
                }
            }

            public class TestInput { }
            public class TestOutput { }
            """;

        await CSharpAnalyzerVerifier<BlockingCallAnalyzer>.VerifyAnalyzerAsync(
            test,
            DiagnosticDescriptors.AvoidBlockingCallsInTasks);
    }

    [Fact]
    public async Task AwaitedTask_NoDiagnostic()
    {
        const string test = """
            using System.Threading;
            using System.Threading.Tasks;
            using DotCelery.Core.Abstractions;

            public sealed class TestTask : ITask<TestInput, TestOutput>
            {
                public static string TaskName => "test.task";

                public async Task<TestOutput> ExecuteAsync(TestInput input, ITaskContext context, CancellationToken cancellationToken = default)
                {
                    await Task.Delay(100);
                    var result = await Task.FromResult(42);
                    return new TestOutput();
                }
            }

            public class TestInput { }
            public class TestOutput { }
            """;

        await CSharpAnalyzerVerifier<BlockingCallAnalyzer>.VerifyAnalyzerAsync(test);
    }

    [Fact]
    public async Task BlockingCallOutsideTask_NoDiagnostic()
    {
        const string test = """
            using System.Threading.Tasks;

            public class NotATask
            {
                public void DoWork()
                {
                    var task = Task.Delay(100);
                    task.Wait(); // Should not report - not in ITask.ExecuteAsync
                }
            }
            """;

        await CSharpAnalyzerVerifier<BlockingCallAnalyzer>.VerifyAnalyzerAsync(test);
    }
}
