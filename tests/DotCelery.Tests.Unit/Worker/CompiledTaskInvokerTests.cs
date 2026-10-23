using DotCelery.Core.Abstractions;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Registry;
using NSubstitute;

namespace DotCelery.Tests.Unit.Worker;

public sealed class CompiledTaskInvokerTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RepeatedTaskTypeRegistrations_InvokeTheirOwnInputSignature(bool precompile)
    {
        var invoker = new CompiledTaskInvoker();
        var numeric = new TaskRegistration(
            "number",
            typeof(OverloadedTask),
            typeof(NumberInput),
            typeof(string)
        );
        var textual = new TaskRegistration(
            "text",
            typeof(OverloadedTask),
            typeof(string),
            typeof(string)
        );
        if (precompile)
        {
            invoker.PreCompile(numeric);
            invoker.PreCompile(textual);
        }
        var context = Substitute.For<ITaskContext>();
        var task = new OverloadedTask();
        Assert.Equal(
            "number:42",
            await invoker.InvokeAsync(
                task,
                new NumberInput(42),
                context,
                numeric,
                CancellationToken.None
            )
        );
        Assert.Equal(
            "text:hello",
            await invoker.InvokeAsync(task, "hello", context, textual, CancellationToken.None)
        );
    }

    public sealed record NumberInput(int Value);

    public sealed class OverloadedTask : ITask<NumberInput, string>, ITask<string, string>
    {
        public static string TaskName => "overloaded";

        public Task<string> ExecuteAsync(
            NumberInput input,
            ITaskContext context,
            CancellationToken cancellationToken
        ) => Task.FromResult($"number:{input.Value}");

        public Task<string> ExecuteAsync(
            string input,
            ITaskContext context,
            CancellationToken cancellationToken
        ) => Task.FromResult($"text:{input}");
    }
}
