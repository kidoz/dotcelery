using DotCelery.Core.Abstractions;
using DotCelery.Core.Canvas;

namespace DotCelery.Tests.Unit.Canvas;

public class SignatureTests
{
    [Fact]
    public void Signature_RequiredProperties_SetCorrectly()
    {
        var sig = new Signature { TaskName = "test.task" };

        Assert.Equal("test.task", sig.TaskName);
        Assert.Equal("celery", sig.Queue);
        Assert.Equal(0, sig.Priority);
        Assert.Equal(3, sig.MaxRetries);
        Assert.True(sig.StoreResult);
    }

    [Fact]
    public void Signature_OptionalProperties_SetCorrectly()
    {
        var eta = DateTimeOffset.UtcNow.AddMinutes(10);
        var expires = DateTimeOffset.UtcNow.AddHours(1);
        var headers = new Dictionary<string, string> { ["key"] = "value" };

        var sig = new Signature
        {
            TaskName = "test.task",
            Queue = "custom",
            Priority = 5,
            MaxRetries = 10,
            Args = [1, 2, 3],
            Countdown = TimeSpan.FromMinutes(5),
            Eta = eta,
            Expires = expires,
            Headers = headers,
            StoreResult = false,
        };

        Assert.Equal("custom", sig.Queue);
        Assert.Equal(5, sig.Priority);
        Assert.Equal(10, sig.MaxRetries);
        Assert.Equal([1, 2, 3], sig.Args);
        Assert.Equal(TimeSpan.FromMinutes(5), sig.Countdown);
        Assert.Equal(eta, sig.Eta);
        Assert.Equal(expires, sig.Expires);
        Assert.Equal(headers, sig.Headers);
        Assert.False(sig.StoreResult);
    }

    [Fact]
    public void Signature_Then_CreatesChain()
    {
        var sig1 = new Signature { TaskName = "task1" };
        var sig2 = new Signature { TaskName = "task2" };

        var chain = sig1.Then(sig2);

        Assert.Equal(2, chain.Tasks.Count);
        Assert.Equal("task1", chain.Tasks[0].TaskName);
        Assert.Equal("task2", chain.Tasks[1].TaskName);
    }

    [Fact]
    public void Signature_And_CreatesGroup()
    {
        var sig1 = new Signature { TaskName = "task1" };
        var sig2 = new Signature { TaskName = "task2" };

        var group = sig1.And(sig2);

        Assert.Equal(2, group.Count);
    }

    [Fact]
    public void Signature_Link_CanBeSet()
    {
        var sig2 = new Signature { TaskName = "task2" };
        var errorSig = new Signature { TaskName = "error.handler" };

        var sig1 = new Signature
        {
            TaskName = "task1",
            Link = sig2,
            LinkError = errorSig,
        };

        Assert.Equal("task2", sig1.Link?.TaskName);
        Assert.Equal("error.handler", sig1.LinkError?.TaskName);
    }

    [Fact]
    public void TypedSignature_WithInputOutput_SetsTaskName()
    {
        var sig = new Signature<TestTask, TestInput, TestOutput>();

        Assert.Equal("test.task", sig.TaskName);
    }

    [Fact]
    public void TypedSignature_WithInputOnly_SetsTaskName()
    {
        var sig = new Signature<TaskWithInputOnly, TestInput>();

        Assert.Equal("task.input.only", sig.TaskName);
    }

    [Fact]
    public void TypedSignature_Input_CanBeSet()
    {
        var input = new TestInput { Value = 42 };
        var sig = new Signature<TestTask, TestInput, TestOutput> { Input = input };

        Assert.NotNull(sig.Input);
        Assert.Equal(42, sig.Input.Value);
    }

    [Fact]
    public void Signature_HasLink_ReturnsTrueWhenLinkSet()
    {
        var sig = new Signature
        {
            TaskName = "task1",
            Link = new Signature { TaskName = "task2" },
        };

        Assert.True(sig.HasLink);
    }

    [Fact]
    public void Signature_HasLink_ReturnsFalseWhenNoLink()
    {
        var sig = new Signature { TaskName = "task1" };

        Assert.False(sig.HasLink);
    }

    [Fact]
    public void Signature_HasErrorLink_ReturnsTrueWhenLinkErrorSet()
    {
        var sig = new Signature
        {
            TaskName = "task1",
            LinkError = new Signature { TaskName = "error.handler" },
        };

        Assert.True(sig.HasErrorLink);
    }

    [Fact]
    public void Signature_HasErrorLink_ReturnsFalseWhenNoLinkError()
    {
        var sig = new Signature { TaskName = "task1" };

        Assert.False(sig.HasErrorLink);
    }

    [Fact]
    public void Signature_HasEta_ReturnsTrueWhenEtaSet()
    {
        var sig = new Signature { TaskName = "task1", Eta = DateTimeOffset.UtcNow.AddMinutes(10) };

        Assert.True(sig.HasEta);
    }

    [Fact]
    public void Signature_HasCountdown_ReturnsTrueWhenCountdownSet()
    {
        var sig = new Signature { TaskName = "task1", Countdown = TimeSpan.FromMinutes(5) };

        Assert.True(sig.HasCountdown);
    }

    [Fact]
    public void Signature_IsScheduled_ReturnsTrueWhenEtaOrCountdownSet()
    {
        var sigWithEta = new Signature
        {
            TaskName = "task1",
            Eta = DateTimeOffset.UtcNow.AddMinutes(10),
        };
        var sigWithCountdown = new Signature
        {
            TaskName = "task2",
            Countdown = TimeSpan.FromMinutes(5),
        };
        var sigNotScheduled = new Signature { TaskName = "task3" };

        Assert.True(sigWithEta.IsScheduled);
        Assert.True(sigWithCountdown.IsScheduled);
        Assert.False(sigNotScheduled.IsScheduled);
    }

    [Fact]
    public void Signature_IsExpired_ReturnsTrueWhenExpiresInPast()
    {
        var expiredSig = new Signature
        {
            TaskName = "task1",
            Expires = DateTimeOffset.UtcNow.AddMinutes(-10),
        };
        var validSig = new Signature
        {
            TaskName = "task2",
            Expires = DateTimeOffset.UtcNow.AddMinutes(10),
        };
        var noExpiresSig = new Signature { TaskName = "task3" };

        Assert.True(expiredSig.IsExpired);
        Assert.False(validSig.IsExpired);
        Assert.False(noExpiresSig.IsExpired);
    }

    [Fact]
    public void Signature_HasHeaders_ReturnsTrueWhenHeadersNotEmpty()
    {
        var sigWithHeaders = new Signature
        {
            TaskName = "task1",
            Headers = new Dictionary<string, string> { ["key"] = "value" },
        };
        var sigEmptyHeaders = new Signature
        {
            TaskName = "task2",
            Headers = new Dictionary<string, string>(),
        };
        var sigNoHeaders = new Signature { TaskName = "task3" };

        Assert.True(sigWithHeaders.HasHeaders);
        Assert.False(sigEmptyHeaders.HasHeaders);
        Assert.False(sigNoHeaders.HasHeaders);
    }

    [Fact]
    public void Signature_EffectiveEta_ReturnsEtaWhenSet()
    {
        var eta = DateTimeOffset.UtcNow.AddMinutes(10);
        var sig = new Signature { TaskName = "task1", Eta = eta };

        Assert.Equal(eta, sig.EffectiveEta);
    }

    [Fact]
    public void Signature_EffectiveEta_CalculatesFromCountdown()
    {
        var countdown = TimeSpan.FromMinutes(5);
        var sig = new Signature { TaskName = "task1", Countdown = countdown };

        var effectiveEta = sig.EffectiveEta;

        Assert.NotNull(effectiveEta);
        Assert.True(effectiveEta.Value > DateTimeOffset.UtcNow);
        Assert.True(effectiveEta.Value < DateTimeOffset.UtcNow.AddMinutes(6));
    }

    [Fact]
    public void Signature_EffectiveEta_ReturnsNullWhenNotScheduled()
    {
        var sig = new Signature { TaskName = "task1" };

        Assert.Null(sig.EffectiveEta);
    }

    [Fact]
    public void Signature_TimeUntilExecution_ReturnsRemainingTime()
    {
        var sig = new Signature { TaskName = "task1", Eta = DateTimeOffset.UtcNow.AddMinutes(10) };

        var remaining = sig.TimeUntilExecution;

        Assert.NotNull(remaining);
        Assert.True(remaining.Value > TimeSpan.FromMinutes(9));
        Assert.True(remaining.Value <= TimeSpan.FromMinutes(10));
    }

    [Fact]
    public void Signature_TimeUntilExecution_ReturnsZeroWhenPast()
    {
        var sig = new Signature { TaskName = "task1", Eta = DateTimeOffset.UtcNow.AddMinutes(-10) };

        Assert.Equal(TimeSpan.Zero, sig.TimeUntilExecution);
    }

    [Fact]
    public void Signature_TimeUntilExecution_ReturnsNullWhenNotScheduled()
    {
        var sig = new Signature { TaskName = "task1" };

        Assert.Null(sig.TimeUntilExecution);
    }

    [Fact]
    public void Signature_PlusOperator_CreatesChain()
    {
        var sig1 = new Signature { TaskName = "task1" };
        var sig2 = new Signature { TaskName = "task2" };

        var chain = sig1 + sig2;

        Assert.Equal(2, chain.Tasks.Count);
        Assert.Equal("task1", chain.Tasks[0].TaskName);
        Assert.Equal("task2", chain.Tasks[1].TaskName);
    }

    [Fact]
    public void Signature_PipeOperator_CreatesGroup()
    {
        var sig1 = new Signature { TaskName = "task1" };
        var sig2 = new Signature { TaskName = "task2" };

        var group = sig1 | sig2;

        Assert.Equal(2, group.Count);
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
