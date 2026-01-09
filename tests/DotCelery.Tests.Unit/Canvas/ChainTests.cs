using DotCelery.Core.Canvas;

namespace DotCelery.Tests.Unit.Canvas;

public class ChainTests
{
    [Fact]
    public void Chain_SingleTask_CreatesChain()
    {
        var sig = new Signature { TaskName = "task1" };

        var chain = new Chain([sig]);

        Assert.Single(chain.Tasks);
        Assert.Equal("task1", chain.First.TaskName);
        Assert.Equal("task1", chain.Last.TaskName);
    }

    [Fact]
    public void Chain_MultipleTasks_PreservesOrder()
    {
        var sig1 = new Signature { TaskName = "task1" };
        var sig2 = new Signature { TaskName = "task2" };
        var sig3 = new Signature { TaskName = "task3" };

        var chain = new Chain([sig1, sig2, sig3]);

        Assert.Equal(3, chain.Tasks.Count);
        Assert.Equal("task1", chain.Tasks[0].TaskName);
        Assert.Equal("task2", chain.Tasks[1].TaskName);
        Assert.Equal("task3", chain.Tasks[2].TaskName);
        Assert.Equal("task1", chain.First.TaskName);
        Assert.Equal("task3", chain.Last.TaskName);
    }

    [Fact]
    public void Chain_EmptyTasks_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => new Chain([]));
    }

    [Fact]
    public void Chain_NullTasks_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new Chain((IEnumerable<Signature>)null!));
    }

    [Fact]
    public void Chain_Then_Signature_AppendsTask()
    {
        var sig1 = new Signature { TaskName = "task1" };
        var sig2 = new Signature { TaskName = "task2" };

        var chain = new Chain([sig1]).Then(sig2);

        Assert.Equal(2, chain.Tasks.Count);
        Assert.Equal("task2", chain.Last.TaskName);
    }

    [Fact]
    public void Chain_Then_Chain_AppendsAllTasks()
    {
        var chain1 = new Chain([new Signature { TaskName = "task1" }]);
        var chain2 = new Chain([
            new Signature { TaskName = "task2" },
            new Signature { TaskName = "task3" },
        ]);

        var result = chain1.Then(chain2);

        Assert.Equal(3, result.Tasks.Count);
        Assert.Equal("task1", result.Tasks[0].TaskName);
        Assert.Equal("task2", result.Tasks[1].TaskName);
        Assert.Equal("task3", result.Tasks[2].TaskName);
    }

    [Fact]
    public void Chain_Then_ReturnsSameInstance()
    {
        var chain = new Chain([new Signature { TaskName = "task1" }]);
        var sig = new Signature { TaskName = "task2" };

        var result = chain.Then(sig);

        Assert.Same(chain, result);
    }

    [Fact]
    public void Chain_Then_NullSignature_ThrowsArgumentNullException()
    {
        var chain = new Chain([new Signature { TaskName = "task1" }]);

        Assert.Throws<ArgumentNullException>(() => chain.Then((Signature)null!));
    }

    [Fact]
    public void Chain_Then_NullChain_ThrowsArgumentNullException()
    {
        var chain = new Chain([new Signature { TaskName = "task1" }]);

        Assert.Throws<ArgumentNullException>(() => chain.Then((Chain)null!));
    }

    [Fact]
    public void Chain_WithCallback_CreatesChord()
    {
        var chain = new Chain([new Signature { TaskName = "task1" }]);
        var callback = new Signature { TaskName = "callback" };

        var chord = chain.WithCallback(callback);

        Assert.Equal(CanvasType.Chord, chord.Type);
        Assert.Equal("callback", chord.Callback.TaskName);
    }

    [Fact]
    public void Chain_Type_ReturnsChain()
    {
        var chain = new Chain([new Signature { TaskName = "task1" }]);

        Assert.Equal(CanvasType.Chain, chain.Type);
    }

    [Fact]
    public void Chain_GetSignatures_ReturnsAllTasks()
    {
        var sig1 = new Signature { TaskName = "task1" };
        var sig2 = new Signature { TaskName = "task2" };
        var chain = new Chain([sig1, sig2]);

        var signatures = chain.GetSignatures().ToList();

        Assert.Equal(2, signatures.Count);
        Assert.Contains(signatures, s => s.TaskName == "task1");
        Assert.Contains(signatures, s => s.TaskName == "task2");
    }

    [Fact]
    public void Chain_FluentChaining_Works()
    {
        var chain = new Chain([new Signature { TaskName = "task1" }])
            .Then(new Signature { TaskName = "task2" })
            .Then(new Signature { TaskName = "task3" })
            .Then(new Signature { TaskName = "task4" });

        Assert.Equal(4, chain.Tasks.Count);
        Assert.Equal("task4", chain.Last.TaskName);
    }

    // C# 14 Features Tests

    [Fact]
    public void Chain_IsEmpty_ReturnsFalse_WhenHasTasks()
    {
        var chain = new Chain([new Signature { TaskName = "task1" }]);

        Assert.False(chain.IsEmpty);
    }

    [Fact]
    public void Chain_IsSingle_ReturnsTrue_WhenOneTask()
    {
        var chain = new Chain([new Signature { TaskName = "task1" }]);

        Assert.True(chain.IsSingle);
    }

    [Fact]
    public void Chain_IsSingle_ReturnsFalse_WhenMultipleTasks()
    {
        var chain = new Chain([
            new Signature { TaskName = "task1" },
            new Signature { TaskName = "task2" },
        ]);

        Assert.False(chain.IsSingle);
    }

    [Fact]
    public void Chain_TotalTaskCount_ReturnsCorrectCount()
    {
        var chain = new Chain([
            new Signature { TaskName = "task1" },
            new Signature { TaskName = "task2" },
            new Signature { TaskName = "task3" },
        ]);

        Assert.Equal(3, chain.TotalTaskCount);
    }

    [Fact]
    public void Chain_PlusOperator_Signature_AppendsTask()
    {
        var chain = new Chain([new Signature { TaskName = "task1" }]);
        var sig = new Signature { TaskName = "task2" };

        var result = chain + sig;

        Assert.Equal(2, result.Tasks.Count);
        Assert.Equal("task2", result.Last.TaskName);
        Assert.Same(chain, result);
    }

    [Fact]
    public void Chain_PlusOperator_Chain_ConcatenatesChains()
    {
        var chain1 = new Chain([new Signature { TaskName = "task1" }]);
        var chain2 = new Chain([
            new Signature { TaskName = "task2" },
            new Signature { TaskName = "task3" },
        ]);

        var result = chain1 + chain2;

        Assert.Equal(3, result.Tasks.Count);
        Assert.Equal("task1", result.Tasks[0].TaskName);
        Assert.Equal("task2", result.Tasks[1].TaskName);
        Assert.Equal("task3", result.Tasks[2].TaskName);
        Assert.Same(chain1, result);
    }

    [Fact]
    public void Chain_ParamsSpanConstructor_CreatesSingleTaskChain()
    {
        var sig = new Signature { TaskName = "task1" };

        var chain = new Chain(sig);

        Assert.Single(chain.Tasks);
        Assert.Equal("task1", chain.First.TaskName);
    }

    [Fact]
    public void Chain_ParamsSpanConstructor_CreatesMultiTaskChain()
    {
        var sig1 = new Signature { TaskName = "task1" };
        var sig2 = new Signature { TaskName = "task2" };
        var sig3 = new Signature { TaskName = "task3" };

        var chain = new Chain(sig1, sig2, sig3);

        Assert.Equal(3, chain.Tasks.Count);
        Assert.Equal("task1", chain.First.TaskName);
        Assert.Equal("task3", chain.Last.TaskName);
    }
}
