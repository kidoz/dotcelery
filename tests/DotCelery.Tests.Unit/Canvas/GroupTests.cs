using DotCelery.Core.Canvas;

namespace DotCelery.Tests.Unit.Canvas;

public class GroupTests
{
    [Fact]
    public void Group_SingleSignature_CreatesGroup()
    {
        var sig = new Signature { TaskName = "task1" };

        var group = new Group([sig]);

        Assert.Equal(1, group.Count);
        Assert.Single(group.Members);
    }

    [Fact]
    public void Group_MultipleSignatures_PreservesAll()
    {
        var sig1 = new Signature { TaskName = "task1" };
        var sig2 = new Signature { TaskName = "task2" };
        var sig3 = new Signature { TaskName = "task3" };

        var group = new Group([sig1, sig2, sig3]);

        Assert.Equal(3, group.Count);
        Assert.Equal(3, group.Members.Count);
    }

    [Fact]
    public void Group_EmptySignatures_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => new Group(Array.Empty<Signature>()));
    }

    [Fact]
    public void Group_NullSignatures_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new Group((IEnumerable<Signature>)null!));
    }

    [Fact]
    public void Group_FromPrimitives_EmptyPrimitives_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => new Group(Array.Empty<CanvasPrimitive>()));
    }

    [Fact]
    public void Group_FromPrimitives_NullPrimitives_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new Group((IEnumerable<CanvasPrimitive>)null!));
    }

    [Fact]
    public void Group_And_Signature_AddsToGroup()
    {
        var sig1 = new Signature { TaskName = "task1" };
        var sig2 = new Signature { TaskName = "task2" };

        var group = new Group([sig1]).And(sig2);

        Assert.Equal(2, group.Count);
    }

    [Fact]
    public void Group_And_ReturnsSameInstance()
    {
        var group = new Group([new Signature { TaskName = "task1" }]);
        var sig = new Signature { TaskName = "task2" };

        var result = group.And(sig);

        Assert.Same(group, result);
    }

    [Fact]
    public void Group_And_NullSignature_ThrowsArgumentNullException()
    {
        var group = new Group([new Signature { TaskName = "task1" }]);

        Assert.Throws<ArgumentNullException>(() => group.And((Signature)null!));
    }

    [Fact]
    public void Group_And_Primitive_AddsPrimitive()
    {
        var group1 = new Group([new Signature { TaskName = "task1" }]);
        var chain = new Chain([new Signature { TaskName = "task2" }]);

        var result = group1.And(chain);

        Assert.Equal(2, result.Count);
    }

    [Fact]
    public void Group_And_NullPrimitive_ThrowsArgumentNullException()
    {
        var group = new Group([new Signature { TaskName = "task1" }]);

        Assert.Throws<ArgumentNullException>(() => group.And((CanvasPrimitive)null!));
    }

    [Fact]
    public void Group_WithCallback_CreatesChord()
    {
        var group = new Group([
            new Signature { TaskName = "task1" },
            new Signature { TaskName = "task2" },
        ]);
        var callback = new Signature { TaskName = "callback" };

        var chord = group.WithCallback(callback);

        Assert.Equal(CanvasType.Chord, chord.Type);
        Assert.Same(group, chord.Header);
        Assert.Equal("callback", chord.Callback.TaskName);
    }

    [Fact]
    public void Group_Then_Signature_CreatesChain()
    {
        var group = new Group([new Signature { TaskName = "task1" }]);
        var sig = new Signature { TaskName = "task2" };

        var chain = group.Then(sig);

        Assert.Equal(CanvasType.Chain, chain.Type);
        Assert.Equal(2, chain.Tasks.Count);
    }

    [Fact]
    public void Group_Type_ReturnsGroup()
    {
        var group = new Group([new Signature { TaskName = "task1" }]);

        Assert.Equal(CanvasType.Group, group.Type);
    }

    [Fact]
    public void Group_GetSignatures_ReturnsAllTasks()
    {
        var sig1 = new Signature { TaskName = "task1" };
        var sig2 = new Signature { TaskName = "task2" };
        var group = new Group([sig1, sig2]);

        var signatures = group.GetSignatures().ToList();

        Assert.Equal(2, signatures.Count);
        Assert.Contains(signatures, s => s.TaskName == "task1");
        Assert.Contains(signatures, s => s.TaskName == "task2");
    }

    [Fact]
    public void Group_FluentChaining_Works()
    {
        var group = new Group([new Signature { TaskName = "task1" }])
            .And(new Signature { TaskName = "task2" })
            .And(new Signature { TaskName = "task3" });

        Assert.Equal(3, group.Count);
    }

    [Fact]
    public void Group_WithNestedChain_GetSignatures_ReturnsAll()
    {
        var chain = new Chain([
            new Signature { TaskName = "chain.task1" },
            new Signature { TaskName = "chain.task2" },
        ]);
        var group = new Group([new Signature { TaskName = "group.task1" }]).And(chain);

        var signatures = group.GetSignatures().ToList();

        Assert.Equal(3, signatures.Count);
    }

    // C# 14 Features Tests

    [Fact]
    public void Group_IsEmpty_ReturnsFalse_WhenHasMembers()
    {
        var group = new Group([new Signature { TaskName = "task1" }]);

        Assert.False(group.IsEmpty);
    }

    [Fact]
    public void Group_IsSingle_ReturnsTrue_WhenOneMember()
    {
        var group = new Group([new Signature { TaskName = "task1" }]);

        Assert.True(group.IsSingle);
    }

    [Fact]
    public void Group_IsSingle_ReturnsFalse_WhenMultipleMembers()
    {
        var group = new Group([
            new Signature { TaskName = "task1" },
            new Signature { TaskName = "task2" },
        ]);

        Assert.False(group.IsSingle);
    }

    [Fact]
    public void Group_TotalSignatureCount_ReturnsCorrectCount()
    {
        var group = new Group([
            new Signature { TaskName = "task1" },
            new Signature { TaskName = "task2" },
            new Signature { TaskName = "task3" },
        ]);

        Assert.Equal(3, group.TotalSignatureCount);
    }

    [Fact]
    public void Group_TotalSignatureCount_IncludesNestedPrimitives()
    {
        var chain = new Chain([
            new Signature { TaskName = "chain.task1" },
            new Signature { TaskName = "chain.task2" },
        ]);
        var group = new Group([new Signature { TaskName = "group.task1" }]).And(chain);

        Assert.Equal(3, group.TotalSignatureCount);
    }

    [Fact]
    public void Group_PipeOperator_GroupPipeSignature_AddsToGroup()
    {
        var group = new Group([new Signature { TaskName = "task1" }]);
        var sig = new Signature { TaskName = "task2" };

        var result = group | sig;

        Assert.Equal(2, result.Count);
        Assert.Same(group, result);
    }

    [Fact]
    public void Group_PipeOperator_SignaturePipeGroup_AddsToGroup()
    {
        var sig = new Signature { TaskName = "task1" };
        var group = new Group([new Signature { TaskName = "task2" }]);

        var result = sig | group;

        Assert.Equal(2, result.Count);
        Assert.Same(group, result);
    }

    [Fact]
    public void Group_ParamsSpanConstructor_CreatesSingleMemberGroup()
    {
        var sig = new Signature { TaskName = "task1" };

        var group = new Group(sig);

        Assert.Equal(1, group.Count);
    }

    [Fact]
    public void Group_ParamsSpanConstructor_CreatesMultiMemberGroup()
    {
        var sig1 = new Signature { TaskName = "task1" };
        var sig2 = new Signature { TaskName = "task2" };
        var sig3 = new Signature { TaskName = "task3" };

        var group = new Group(sig1, sig2, sig3);

        Assert.Equal(3, group.Count);
        Assert.Equal(3, group.TotalSignatureCount);
    }

    [Fact]
    public void Group_ParamsSpanPrimitivesConstructor_CreatesGroup()
    {
        var chain1 = new Chain([new Signature { TaskName = "chain1.task1" }]);
        var chain2 = new Chain([new Signature { TaskName = "chain2.task1" }]);

        var group = new Group(chain1, chain2);

        Assert.Equal(2, group.Count);
        Assert.Equal(2, group.TotalSignatureCount);
    }
}
