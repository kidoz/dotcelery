using DotCelery.Core.Canvas;

namespace DotCelery.Tests.Unit.Canvas;

public class ChordTests
{
    [Fact]
    public void Chord_ValidParams_CreatesChord()
    {
        var group = new Group([
            new Signature { TaskName = "task1" },
            new Signature { TaskName = "task2" },
        ]);
        var callback = new Signature { TaskName = "callback" };

        var chord = new Chord(group, callback);

        Assert.Same(group, chord.Header);
        Assert.Same(callback, chord.Callback);
        Assert.Equal(2, chord.Count);
    }

    [Fact]
    public void Chord_NullHeader_ThrowsArgumentNullException()
    {
        var callback = new Signature { TaskName = "callback" };

        Assert.Throws<ArgumentNullException>(() => new Chord(null!, callback));
    }

    [Fact]
    public void Chord_NullCallback_ThrowsArgumentNullException()
    {
        var group = new Group([new Signature { TaskName = "task1" }]);

        Assert.Throws<ArgumentNullException>(() => new Chord(group, null!));
    }

    [Fact]
    public void Chord_Type_ReturnsChord()
    {
        var group = new Group([new Signature { TaskName = "task1" }]);
        var callback = new Signature { TaskName = "callback" };
        var chord = new Chord(group, callback);

        Assert.Equal(CanvasType.Chord, chord.Type);
    }

    [Fact]
    public void Chord_GetSignatures_ReturnsHeaderAndCallback()
    {
        var group = new Group([
            new Signature { TaskName = "task1" },
            new Signature { TaskName = "task2" },
        ]);
        var callback = new Signature { TaskName = "callback" };
        var chord = new Chord(group, callback);

        var signatures = chord.GetSignatures().ToList();

        Assert.Equal(3, signatures.Count);
        Assert.Contains(signatures, s => s.TaskName == "task1");
        Assert.Contains(signatures, s => s.TaskName == "task2");
        Assert.Contains(signatures, s => s.TaskName == "callback");
    }

    [Fact]
    public void Chord_Count_ReturnsHeaderCount()
    {
        var group = new Group([
            new Signature { TaskName = "task1" },
            new Signature { TaskName = "task2" },
            new Signature { TaskName = "task3" },
        ]);
        var callback = new Signature { TaskName = "callback" };
        var chord = new Chord(group, callback);

        Assert.Equal(3, chord.Count);
    }

    [Fact]
    public void Chord_CreatedFromGroup_Works()
    {
        var chord = new Group([
            new Signature { TaskName = "task1" },
            new Signature { TaskName = "task2" },
        ]).WithCallback(new Signature { TaskName = "callback" });

        Assert.Equal(2, chord.Count);
        Assert.Equal("callback", chord.Callback.TaskName);
    }

    [Fact]
    public void Chord_GetSignatures_CallbackIsLast()
    {
        var group = new Group([new Signature { TaskName = "task1" }]);
        var callback = new Signature { TaskName = "callback" };
        var chord = new Chord(group, callback);

        var signatures = chord.GetSignatures().ToList();

        Assert.Equal("callback", signatures.Last().TaskName);
    }
}
