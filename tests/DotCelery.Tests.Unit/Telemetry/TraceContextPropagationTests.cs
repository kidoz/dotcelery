using System.Diagnostics;
using DotCelery.Core.Instrumentation;

namespace DotCelery.Tests.Unit.Telemetry;

public sealed class TraceContextPropagationTests
{
    [Fact]
    public void InjectCurrent_NoActivity_ReturnsSourceUnchanged()
    {
        Activity.Current = null;
        var existing = new Dictionary<string, string> { ["x"] = "y" };

        var result = TraceContextPropagation.InjectCurrent(existing);

        Assert.Same(existing, result);
    }

    [Fact]
    public void InjectCurrent_ActiveActivity_AddsTraceparentToCopy()
    {
        using var source = new ActivitySource("dotcelery.tests");
        using var listener = new ActivityListener
        {
            ShouldListenTo = _ => true,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(listener);

        using var activity = source.StartActivity("test");
        Assert.NotNull(activity);
        Assert.Equal(ActivityIdFormat.W3C, activity!.IdFormat);

        var headers = TraceContextPropagation.InjectCurrent(null);

        Assert.NotNull(headers);
        Assert.True(headers!.ContainsKey(TraceContextPropagation.TraceParentHeader));
        Assert.Equal(activity.Id, headers[TraceContextPropagation.TraceParentHeader]);
    }

    [Fact]
    public void TryExtract_ValidHeaders_ProducesRemoteContext()
    {
        const string traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        var headers = new Dictionary<string, string>
        {
            [TraceContextPropagation.TraceParentHeader] = traceparent,
        };

        Assert.True(TraceContextPropagation.TryExtract(headers, out var context));
        Assert.Equal("0af7651916cd43dd8448eb211c80319c", context.TraceId.ToHexString());
        Assert.Equal("b7ad6b7169203331", context.SpanId.ToHexString());
        Assert.True(context.IsRemote);
    }

    [Fact]
    public void TryExtract_MissingHeaders_ReturnsFalse()
    {
        Assert.False(TraceContextPropagation.TryExtract(null, out _));
        Assert.False(TraceContextPropagation.TryExtract(new Dictionary<string, string>(), out _));
    }
}
