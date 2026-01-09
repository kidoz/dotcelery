using System.Diagnostics;
using DotCelery.Telemetry;

namespace DotCelery.Tests.Unit.Telemetry;

public class DotCeleryInstrumentationTests
{
    [Fact]
    public void InstrumentationName_ReturnsExpectedValue()
    {
        Assert.Equal("DotCelery", DotCeleryInstrumentation.InstrumentationName);
    }

    [Fact]
    public void InstrumentationVersion_ReturnsExpectedValue()
    {
        Assert.Equal("1.0.0", DotCeleryInstrumentation.InstrumentationVersion);
    }

    [Fact]
    public void ActivitySource_IsNotNull()
    {
        Assert.NotNull(DotCeleryInstrumentation.ActivitySource);
        Assert.Equal("DotCelery", DotCeleryInstrumentation.ActivitySource.Name);
        Assert.Equal("1.0.0", DotCeleryInstrumentation.ActivitySource.Version);
    }

    [Fact]
    public void Meter_IsNotNull()
    {
        Assert.NotNull(DotCeleryInstrumentation.Meter);
        Assert.Equal("DotCelery", DotCeleryInstrumentation.Meter.Name);
        Assert.Equal("1.0.0", DotCeleryInstrumentation.Meter.Version);
    }

    [Fact]
    public void RecordTaskSent_DoesNotThrow()
    {
        // Should not throw even without a listener
        DotCeleryInstrumentation.RecordTaskSent("test.task", "celery");
    }

    [Fact]
    public void RecordTaskReceived_DoesNotThrow()
    {
        DotCeleryInstrumentation.RecordTaskReceived("test.task", "celery");
    }

    [Fact]
    public void RecordTaskCompleted_Success_DoesNotThrow()
    {
        DotCeleryInstrumentation.RecordTaskCompleted(
            "test.task",
            true,
            TimeSpan.FromMilliseconds(100)
        );
    }

    [Fact]
    public void RecordTaskCompleted_Failure_DoesNotThrow()
    {
        DotCeleryInstrumentation.RecordTaskCompleted(
            "test.task",
            false,
            TimeSpan.FromMilliseconds(100)
        );
    }

    [Fact]
    public void RecordTaskRetry_DoesNotThrow()
    {
        DotCeleryInstrumentation.RecordTaskRetry("test.task", 1);
    }

    [Fact]
    public void RecordQueueTime_DoesNotThrow()
    {
        DotCeleryInstrumentation.RecordQueueTime("test.task", TimeSpan.FromMilliseconds(50));
    }

    [Fact]
    public void IncrementTasksInProgress_DoesNotThrow()
    {
        DotCeleryInstrumentation.IncrementTasksInProgress("test.task");
    }

    [Fact]
    public void DecrementTasksInProgress_DoesNotThrow()
    {
        DotCeleryInstrumentation.DecrementTasksInProgress("test.task");
    }

    [Fact]
    public void StartSendActivity_WithoutListener_ReturnsNull()
    {
        // Without an ActivityListener, the activity won't be sampled
        var activity = DotCeleryInstrumentation.StartSendActivity("test.task", "task-123");

        // May be null without a listener, but shouldn't throw
        activity?.Dispose();
    }

    [Fact]
    public void StartProcessActivity_WithoutListener_ReturnsNull()
    {
        var activity = DotCeleryInstrumentation.StartProcessActivity("test.task", "task-123");

        activity?.Dispose();
    }

    [Fact]
    public void StartProcessActivity_WithParentContext_DoesNotThrow()
    {
        var parentContext = new ActivityContext(
            ActivityTraceId.CreateRandom(),
            ActivitySpanId.CreateRandom(),
            ActivityTraceFlags.Recorded
        );

        var activity = DotCeleryInstrumentation.StartProcessActivity(
            "test.task",
            "task-123",
            parentContext
        );

        activity?.Dispose();
    }

    [Fact]
    public void RecordException_NullActivity_DoesNotThrow()
    {
        var exception = new InvalidOperationException("Test error");

        DotCeleryInstrumentation.RecordException(null, exception);
    }

    [Fact]
    public void StartSendActivity_WithListener_CreatesActivity()
    {
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "DotCelery",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllData,
        };
        ActivitySource.AddActivityListener(listener);

        var activity = DotCeleryInstrumentation.StartSendActivity("test.task", "task-123");

        Assert.NotNull(activity);
        Assert.Equal("send test.task", activity.DisplayName);
        Assert.Equal(ActivityKind.Producer, activity.Kind);
        Assert.Equal("celery", activity.GetTagItem("messaging.system"));
        Assert.Equal("send", activity.GetTagItem("messaging.operation"));
        Assert.Equal("test.task", activity.GetTagItem("celery.task.name"));
        Assert.Equal("task-123", activity.GetTagItem("celery.task.id"));

        activity.Dispose();
    }

    [Fact]
    public void StartProcessActivity_WithListener_CreatesActivity()
    {
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "DotCelery",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllData,
        };
        ActivitySource.AddActivityListener(listener);

        var activity = DotCeleryInstrumentation.StartProcessActivity("test.task", "task-456");

        Assert.NotNull(activity);
        Assert.Equal("process test.task", activity.DisplayName);
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
        Assert.Equal("process", activity.GetTagItem("messaging.operation"));

        activity.Dispose();
    }

    [Fact]
    public void RecordException_WithActivity_SetsStatusAndAddsException()
    {
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "DotCelery",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllData,
        };
        ActivitySource.AddActivityListener(listener);

        var activity = DotCeleryInstrumentation.StartProcessActivity("test.task", "task-789");
        Assert.NotNull(activity);

        var exception = new InvalidOperationException("Something went wrong");
        DotCeleryInstrumentation.RecordException(activity, exception);

        Assert.Equal(ActivityStatusCode.Error, activity.Status);
        Assert.Equal("Something went wrong", activity.StatusDescription);

        activity.Dispose();
    }
}
