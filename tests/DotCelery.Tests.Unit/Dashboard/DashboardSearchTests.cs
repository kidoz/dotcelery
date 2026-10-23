using System.Runtime.CompilerServices;
using DotCelery.Backend.InMemory.Storage;
using DotCelery.Broker.InMemory;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Dashboard;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage.Stores;
using DotCelery.Dashboard;
using DotCelery.Dashboard.Controllers;
using DotCelery.Dashboard.Models;
using DotCelery.Dashboard.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace DotCelery.Tests.Unit.Dashboard;

public sealed class DashboardSearchTests
{
    private readonly TaskRecordStore _records = new(
        Options.Create(new DashboardOptions()),
        new InMemoryStorageProvider()
    );
    private readonly IDashboardDataProvider _provider = Substitute.For<IDashboardDataProvider>();
    private static readonly DateTimeOffset Start = DateTimeOffset.UtcNow.AddHours(-1);

    [Fact]
    public async Task All_IncludesEveryState_InNewestFirstOrder()
    {
        await Record("pending", "mail.send", TaskState.Pending, 0);
        await Record("failed", "mail.send", TaskState.Failure, 1);
        await Record("success", "mail.send", TaskState.Success, 2);
        var result = await Controller().GetTasks(null, null, 1, 50);
        Assert.Equal(["success", "failed", "pending"], Value(result).Tasks.Select(t => t.TaskId));
    }

    [Fact]
    public async Task CombinedFilters_AreAppliedBeforePaging_WithExactHasMore()
    {
        await Record("a", "mail.send", TaskState.Failure, 0);
        await Record("b", "mail.send", TaskState.Success, 1);
        await Record("c", "mail.send", TaskState.Failure, 2, queue: "other");
        await Record("d", "mail.send", TaskState.Failure, 3, worker: "other");
        await Record("e", "mail.send", TaskState.Failure, 4);
        await Record("f", "other.task", TaskState.Failure, 5);
        var query = new TaskSearchRequest
        {
            State = TaskState.Failure,
            TaskName = "mail.send",
            Queue = "celery",
            Worker = "w1",
            PageSize = 1,
        };
        var controller = Controller();
        var first = Value(await controller.SearchTasks(query));
        Assert.Equal("e", Assert.Single(first.Tasks).TaskId);
        Assert.True(first.HasMore);
        var second = Value(await controller.SearchTasks(query with { Page = 2 }));
        Assert.Equal("a", Assert.Single(second.Tasks).TaskId);
        Assert.False(second.HasMore);
    }

    [Fact]
    public async Task Search_CombinesIdSubstring_TimeRange_AndOldestOrder()
    {
        await Record("job-a", "mail.send", TaskState.Success, 0);
        await Record("job-b", "mail.send", TaskState.Success, 1);
        await Record("job-c", "mail.send", TaskState.Success, 2);
        var result = Value(
            await Controller()
                .SearchTasks(
                    new TaskSearchRequest
                    {
                        Search = "JOB",
                        From = Start.AddMinutes(1),
                        Until = Start.AddMinutes(3),
                        OldestFirst = true,
                    }
                )
        );
        Assert.Equal(["job-b", "job-c"], result.Tasks.Select(t => t.TaskId));
    }

    [Fact]
    public async Task BulkPreview_CombinesNameQueueStateAndCompletionRange()
    {
        await Record("matching", "mail.send", TaskState.Failure, 2);
        await Record("wrong-queue", "mail.send", TaskState.Failure, 3, queue: "other");
        await Record("wrong-state", "mail.send", TaskState.Success, 4);
        await Record("too-old", "mail.send", TaskState.Failure, 0);
        var result = Value(
            await Controller()
                .PreviewFilter(
                    new TaskFilter
                    {
                        State = TaskState.Failure,
                        TaskNamePattern = "mail.*",
                        Queue = "celery",
                        CompletedAfter = Start.AddMinutes(1),
                    }
                )
        );
        Assert.Equal("matching", Assert.Single(result.Tasks).TaskId);
    }

    [Fact]
    public async Task Search_InvalidStateOrRange_IsRejected()
    {
        var controller = Controller();
        Assert.IsType<BadRequestObjectResult>(
            (await controller.SearchTasks(new TaskSearchRequest { State = (TaskState)999 })).Result
        );
        Assert.IsType<BadRequestObjectResult>(
            (
                await controller.SearchTasks(new TaskSearchRequest { From = Start, Until = Start })
            ).Result
        );
    }

    [Fact]
    public async Task Retry_UsesTheDeadLetterEntryId_AndReportsUnavailableTasks()
    {
        var deadLetters = Substitute.For<IDeadLetterStore>();
        deadLetters
            .GetAllAsync(100, 0, Arg.Any<CancellationToken>())
            .Returns(
                Messages(
                    new DeadLetterMessage
                    {
                        Id = "dlq-entry",
                        TaskId = "original-task",
                        TaskName = "mail.send",
                        Queue = "celery",
                        Reason = DeadLetterReason.Failed,
                        OriginalMessage = [],
                        Timestamp = Start,
                    }
                )
            );
        deadLetters.RequeueAsync("dlq-entry", Arg.Any<CancellationToken>()).Returns(true);
        var controller = Controller(deadLetters: deadLetters);
        var result = Value(
            await controller.BulkRetry(
                new BulkRetryRequest { TaskIds = ["original-task", "unavailable"] }
            )
        );
        Assert.Equal(1, result.SuccessCount);
        Assert.Equal(1, result.FailureCount);
        Assert.Equal(["unavailable"], result.FailedTaskIds);
        await deadLetters.Received(1).RequeueAsync("dlq-entry", Arg.Any<CancellationToken>());
        await deadLetters
            .DidNotReceive()
            .RequeueAsync("original-task", Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Retry_RequeuesTheOriginalTaskThroughTheRealStoreAndBroker()
    {
        await using var broker = new InMemoryBroker();
        var serializer = new JsonMessageSerializer();
        await using var deadLetters = new DeadLetterStore(
            new InMemoryStorageProvider(),
            broker,
            serializer
        );
        var original = new TaskMessage
        {
            Id = "original",
            Task = "mail.send",
            Queue = "retries",
            Timestamp = DateTimeOffset.UtcNow,
            ContentType = "application/json",
            Args = "{\"to\":\"test@example.com\"}"u8.ToArray(),
        };
        await deadLetters.StoreAsync(
            new DeadLetterMessage
            {
                Id = "entry",
                TaskId = original.Id,
                TaskName = original.Task,
                Queue = original.Queue,
                Timestamp = DateTimeOffset.UtcNow,
                Reason = DeadLetterReason.Failed,
                OriginalMessage = serializer.Serialize(original),
            }
        );
        var result = Value(
            await Controller(deadLetters: deadLetters)
                .BulkRetry(new BulkRetryRequest { TaskIds = [original.Id] })
        );
        Assert.Equal(1, result.SuccessCount);
        Assert.Null(await deadLetters.GetAsync("entry"));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await using var deliveries = broker
            .ConsumeAsync(["retries"], timeout.Token)
            .GetAsyncEnumerator();
        Assert.True(await deliveries.MoveNextAsync());
        Assert.Equal(original.Id, deliveries.Current.Message.Id);
        Assert.Equal(original.Args, deliveries.Current.Message.Args);
        await broker.AckAsync(deliveries.Current, timeout.Token);
    }

    [Fact]
    public async Task ReadOnly_RefusesOperations_AndDoesNotReadRetryPayloads()
    {
        var deadLetters = Substitute.For<IDeadLetterStore>();
        var controller = Controller(readOnly: true, deadLetters);
        var result = await controller.BulkRetry(new BulkRetryRequest { TaskIds = ["task"] });
        Assert.Equal(403, Assert.IsType<StatusCodeResult>(result.Result).StatusCode);
        await controller.PreviewActions(new BulkRetryRequest { TaskIds = ["task"] });
        deadLetters.DidNotReceiveWithAnyArgs().GetAllAsync(default, default, default);
    }

    private DashboardApiController Controller(
        bool readOnly = false,
        IDeadLetterStore? deadLetters = null
    ) =>
        new(
            _records,
            _provider,
            Options.Create(new DashboardOptions { ReadOnly = readOnly }),
            deadLetterStore: deadLetters
        );

    private ValueTask Record(
        string id,
        string name,
        TaskState state,
        int minute,
        string queue = "celery",
        string worker = "w1"
    ) =>
        _records.RecordAsync(
            new TaskSummary
            {
                TaskId = id,
                TaskName = name,
                State = state.ToString(),
                Queue = queue,
                Worker = worker,
                SentAt = Start.AddMinutes(minute),
                CompletedAt = state is TaskState.Pending ? null : Start.AddMinutes(minute),
            }
        );

    private static T Value<T>(ActionResult<T> result)
        where T : class => Assert.IsType<T>(Assert.IsType<OkObjectResult>(result.Result).Value);

    private static async IAsyncEnumerable<DeadLetterMessage> Messages(
        DeadLetterMessage message,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        cancellationToken.ThrowIfCancellationRequested();
        await Task.CompletedTask;
        yield return message;
    }
}
