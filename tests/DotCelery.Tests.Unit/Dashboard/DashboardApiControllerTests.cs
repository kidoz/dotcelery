using DotCelery.Core.Abstractions;
using DotCelery.Core.Dashboard;
using DotCelery.Core.Models;
using DotCelery.Dashboard;
using DotCelery.Dashboard.Controllers;
using DotCelery.Dashboard.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace DotCelery.Tests.Unit.Dashboard;

/// <summary>
/// Exercises the dashboard API's request bounds: paging is clamped into the configured page
/// size and a bulk operation is refused when it exceeds the configured limit.
/// </summary>
public sealed class DashboardApiControllerTests
{
    private readonly IDashboardDataProvider _provider = Substitute.For<IDashboardDataProvider>();
    private readonly IRevocationStore _revocations = Substitute.For<IRevocationStore>();

    public DashboardApiControllerTests()
    {
#pragma warning disable CA2012 // The ValueTask is consumed by NSubstitute's Returns extension
        _provider
            .GetTasksByStateAsync(
                Arg.Any<TaskState>(),
                Arg.Any<int>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>()
            )
            .Returns(_ => ValueTask.FromResult<IReadOnlyList<TaskSummary>>([]));
#pragma warning restore CA2012
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(31)]
    public async Task HistoricalEndpoints_RejectInvalidRangesBeforeCallingTheStore(int days)
    {
        var history = Substitute.For<IHistoricalDataStore>();
        var controller = new DashboardApiController(
            _provider,
            Options.Create(new DashboardOptions()),
            historicalDataStore: history
        );
        var from = DateTimeOffset.UtcNow;
        var until = from.AddDays(days);
        Assert.IsType<BadRequestObjectResult>(
            (await controller.GetHistoricalMetrics(from, until)).Result
        );
        Assert.IsType<BadRequestObjectResult>((await controller.GetTimeSeries(from, until)).Result);
        Assert.IsType<BadRequestObjectResult>(
            (await controller.GetMetricsByTask(from, until)).Result
        );
        Assert.Empty(history.ReceivedCalls());
    }

    [Fact]
    public async Task HistoricalSnapshotBudgetFailure_IsReportedAsBadRequest()
    {
        var history = Substitute.For<IHistoricalDataStore>();
#pragma warning disable CA2012 // NSubstitute consumes the configured ValueTask.
        history
            .GetMetricsAsync(
                Arg.Any<DateTimeOffset>(),
                Arg.Any<DateTimeOffset>(),
                Arg.Any<MetricsGranularity>(),
                Arg.Any<CancellationToken>()
            )
            .Returns(_ =>
                ValueTask.FromException<AggregatedMetrics>(
                    new ArgumentOutOfRangeException("until", "Snapshot budget exceeded")
                )
            );
#pragma warning restore CA2012
        var controller = new DashboardApiController(
            _provider,
            Options.Create(new DashboardOptions()),
            historicalDataStore: history
        );
        Assert.IsType<BadRequestObjectResult>(
            (
                await controller.GetHistoricalMetrics(
                    DateTimeOffset.UtcNow.AddHours(-1),
                    DateTimeOffset.UtcNow
                )
            ).Result
        );
    }

    [Fact]
    public async Task GetTasks_ClampsNegativePaging()
    {
        var controller = CreateController(o => o.PageSize = 50);

        var result = await controller.GetTasks(
            state: "Success",
            taskName: null,
            page: 0,
            pageSize: -5,
            CancellationToken.None
        );

        await _provider
            .Received(1)
            .GetTasksByStateAsync(TaskState.Success, 1, 0, Arg.Any<CancellationToken>());

        var response = OkValue(result);
        Assert.Equal(1, response.Page);
        Assert.Equal(1, response.PageSize);
    }

    [Fact]
    public async Task GetTasks_CapsThePageSizeAndComputesTheOffset()
    {
        var controller = CreateController(o => o.PageSize = 50);

        var result = await controller.GetTasks(
            state: "Success",
            taskName: null,
            page: 3,
            pageSize: 9999,
            CancellationToken.None
        );

        await _provider
            .Received(1)
            .GetTasksByStateAsync(TaskState.Success, 50, 100, Arg.Any<CancellationToken>());

        var response = OkValue(result);
        Assert.Equal(50, response.PageSize);
    }

    [Fact]
    public async Task GetTasks_AHugePageDoesNotOverflowTheOffset()
    {
        var controller = CreateController();

        await controller.GetTasks(
            state: "Success",
            taskName: null,
            page: int.MaxValue,
            pageSize: 50,
            CancellationToken.None
        );

        await _provider
            .Received(1)
            .GetTasksByStateAsync(
                TaskState.Success,
                50,
                int.MaxValue,
                Arg.Any<CancellationToken>()
            );
    }

    [Fact]
    public async Task BulkRevoke_BeyondTheBulkLimit_IsRefusedWithoutRevoking()
    {
        var controller = CreateController(o => o.MaxBulkOperationSize = 2);

        var result = await controller.BulkRevoke(
            new BulkRevokeRequest { TaskIds = ["1", "2", "3"] },
            CancellationToken.None
        );

        Assert.IsType<BadRequestObjectResult>(result.Result);
        await _revocations
            .DidNotReceiveWithAnyArgs()
            .RevokeAsync(
                Arg.Any<string>(),
                Arg.Any<RevokeOptions?>(),
                Arg.Any<CancellationToken>()
            );
    }

    [Fact]
    public async Task BulkRevoke_WithinTheBulkLimit_RevokesEveryTask()
    {
        var controller = CreateController(o => o.MaxBulkOperationSize = 3);

        var result = await controller.BulkRevoke(
            new BulkRevokeRequest { TaskIds = ["1", "2"] },
            CancellationToken.None
        );

        var response = OkValue(result);
        Assert.Equal(2, response.SuccessCount);
        await _revocations
            .Received(1)
            .RevokeAsync("1", Arg.Any<RevokeOptions>(), Arg.Any<CancellationToken>());
        await _revocations
            .Received(1)
            .RevokeAsync("2", Arg.Any<RevokeOptions>(), Arg.Any<CancellationToken>());
    }

    private DashboardApiController CreateController(Action<DashboardOptions>? configure = null)
    {
        var options = new DashboardOptions
        {
            RequireAuthorization = false,
            AllowTaskOperations = true,
        };
        configure?.Invoke(options);

        return new DashboardApiController(
            _provider,
            Options.Create(options),
            revocationStore: _revocations
        )
        {
            ControllerContext = new ControllerContext { HttpContext = new DefaultHttpContext() },
        };
    }

    private static T OkValue<T>(ActionResult<T> result)
        where T : class => Assert.IsType<T>(Assert.IsType<OkObjectResult>(result.Result).Value);
}
