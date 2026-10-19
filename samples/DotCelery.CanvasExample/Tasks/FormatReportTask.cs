using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Logging;

namespace DotCelery.CanvasExample.Tasks;

/// <summary>
/// The final report produced by the chain.
/// </summary>
public sealed record ReportResult
{
    public required string Text { get; init; }
}

/// <summary>
/// Last step of the chain: formats the previous step's result.
/// </summary>
public sealed class FormatReportTask(ILogger<FormatReportTask> logger)
    : ITask<SumResult, ReportResult>
{
    public static string TaskName => "canvas.format_report";

    public async Task<ReportResult> ExecuteAsync(
        SumResult input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    )
    {
        await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);

        var text = $"Sum of {input.Count} values = {input.Sum}";

        logger.LogInformation("[Task {TaskId}] Formatted report: {Report}", context.TaskId, text);

        return new ReportResult { Text = text };
    }
}
