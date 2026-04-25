using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Logging;

namespace DotCelery.RedisPostgresExample;

public sealed record ResizeImageInput(string SourceUri, int Width, int Height);

public sealed record ResizeImageResult(string OutputUri, int Width, int Height);

public sealed class ResizeImageTask(ILogger<ResizeImageTask> logger)
    : ITask<ResizeImageInput, ResizeImageResult>
{
    public static string TaskName => "media.image.resize";

    public async Task<ResizeImageResult> ExecuteAsync(
        ResizeImageInput input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    )
    {
        logger.LogInformation(
            "Resizing {SourceUri} to {Width}x{Height} for task {TaskId}",
            input.SourceUri,
            input.Width,
            input.Height,
            context.TaskId
        );

        await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);

        var fileName = Path.GetFileName(input.SourceUri);
        return new ResizeImageResult(
            OutputUri: $"s3://media/resized/{input.Width}x{input.Height}/{fileName}",
            Width: input.Width,
            Height: input.Height
        );
    }
}
