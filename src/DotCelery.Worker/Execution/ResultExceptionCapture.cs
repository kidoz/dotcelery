using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;

namespace DotCelery.Worker.Execution;

internal static partial class ResultExceptionCapture
{
    private const int MaxDepth = 5;

    public static TaskExceptionInfo Capture(
        Exception exception,
        WorkerOptions options,
        ILogger logger,
        int depth = 1
    )
    {
        var message = options.ResultExceptionDetail
            is ResultExceptionDetail.Message
                or ResultExceptionDetail.Full
            ? GetMessage(exception, options.ExceptionMessageRedactor, logger)
            : string.Empty;
        var length = Math.Max(0, options.MaxExceptionMessageLength);
        return new TaskExceptionInfo
        {
            Type = exception.GetType().FullName ?? exception.GetType().Name,
            Message = message.Length <= length ? message : message[..length],
            StackTrace =
                options.ResultExceptionDetail == ResultExceptionDetail.Full
                    ? exception.StackTrace
                    : null,
            InnerException =
                depth < MaxDepth && exception.InnerException is { } inner
                    ? Capture(inner, options, logger, depth + 1)
                    : null,
        };
    }

    private static string GetMessage(
        Exception exception,
        Func<Exception, string?>? redact,
        ILogger logger
    )
    {
        if (redact is null)
            return exception.Message;
        try
        {
            return redact(exception) ?? string.Empty;
        }
        catch (Exception)
        {
            LogRedactionFailure(logger);
            return string.Empty;
        }
    }

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "The exception redactor failed. The exception message was omitted from the result."
    )]
    private static partial void LogRedactionFailure(ILogger logger);
}
