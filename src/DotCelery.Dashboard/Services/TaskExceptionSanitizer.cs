using DotCelery.Core.Models;

namespace DotCelery.Dashboard.Services;

/// <summary>
/// Reduces exception information to what the dashboard may show, so task records and summaries
/// never expose more than the configured detail.
/// </summary>
internal static class TaskExceptionSanitizer
{
    /// <summary>
    /// Sanitizes an exception message.
    /// </summary>
    /// <param name="exceptionType">The CLR type name of the exception, if known.</param>
    /// <param name="message">The exception message, if known.</param>
    /// <param name="options">The dashboard options.</param>
    /// <returns>The message to show, or <c>null</c> when there is nothing to show.</returns>
    public static string? Sanitize(string? exceptionType, string? message, DashboardOptions options)
    {
        if (string.IsNullOrEmpty(exceptionType) && string.IsNullOrEmpty(message))
        {
            return null;
        }

        if (options.ExposeExceptionDetails)
        {
            if (message is null)
            {
                return null;
            }

            return message.Length > options.MaxExceptionMessageLength
                ? message[..options.MaxExceptionMessageLength] + "..."
                : message;
        }

        if (string.IsNullOrEmpty(exceptionType))
        {
            return "An error occurred";
        }

        // Only the type name without its namespace, so internals stay private
        var typeName = exceptionType.Contains('.')
            ? exceptionType[(exceptionType.LastIndexOf('.') + 1)..]
            : exceptionType;

        return $"{typeName}: Task execution failed";
    }

    /// <summary>
    /// Sanitizes a task result's exception information.
    /// </summary>
    /// <param name="exception">The exception information, if any.</param>
    /// <param name="options">The dashboard options.</param>
    /// <returns>The message to show, or <c>null</c> when there is nothing to show.</returns>
    public static string? Sanitize(TaskExceptionInfo? exception, DashboardOptions options) =>
        exception is null ? null : Sanitize(exception.Type, exception.Message, options);
}
