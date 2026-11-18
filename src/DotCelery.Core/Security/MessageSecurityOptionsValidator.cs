using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.Security;

internal sealed partial class MessageSecurityOptionsValidator(
    ILogger<MessageSecurityOptionsValidator>? logger = null
) : IValidateOptions<MessageSecurityOptions>
{
    private int _warned;

    public ValidateOptionsResult Validate(string? name, MessageSecurityOptions options)
    {
        try
        {
            options.Validate();
        }
        catch (InvalidOperationException exception)
        {
            return ValidateOptionsResult.Fail(exception.Message);
        }

        if (
            logger is not null
            && (options.EnableMessageSigning || options.VerifySignaturesWhenPresent)
            && (
                options.SigningKey is { Length: < 32 }
                || options.TenantSigningKeys.Values.Any(key => key.Length < 32)
                || options.AdditionalVerificationKeys.Values.Any(key => key.Length < 32)
                || options.TenantAdditionalVerificationKeys.Values.Any(keys =>
                    keys.Values.Any(key => key.Length < 32)
                )
            )
            && Interlocked.Exchange(ref _warned, 1) == 0
        )
            LogShortKey(logger);
        return ValidateOptionsResult.Success;
    }

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Message signing uses a key shorter than 32 bytes. Rotate it to a randomly generated key of at least 32 bytes."
    )]
    private static partial void LogShortKey(ILogger logger);
}
