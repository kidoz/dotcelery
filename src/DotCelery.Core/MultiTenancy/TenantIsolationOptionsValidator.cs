using DotCelery.Core.Security;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.MultiTenancy;

internal sealed class TenantIsolationOptionsValidator(IOptions<MessageSecurityOptions> security)
    : IValidateOptions<MultiTenancyOptions>
{
    public ValidateOptionsResult Validate(string? name, MultiTenancyOptions options)
    {
        if (!options.Enabled || !options.RequireAuthenticatedTenants)
            return ValidateOptionsResult.Success;
        var messages = security.Value;
        if (
            !messages.EnableMessageSigning
            || !messages.RejectUnsignedMessages
            || messages.TenantSigningKeys.Count == 0
        )
            return ValidateOptionsResult.Fail(
                "Tenant isolation requires signing, rejection of unsigned messages, and independent TenantSigningKeys."
            );
        messages.Validate();
        if (options.ValidTenants.Any(tenant => !messages.TenantSigningKeys.ContainsKey(tenant)))
            return ValidateOptionsResult.Fail("Every allowed tenant requires its own signing key.");
        return ValidateOptionsResult.Success;
    }
}
