using System.Text.Json;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.MultiTenancy;
using DotCelery.Core.Security;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Execution;

internal sealed class TenantDeliveryValidator(
    MultiTenancyOptions? tenancyOptions,
    IServiceProvider services
)
{
    private readonly Lazy<(
        MessageSecurityOptions? Security,
        IMessageSecurityValidator? Validator,
        ITenantRouter? Router
    )> _auth = new(() =>
        (
            services.GetService<IOptions<MessageSecurityOptions>>()?.Value,
            services.GetService<IMessageSecurityValidator>(),
            services.GetService<ITenantRouter>()
        )
    );

    public bool RequiresAuthentication => tenancyOptions is { RequireAuthenticatedTenants: true };

    public string? Validate(BrokerMessage delivery)
    {
        if (tenancyOptions is not { RequireAuthenticatedTenants: true } tenancy)
            return null;
        var message = delivery.Message;
        var (security, validator, router) = _auth.Value;
        if (
            message.TenantId is not { Length: > 0 } tenant
            || security is null
            || validator is null
            || !security.EnableMessageSigning
            || !security.RejectUnsignedMessages
            || !security.TenantSigningKeys.ContainsKey(tenant)
            || delivery.RawBody is not { Length: > 0 } body
            || string.IsNullOrEmpty(delivery.Signature)
            || !validator.VerifySignature(body, delivery.Signature)
        )
            return "The delivery has no authenticated tenant identity.";
        if (tenancy.ValidTenants.Count > 0 && !tenancy.ValidTenants.Contains(tenant))
            return "The tenant is not authorized on this worker.";
        if (message.Headers.GetTenantId(tenancy.TenantIdHeader) is { } header && header != tenant)
            return "The tenant header conflicts with the authenticated identity.";
        if (
            router is null
            || !router
                .GetTenantQueues()
                .Any(pair => pair.Key == tenant && pair.Value == delivery.Queue)
        )
            return "Tenant isolation requires an explicit tenant-to-delivery-queue mapping.";
        if (!string.Equals(message.Queue, delivery.Queue, StringComparison.Ordinal))
            return "The declared queue does not match the delivery queue.";
        try
        {
            var parsed = JsonSerializer.Deserialize(body, DotCeleryJsonContext.Default.TaskMessage);
            if (
                parsed is null
                || !JsonSerializer
                    .SerializeToUtf8Bytes(parsed, DotCeleryJsonContext.Default.TaskMessage)
                    .AsSpan()
                    .SequenceEqual(
                        JsonSerializer.SerializeToUtf8Bytes(
                            message,
                            DotCeleryJsonContext.Default.TaskMessage
                        )
                    )
            )
                return "The delivered task does not match its authenticated body.";
        }
        catch (JsonException)
        {
            return "The authenticated body is unreadable.";
        }
        return null;
    }

    // Makes the tenant of the message ambient for the task and its remaining filters, and
    // restores the previous tenant when the task is done
    public IDisposable? EnterScope(TaskMessage message) =>
        tenancyOptions is null ? null
        : tenancyOptions.RequireAuthenticatedTenants
            ? TenantContext.SetTenant(tenancyOptions.ResolveTenantId(message), message.Queue)
        : TenantContext.SetTenant(tenancyOptions.ResolveTenantId(message));
}
