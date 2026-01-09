namespace DotCelery.Core.MultiTenancy;

/// <summary>
/// Provides access to the current tenant context during task execution.
/// </summary>
public interface ITenantContext
{
    /// <summary>
    /// Gets the current tenant ID.
    /// </summary>
    string? TenantId { get; }

    /// <summary>
    /// Gets whether a tenant is set.
    /// </summary>
    bool HasTenant { get; }
}

/// <summary>
/// Default implementation of <see cref="ITenantContext"/>.
/// Uses AsyncLocal for ambient context.
/// </summary>
public sealed class TenantContext : ITenantContext
{
    private static readonly AsyncLocal<TenantHolder?> _currentHolder = new();

    /// <summary>
    /// Gets the current tenant context instance.
    /// </summary>
    public static TenantContext Current { get; } = new();

    /// <inheritdoc />
    public string? TenantId => _currentHolder.Value?.TenantId;

    /// <inheritdoc />
    public bool HasTenant => !string.IsNullOrEmpty(_currentHolder.Value?.TenantId);

    /// <summary>
    /// Sets the current tenant ID for the async context.
    /// </summary>
    /// <param name="tenantId">The tenant ID.</param>
    /// <returns>A disposable that restores the previous tenant on dispose.</returns>
    public static IDisposable SetTenant(string? tenantId)
    {
        var previousHolder = _currentHolder.Value;
        _currentHolder.Value = new TenantHolder(tenantId);
        return new TenantScope(previousHolder);
    }

    private sealed class TenantHolder(string? tenantId)
    {
        public string? TenantId { get; } = tenantId;
    }

    private sealed class TenantScope(TenantHolder? previousHolder) : IDisposable
    {
        public void Dispose()
        {
            _currentHolder.Value = previousHolder;
        }
    }
}

/// <summary>
/// Extension methods for tenant context.
/// </summary>
public static class TenantContextExtensions
{
    /// <summary>
    /// Gets the tenant ID from message headers.
    /// </summary>
    /// <param name="headers">The message headers.</param>
    /// <param name="headerName">The header name containing tenant ID.</param>
    /// <returns>The tenant ID or null.</returns>
    public static string? GetTenantId(
        this IReadOnlyDictionary<string, string>? headers,
        string headerName = "X-Tenant-Id"
    )
    {
        if (headers is null)
        {
            return null;
        }

        return headers.GetValueOrDefault(headerName);
    }
}
