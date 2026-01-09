namespace DotCelery.Core.MultiTenancy;

/// <summary>
/// Routes messages to tenant-specific queues.
/// </summary>
public interface ITenantRouter
{
    /// <summary>
    /// Gets the queue name for a tenant.
    /// </summary>
    /// <param name="tenantId">The tenant ID.</param>
    /// <param name="baseQueue">The base queue name.</param>
    /// <returns>The tenant-specific queue name.</returns>
    string GetQueue(string tenantId, string baseQueue = "celery");

    /// <summary>
    /// Adds a custom queue mapping for a tenant.
    /// </summary>
    /// <param name="tenantId">The tenant ID.</param>
    /// <param name="queue">The queue name to use.</param>
    void AddTenantQueue(string tenantId, string queue);

    /// <summary>
    /// Sets the queue prefix for tenant isolation.
    /// </summary>
    /// <param name="prefix">The prefix to prepend to queue names.</param>
    void SetQueuePrefix(string prefix);

    /// <summary>
    /// Sets the queue suffix for tenant isolation.
    /// </summary>
    /// <param name="suffix">The suffix to append to queue names.</param>
    void SetQueueSuffix(string suffix);

    /// <summary>
    /// Gets all configured tenant queues.
    /// </summary>
    /// <returns>Dictionary of tenant IDs to queue names.</returns>
    IReadOnlyDictionary<string, string> GetTenantQueues();
}

/// <summary>
/// Options for multi-tenancy configuration.
/// </summary>
public sealed class MultiTenancyOptions
{
    /// <summary>
    /// Gets or sets whether multi-tenancy is enabled.
    /// Default is false.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Gets or sets the header name for tenant ID.
    /// Default is "X-Tenant-Id".
    /// </summary>
    public string TenantIdHeader { get; set; } = "X-Tenant-Id";

    /// <summary>
    /// Gets or sets the default tenant ID when not specified.
    /// Default is "default".
    /// </summary>
    public string DefaultTenantId { get; set; } = "default";

    /// <summary>
    /// Gets or sets the queue naming strategy.
    /// </summary>
    public TenantQueueStrategy QueueStrategy { get; set; } = TenantQueueStrategy.Suffix;

    /// <summary>
    /// Gets or sets the separator between base queue and tenant ID.
    /// Default is "-".
    /// </summary>
    public string Separator { get; set; } = "-";

    /// <summary>
    /// Gets or sets whether to validate tenant IDs.
    /// Default is false.
    /// </summary>
    public bool ValidateTenants { get; set; }

    /// <summary>
    /// Gets or sets the list of valid tenant IDs when validation is enabled.
    /// </summary>
    public IReadOnlyList<string> ValidTenants { get; set; } = [];
}

/// <summary>
/// Strategy for naming tenant queues.
/// </summary>
public enum TenantQueueStrategy
{
    /// <summary>
    /// Append tenant ID as suffix: "celery-tenant1"
    /// </summary>
    Suffix,

    /// <summary>
    /// Prepend tenant ID as prefix: "tenant1-celery"
    /// </summary>
    Prefix,

    /// <summary>
    /// Use tenant ID as separate queue path: "tenant1.celery"
    /// </summary>
    Path,

    /// <summary>
    /// Use custom mapping only, no automatic naming.
    /// </summary>
    Custom,
}
