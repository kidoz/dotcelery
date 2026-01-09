using System.Collections.Frozen;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.MultiTenancy;

/// <summary>
/// Default implementation of <see cref="ITenantRouter"/>.
/// </summary>
public sealed class TenantRouter : ITenantRouter
{
    private readonly MultiTenancyOptions _options;
    private readonly object _lock = new();
    private Dictionary<string, string> _tenantQueues = new();
    private FrozenDictionary<string, string> _frozenQueues = FrozenDictionary<string, string>.Empty;
    private string? _prefix;
    private string? _suffix;

    /// <summary>
    /// Initializes a new instance of the <see cref="TenantRouter"/> class.
    /// </summary>
    public TenantRouter(IOptions<MultiTenancyOptions> options)
    {
        _options = options.Value;
    }

    /// <inheritdoc />
    public string GetQueue(string tenantId, string baseQueue = "celery")
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);

        // Check custom mapping first
        if (_frozenQueues.TryGetValue(tenantId, out var customQueue))
        {
            return customQueue;
        }

        // Validate tenant if enabled
        if (_options.ValidateTenants && _options.ValidTenants.Count > 0)
        {
            if (!_options.ValidTenants.Contains(tenantId))
            {
                throw new InvalidOperationException($"Unknown tenant: {tenantId}");
            }
        }

        // Apply naming strategy
        return _options.QueueStrategy switch
        {
            TenantQueueStrategy.Suffix =>
                $"{ApplyPrefixSuffix(baseQueue)}{_options.Separator}{tenantId}",
            TenantQueueStrategy.Prefix =>
                $"{tenantId}{_options.Separator}{ApplyPrefixSuffix(baseQueue)}",
            TenantQueueStrategy.Path => $"{tenantId}.{ApplyPrefixSuffix(baseQueue)}",
            TenantQueueStrategy.Custom => throw new InvalidOperationException(
                $"No custom queue mapping found for tenant: {tenantId}"
            ),
            _ => ApplyPrefixSuffix(baseQueue),
        };
    }

    /// <inheritdoc />
    public void AddTenantQueue(string tenantId, string queue)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(queue);

        lock (_lock)
        {
            _tenantQueues[tenantId] = queue;
            _frozenQueues = _tenantQueues.ToFrozenDictionary();
        }
    }

    /// <inheritdoc />
    public void SetQueuePrefix(string prefix)
    {
        _prefix = prefix;
    }

    /// <inheritdoc />
    public void SetQueueSuffix(string suffix)
    {
        _suffix = suffix;
    }

    /// <inheritdoc />
    public IReadOnlyDictionary<string, string> GetTenantQueues()
    {
        return _frozenQueues;
    }

    private string ApplyPrefixSuffix(string queue)
    {
        var result = queue;

        if (!string.IsNullOrEmpty(_prefix))
        {
            result = $"{_prefix}{result}";
        }

        if (!string.IsNullOrEmpty(_suffix))
        {
            result = $"{result}{_suffix}";
        }

        return result;
    }
}
