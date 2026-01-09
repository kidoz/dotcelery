using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DotCelery.Worker.Execution;

/// <summary>
/// A service provider wrapper that restricts access to sensitive services.
/// </summary>
internal sealed class RestrictedServiceProvider : IServiceProvider
{
    private readonly IServiceProvider _inner;
    private readonly HashSet<Type> _blockedTypes;
    private readonly ILogger? _logger;

    /// <summary>
    /// Default set of blocked service types that tasks should not access directly.
    /// </summary>
    public static readonly IReadOnlySet<Type> DefaultBlockedTypes = new HashSet<Type>
    {
        typeof(IServiceProvider),
        typeof(IServiceScopeFactory),
        typeof(IServiceScope),
    };

    /// <summary>
    /// Initializes a new instance of the <see cref="RestrictedServiceProvider"/> class.
    /// </summary>
    /// <param name="inner">The inner service provider to wrap.</param>
    /// <param name="blockedTypes">Types that should be blocked from resolution.</param>
    /// <param name="logger">Optional logger for blocked access attempts.</param>
    public RestrictedServiceProvider(
        IServiceProvider inner,
        IEnumerable<Type>? blockedTypes = null,
        ILogger? logger = null
    )
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _blockedTypes = blockedTypes is not null
            ? new HashSet<Type>(blockedTypes)
            : new HashSet<Type>(DefaultBlockedTypes);
        _logger = logger;
    }

    /// <inheritdoc />
    public object? GetService(Type serviceType)
    {
        ArgumentNullException.ThrowIfNull(serviceType);

        if (IsBlocked(serviceType))
        {
            _logger?.LogWarning(
                "Blocked task access to restricted service type: {ServiceType}",
                serviceType.FullName
            );
            throw new InvalidOperationException(
                $"Access to service type '{serviceType.FullName}' is not allowed from task context. "
                    + "Inject required services through the task constructor instead."
            );
        }

        return _inner.GetService(serviceType);
    }

    private bool IsBlocked(Type serviceType)
    {
        // Check exact type match
        if (_blockedTypes.Contains(serviceType))
        {
            return true;
        }

        // Check if requesting a blocked generic type
        if (serviceType.IsGenericType)
        {
            var genericDef = serviceType.GetGenericTypeDefinition();
            if (_blockedTypes.Contains(genericDef))
            {
                return true;
            }
        }

        // Check interfaces that the service type implements
        foreach (var iface in serviceType.GetInterfaces())
        {
            if (_blockedTypes.Contains(iface))
            {
                return true;
            }
        }

        return false;
    }
}
