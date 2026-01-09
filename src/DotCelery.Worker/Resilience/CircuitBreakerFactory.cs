using System.Collections.Concurrent;
using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Resilience;

/// <summary>
/// Factory for creating and managing circuit breakers per queue.
/// </summary>
public sealed class CircuitBreakerFactory : ICircuitBreakerFactory, IDisposable
{
    private readonly ConcurrentDictionary<string, CircuitBreaker> _circuitBreakers = new();
    private readonly CircuitBreakerOptions _options;
    private readonly ILoggerFactory _loggerFactory;
    private readonly TimeProvider _timeProvider;
    private readonly CircuitBreaker _global;

    /// <summary>
    /// Initializes a new instance of the <see cref="CircuitBreakerFactory"/> class.
    /// </summary>
    public CircuitBreakerFactory(
        IOptions<CircuitBreakerOptions> options,
        ILoggerFactory loggerFactory,
        TimeProvider? timeProvider = null
    )
    {
        _options = options.Value;
        _loggerFactory = loggerFactory;
        _timeProvider = timeProvider ?? TimeProvider.System;

        _global = CreateCircuitBreaker("global");
    }

    /// <inheritdoc />
    public ICircuitBreaker GlobalCircuitBreaker => _global;

    /// <inheritdoc />
    public IReadOnlyCollection<ICircuitBreaker> All
    {
        get
        {
            var all = new List<ICircuitBreaker> { _global };
            all.AddRange(_circuitBreakers.Values);
            return all.AsReadOnly();
        }
    }

    /// <inheritdoc />
    public ICircuitBreaker GetOrCreate(string queueName)
    {
        ArgumentException.ThrowIfNullOrEmpty(queueName);

        if (!_options.UsePerQueueCircuitBreakers)
        {
            return GlobalCircuitBreaker;
        }

        return _circuitBreakers.GetOrAdd(queueName, CreateCircuitBreaker);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _global.Dispose();

        foreach (var cb in _circuitBreakers.Values)
        {
            cb.Dispose();
        }

        _circuitBreakers.Clear();
    }

    private CircuitBreaker CreateCircuitBreaker(string name)
    {
        var logger = _loggerFactory.CreateLogger<CircuitBreaker>();
        return new CircuitBreaker(name, _options, logger, _timeProvider);
    }
}
