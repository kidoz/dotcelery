using DotCelery.Core.Abstractions;

namespace DotCelery.Worker.Execution;

/// <summary>
/// Gates a worker's messages on circuit breakers and feeds them infrastructure failures only: a
/// task that fails is handled by its retries, not by stopping its queue.
/// </summary>
/// <remarks>
/// Each queue has its own circuit, and a global circuit additionally covers failures that
/// affect every queue.
/// </remarks>
internal sealed class CircuitBreakerMonitor(ICircuitBreakerFactory? factory)
{
    /// <summary>
    /// Gets the open circuit that should stop a message from running, or <c>null</c> when its
    /// queue and the global circuit are both closed.
    /// </summary>
    public ICircuitBreaker? OpenFor(BrokerMessage message)
    {
        if (factory is null)
        {
            return null;
        }

        var queueCircuit = factory.GetOrCreate(message.Queue);
        if (!queueCircuit.IsAllowed)
        {
            return queueCircuit;
        }

        var global = factory.GlobalCircuitBreaker;
        return !ReferenceEquals(queueCircuit, global) && !global.IsAllowed ? global : null;
    }

    public void RecordSuccess(BrokerMessage message) =>
        Record(message, circuit => circuit.RecordSuccess());

    public void RecordFailure(BrokerMessage message, Exception exception) =>
        Record(message, circuit => circuit.RecordFailure(exception));

    private void Record(BrokerMessage message, Action<ICircuitBreaker> record)
    {
        if (factory is null)
        {
            return;
        }

        var queueCircuit = factory.GetOrCreate(message.Queue);
        record(queueCircuit);

        var global = factory.GlobalCircuitBreaker;
        if (!ReferenceEquals(queueCircuit, global))
        {
            record(global);
        }
    }
}
