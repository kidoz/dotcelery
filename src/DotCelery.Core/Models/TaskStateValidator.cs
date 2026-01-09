namespace DotCelery.Core.Models;

/// <summary>
/// Validates task state transitions according to the task lifecycle state machine.
/// </summary>
public static class TaskStateValidator
{
    /// <summary>
    /// Valid state transitions. Key is current state, value is set of valid next states.
    /// </summary>
    private static readonly Dictionary<TaskState, HashSet<TaskState>> ValidTransitions = new()
    {
        [TaskState.Pending] = [TaskState.Received, TaskState.Revoked],
        [TaskState.Received] = [TaskState.Started, TaskState.Revoked],
        [TaskState.Started] =
        [
            TaskState.Success,
            TaskState.Failure,
            TaskState.Retry,
            TaskState.Revoked,
            TaskState.Rejected,
            TaskState.Requeued,
            TaskState.Progress,
        ],
        [TaskState.Retry] =
        [
            TaskState.Received,
            TaskState.Failure,
            TaskState.Revoked,
            TaskState.Rejected,
        ],
        [TaskState.Requeued] = [TaskState.Received, TaskState.Revoked],
        [TaskState.Progress] =
        [
            TaskState.Progress,
            TaskState.Success,
            TaskState.Failure,
            TaskState.Revoked,
            TaskState.Rejected,
        ],
        // Terminal states - no valid transitions out
        [TaskState.Success] = [],
        [TaskState.Failure] = [],
        [TaskState.Revoked] = [],
        [TaskState.Rejected] = [],
    };

    /// <summary>
    /// Terminal states that cannot transition to other states.
    /// </summary>
    public static readonly IReadOnlySet<TaskState> TerminalStates = new HashSet<TaskState>
    {
        TaskState.Success,
        TaskState.Failure,
        TaskState.Revoked,
        TaskState.Rejected,
    };

    /// <summary>
    /// Validates whether a state transition is valid.
    /// </summary>
    /// <param name="from">The current state.</param>
    /// <param name="to">The target state.</param>
    /// <returns>True if the transition is valid.</returns>
    public static bool IsValidTransition(TaskState from, TaskState to)
    {
        // Same state is always valid (idempotent)
        if (from == to)
        {
            return true;
        }

        return ValidTransitions.TryGetValue(from, out var validTargets)
            && validTargets.Contains(to);
    }

    /// <summary>
    /// Validates whether a state transition is valid.
    /// </summary>
    /// <param name="from">The current state (null if task doesn't exist yet).</param>
    /// <param name="to">The target state.</param>
    /// <returns>True if the transition is valid.</returns>
    public static bool IsValidTransition(TaskState? from, TaskState to)
    {
        // No current state - only Pending or Received are valid initial states
        if (from is null)
        {
            return to is TaskState.Pending or TaskState.Received;
        }

        return IsValidTransition(from.Value, to);
    }

    /// <summary>
    /// Checks whether a state is terminal (no further transitions allowed).
    /// </summary>
    /// <param name="state">The state to check.</param>
    /// <returns>True if the state is terminal.</returns>
    public static bool IsTerminal(TaskState state) => TerminalStates.Contains(state);

    /// <summary>
    /// Gets the valid target states from a given state.
    /// </summary>
    /// <param name="state">The current state.</param>
    /// <returns>The set of valid target states.</returns>
    public static IReadOnlySet<TaskState> GetValidTransitions(TaskState state)
    {
        return ValidTransitions.TryGetValue(state, out var validTargets)
            ? validTargets
            : new HashSet<TaskState>();
    }

    /// <summary>
    /// Validates a transition and throws if invalid.
    /// </summary>
    /// <param name="from">The current state.</param>
    /// <param name="to">The target state.</param>
    /// <exception cref="InvalidOperationException">Thrown if the transition is invalid.</exception>
    public static void ValidateTransition(TaskState from, TaskState to)
    {
        if (!IsValidTransition(from, to))
        {
            throw new InvalidOperationException(
                $"Invalid state transition from {from} to {to}. "
                    + $"Valid transitions from {from}: {string.Join(", ", GetValidTransitions(from))}"
            );
        }
    }
}
