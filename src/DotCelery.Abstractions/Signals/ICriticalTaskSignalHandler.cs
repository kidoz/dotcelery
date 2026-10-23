namespace DotCelery.Core.Signals;

/// <summary>
/// Marks an orchestration handler whose failure must prevent settlement of the parent task.
/// Observational handlers may still log and continue on failure.
/// </summary>
public interface ICriticalTaskSignalHandler;
