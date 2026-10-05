using System.Text.Json;
using System.Text.Json.Serialization;

namespace DotCelery.Beat;

/// <summary>
/// Remembers when each schedule entry last ran, so a restart resumes the schedule instead of
/// starting it over.
/// </summary>
internal static class ScheduleStateStore
{
    /// <summary>
    /// Loads the last run time of every entry from the state file.
    /// </summary>
    /// <param name="path">The state file path.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The last run time by entry name; empty when there is no readable state.</returns>
    public static async Task<Dictionary<string, DateTimeOffset>> LoadAsync(
        string path,
        CancellationToken cancellationToken
    )
    {
        if (!File.Exists(path))
        {
            return new Dictionary<string, DateTimeOffset>(StringComparer.Ordinal);
        }

        try
        {
            await using var stream = File.OpenRead(path);
            var state = await JsonSerializer
                .DeserializeAsync(stream, BeatStateJsonContext.Default.BeatState, cancellationToken)
                .ConfigureAwait(false);

            return state is null
                ? new Dictionary<string, DateTimeOffset>(StringComparer.Ordinal)
                : new Dictionary<string, DateTimeOffset>(state.Entries, StringComparer.Ordinal);
        }
        catch (Exception ex) when (ex is IOException or JsonException)
        {
            // Unreadable state starts the schedule over instead of stopping the scheduler
            return new Dictionary<string, DateTimeOffset>(StringComparer.Ordinal);
        }
    }

    /// <summary>
    /// Writes the last run time of every entry that has run.
    /// </summary>
    /// <param name="path">The state file path.</param>
    /// <param name="entries">The schedule entries.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static async Task SaveAsync(
        string path,
        IEnumerable<ScheduleEntry> entries,
        CancellationToken cancellationToken
    )
    {
        var state = new BeatState();
        foreach (var entry in entries)
        {
            if (entry.LastRunTime is { } lastRun)
            {
                state.Entries[entry.Name] = lastRun;
            }
        }

        // Write beside the state file and replace it, so a crash cannot leave it half written
        var temporaryPath = path + ".tmp";
        await using (var stream = File.Create(temporaryPath))
        {
            await JsonSerializer
                .SerializeAsync(
                    stream,
                    state,
                    BeatStateJsonContext.Default.BeatState,
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        File.Move(temporaryPath, path, overwrite: true);
    }
}

/// <summary>
/// The persisted schedule state.
/// </summary>
internal sealed class BeatState
{
    /// <summary>
    /// Gets or sets the last run time of each entry by name.
    /// </summary>
    public Dictionary<string, DateTimeOffset> Entries { get; init; } = new(StringComparer.Ordinal);
}

[JsonSerializable(typeof(BeatState))]
internal sealed partial class BeatStateJsonContext : JsonSerializerContext;
