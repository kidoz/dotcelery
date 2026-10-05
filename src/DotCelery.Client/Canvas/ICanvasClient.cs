using DotCelery.Core.Canvas;

namespace DotCelery.Client.Canvas;

/// <summary>
/// Client interface for canvas workflows: chains, groups, and chords.
/// </summary>
public interface ICanvasClient
{
    /// <summary>
    /// Sends a chain: each task runs after the previous one succeeds, with the previous result
    /// as its input.
    /// </summary>
    /// <param name="chain">The chain to send.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The chain's ID and the task IDs it runs with.</returns>
    ValueTask<ChainResult> SendChainAsync(
        Chain chain,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Sends a group: its signatures run in parallel.
    /// </summary>
    /// <param name="group">The group to send.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The group's ID and the task IDs it runs with.</returns>
    ValueTask<GroupResult> SendGroupAsync(
        Group group,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Sends a chord: its header runs in parallel and its callback runs once when every header
    /// task finished, however it finished. Requires an <c>IBatchStore</c> registration.
    /// </summary>
    /// <param name="chord">The chord to send.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The chord's ID, its header, and the callback task ID.</returns>
    ValueTask<ChordResult> SendChordAsync(
        Chord chord,
        CancellationToken cancellationToken = default
    );
}
