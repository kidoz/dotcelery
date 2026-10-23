using DotCelery.Core.Abstractions;
using DotCelery.Core.Batches;
using DotCelery.Core.Canvas;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Client.Canvas;

/// <summary>
/// Default implementation of <see cref="ICanvasClient"/>.
/// </summary>
/// <remarks>
/// A chain carries its remaining steps on the first task's message, so the worker that runs a
/// step publishes the next one with its result; no scheduler or stored workflow is involved. A
/// chord is a group whose callback is the batch completion callback, so it needs the batch
/// store that tracks the header tasks.
/// </remarks>
public sealed class CanvasClient : ICanvasClient
{
    private readonly IMessageBroker _broker;
    private readonly IMessageSerializer _serializer;
    private readonly IBatchStore? _batchStore;
    private readonly CeleryClientOptions _options;
    private readonly ILogger<CanvasClient> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="CanvasClient"/> class.
    /// </summary>
    /// <param name="broker">The message broker.</param>
    /// <param name="serializer">The message serializer.</param>
    /// <param name="options">The client options, for the default queue.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="batchStore">The batch store chords are tracked in, when registered.</param>
    public CanvasClient(
        IMessageBroker broker,
        IMessageSerializer serializer,
        IOptions<CeleryClientOptions> options,
        ILogger<CanvasClient> logger,
        IBatchStore? batchStore = null
    )
    {
        ArgumentNullException.ThrowIfNull(broker);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(options);

        _broker = broker;
        _serializer = serializer;
        _options = options.Value;
        _logger = logger;
        _batchStore = batchStore;
    }

    /// <inheritdoc />
    public async ValueTask<ChainResult> SendChainAsync(
        Chain chain,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(chain);

        var canvasId = Guid.NewGuid().ToString("N");
        var steps = BuildChainSteps(chain.Tasks);

        await PublishSignatureAsync(
                steps[0].Signature,
                steps[0].TaskId,
                canvasId,
                parentId: null,
                chain: [.. steps.Skip(1)],
                cancellationToken
            )
            .ConfigureAwait(false);

        _logger.LogInformation(
            "Sent chain {CanvasId} with {Count} tasks, starting with {FirstTaskId}",
            canvasId,
            steps.Count,
            steps[0].TaskId
        );

        return new ChainResult
        {
            Id = canvasId,
            FirstTaskId = steps[0].TaskId,
            LastTaskId = steps[^1].TaskId,
            TaskIds = [.. steps.Select(step => step.TaskId)],
        };
    }

    /// <inheritdoc />
    public async ValueTask<GroupResult> SendGroupAsync(
        Group group,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(group);

        var canvasId = Guid.NewGuid().ToString("N");

        // A member chain completes with its last task, and a nested group with each of its
        // members, so the result tracks what a caller waits on
        var publishes = BuildMemberPublishes(group);
        var taskIds = publishes.Select(publish => publish.CompletionId).ToList();

        await PublishMembersAsync(publishes, canvasId, batchId: null, cancellationToken)
            .ConfigureAwait(false);

        _logger.LogInformation("Sent group {CanvasId} with {Count} tasks", canvasId, taskIds.Count);

        return new GroupResult { Id = canvasId, TaskIds = taskIds };
    }

    /// <inheritdoc />
    public async ValueTask<ChordResult> SendChordAsync(
        Chord chord,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(chord);

        if (_batchStore is null)
        {
            throw new InvalidOperationException(
                "Chords require an IBatchStore registration, which tracks the header tasks and "
                    + "runs the callback."
            );
        }

        var canvasId = Guid.NewGuid().ToString("N");
        var callback = ToMessageSignature(chord.Callback);
        var callbackTaskId = Guid.NewGuid().ToString("N");

        // The record goes first, so a header task that finishes immediately is still counted;
        // it holds the task each header member completes with, which for a chain is its last step
        var publishes = BuildMemberPublishes(chord.Header);
        var taskIds = publishes.Select(publish => publish.CompletionId).ToList();

        await _batchStore
            .CreateAsync(
                new Batch
                {
                    Id = canvasId,
                    Name = $"chord-{canvasId}",
                    State = BatchState.Pending,
                    TaskIds = taskIds,
                    CreatedAt = DateTimeOffset.UtcNow,
                    Callback = new BatchCallback
                    {
                        TaskId = callbackTaskId,
                        TaskName = callback.TaskName,
                        Args = callback.Args,
                        ContentType = _serializer.ContentType,
                        Queue = callback.Queue,
                        Priority = callback.Priority,
                        MaxRetries = callback.MaxRetries,
                        Headers = callback.Headers,
                    },
                },
                cancellationToken
            )
            .ConfigureAwait(false);

        await PublishMembersAsync(publishes, canvasId, batchId: canvasId, cancellationToken)
            .ConfigureAwait(false);

        _logger.LogInformation(
            "Sent chord {CanvasId} with {Count} header tasks and callback {CallbackTaskId}",
            canvasId,
            taskIds.Count,
            callbackTaskId
        );

        return new ChordResult
        {
            Id = canvasId,
            Header = new GroupResult { Id = canvasId, TaskIds = taskIds },
            CallbackTaskId = callbackTaskId,
        };
    }

    private async Task PublishSignatureAsync(
        Signature signature,
        string taskId,
        string canvasId,
        string? parentId,
        IReadOnlyList<ChainStep>? chain,
        CancellationToken cancellationToken,
        string? batchId = null
    )
    {
        var eta = signature.EffectiveEta;
        var queue = string.IsNullOrWhiteSpace(signature.Queue)
            ? _options.DefaultQueue
            : signature.Queue;

        await _broker
            .PublishAsync(
                TaskMessageFactory.ForTask(
                    taskId,
                    signature.TaskName,
                    signature.Args ?? [],
                    _serializer.ContentType,
                    DateTimeOffset.UtcNow,
                    queue,
                    eta: eta,
                    expires: signature.Expires,
                    maxRetries: signature.MaxRetries,
                    priority: signature.Priority,
                    headers: signature.Headers,
                    batchId: batchId
                ) with
                {
                    RootId = canvasId,
                    ParentId = parentId,
                    Chain = chain is { Count: > 0 } ? chain : null,
                    Link = signature.Link,
                    LinkError = signature.LinkError,
                },
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    // A signature travels as bytes: the typed input becomes Args, and computed properties are
    // left behind. Linked callbacks are carried with it, materialized the same way, so a linked
    // signature whose input is typed still arrives with its Args. The visited set cuts a link
    // that refers back to one of its own callbacks instead of recursing forever.
    private Signature ToMessageSignature(Signature signature) =>
        Materialize(signature, new HashSet<Signature>(ReferenceEqualityComparer.Instance));

    private Signature Materialize(Signature signature, HashSet<Signature> visited)
    {
        visited.Add(signature);

        return new()
        {
            TaskName = signature.TaskName,
            Args =
                signature.Args
                ?? (signature.GetInput() is { } input ? _serializer.Serialize(input) : null),
            Queue = string.IsNullOrWhiteSpace(signature.Queue)
                ? _options.DefaultQueue
                : signature.Queue,
            Priority = signature.Priority,
            MaxRetries = signature.MaxRetries,
            Countdown = signature.Countdown,
            Eta = signature.Eta,
            Expires = signature.Expires,
            Headers = signature.Headers,
            StoreResult = signature.StoreResult,
            Link =
                signature.Link is { } link && visited.Add(link) ? Materialize(link, visited) : null,
            LinkError =
                signature.LinkError is { } error && visited.Add(error)
                    ? Materialize(error, visited)
                    : null,
        };
    }

    // Builds what a group's members publish, so their task ids are known before anything is
    // sent and a chord can record them first. A signature is one task; a chain is its first
    // task with the remaining steps carried on its message, and completes with its last step;
    // a nested group flattens into its members. A chord cannot be a member of a group.
    private List<MemberPublish> BuildMemberPublishes(Group group)
    {
        var publishes = new List<MemberPublish>();

        foreach (var member in group.Members)
        {
            switch (member.Type)
            {
                case CanvasType.Signature:
                {
                    var taskId = Guid.NewGuid().ToString("N");
                    publishes.Add(
                        new MemberPublish(
                            ToMessageSignature((Signature)member),
                            taskId,
                            Chain: null,
                            CompletionId: taskId
                        )
                    );
                    break;
                }

                case CanvasType.Chain:
                {
                    var steps = BuildChainSteps(member.GetSignatures().ToList());
                    publishes.Add(
                        new MemberPublish(
                            steps[0].Signature,
                            steps[0].TaskId,
                            Chain: [.. steps.Skip(1)],
                            CompletionId: steps[^1].TaskId
                        )
                    );
                    break;
                }

                case CanvasType.Group:
                    publishes.AddRange(BuildMemberPublishes((Group)member));
                    break;

                default:
                    throw new NotSupportedException(
                        "A chord cannot be a member of a group; send the chord separately."
                    );
            }
        }

        return publishes;
    }

    private async Task PublishMembersAsync(
        IReadOnlyList<MemberPublish> publishes,
        string canvasId,
        string? batchId,
        CancellationToken cancellationToken
    )
    {
        foreach (var publish in publishes)
        {
            await PublishSignatureAsync(
                    publish.Signature,
                    publish.TaskId,
                    canvasId,
                    parentId: null,
                    publish.Chain is { Count: > 0 } ? publish.Chain : null,
                    cancellationToken,
                    batchId
                )
                .ConfigureAwait(false);
        }
    }

    // A chain travels as its first task with the remaining steps on its message, so the worker
    // that runs a step publishes the next one
    private List<ChainStep> BuildChainSteps(IReadOnlyList<Signature> signatures) =>
        [
            .. signatures.Select(signature => new ChainStep
            {
                TaskId = Guid.NewGuid().ToString("N"),
                Signature = ToMessageSignature(signature),
            }),
        ];

    // What a group member publishes, and the task it completes with
    private sealed record MemberPublish(
        Signature Signature,
        string TaskId,
        IReadOnlyList<ChainStep>? Chain,
        string CompletionId
    );
}
