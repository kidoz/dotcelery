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
        var steps = chain
            .Tasks.Select(signature => new ChainStep
            {
                TaskId = Guid.NewGuid().ToString("N"),
                Signature = ToMessageSignature(signature),
            })
            .ToList();

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
        var taskIds = new List<string>();

        foreach (var signature in RequireSignatures(group))
        {
            var taskId = Guid.NewGuid().ToString("N");
            taskIds.Add(taskId);

            await PublishSignatureAsync(
                    signature,
                    taskId,
                    canvasId,
                    parentId: null,
                    chain: null,
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

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
        var signatures = RequireSignatures(chord.Header);
        var taskIds = signatures.Select(_ => Guid.NewGuid().ToString("N")).ToList();

        // The record goes first, so a header task that finishes immediately is still counted
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

        for (var index = 0; index < signatures.Count; index++)
        {
            await PublishSignatureAsync(
                    signatures[index],
                    taskIds[index],
                    canvasId,
                    parentId: null,
                    chain: null,
                    cancellationToken,
                    batchId: canvasId
                )
                .ConfigureAwait(false);
        }

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

        await _broker
            .PublishAsync(
                new TaskMessage
                {
                    Id = taskId,
                    Task = signature.TaskName,
                    Args = signature.Args ?? [],
                    ContentType = _serializer.ContentType,
                    Timestamp = DateTimeOffset.UtcNow,
                    Eta = eta,
                    Expires = signature.Expires,
                    MaxRetries = signature.MaxRetries,
                    Priority = signature.Priority,
                    Queue = string.IsNullOrWhiteSpace(signature.Queue)
                        ? _options.DefaultQueue
                        : signature.Queue,
                    Headers = signature.Headers,
                    RootId = canvasId,
                    ParentId = parentId,
                    Chain = chain is { Count: > 0 } ? chain : null,
                    BatchId = batchId,
                },
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    // A signature travels as bytes: the typed input becomes Args, and computed properties are
    // left behind
    private Signature ToMessageSignature(Signature signature) =>
        new()
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
            Link = signature.Link,
            LinkError = signature.LinkError,
        };

    private List<Signature> RequireSignatures(Group group)
    {
        if (group.Members.Any(member => member.Type != CanvasType.Signature))
        {
            throw new NotSupportedException(
                "A group of chains, groups, or chords is not supported; send its members "
                    + "separately or as a chord."
            );
        }

        // Members travel as bytes too, so a typed input is serialized before publishing
        return [.. group.GetSignatures().Select(ToMessageSignature)];
    }
}
