using System.Runtime.CompilerServices;
using System.Text.Json;
using DotCelery.Core.Canvas;
using DotCelery.Core.Sagas;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.Sagas;

/// <summary>
/// MongoDB implementation of <see cref="ISagaStore"/>.
/// </summary>
public sealed class MongoSagaStore : ISagaStore
{
    private readonly MongoSagaStoreOptions _options;
    private readonly ILogger<MongoSagaStore> _logger;
    private readonly MongoClient _client;
    private readonly IMongoDatabase _database;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    private IMongoCollection<SagaDocument>? _collection;
    private bool _initialized;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoSagaStore"/> class.
    /// </summary>
    public MongoSagaStore(IOptions<MongoSagaStoreOptions> options, ILogger<MongoSagaStore> logger)
    {
        _options = options.Value;
        _logger = logger;
        _client = new MongoClient(_options.ConnectionString);
        _database = _client.GetDatabase(_options.DatabaseName);
    }

    /// <inheritdoc />
    public async ValueTask CreateAsync(Saga saga, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(saga);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var document = SagaDocument.FromSaga(saga);

        await _collection!
            .InsertOneAsync(document, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        _logger.LogDebug("Created saga {SagaId} with {StepCount} steps", saga.Id, saga.Steps.Count);
    }

    /// <inheritdoc />
    public async ValueTask<Saga?> GetAsync(
        string sagaId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(sagaId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<SagaDocument>.Filter.Eq(d => d.Id, sagaId);
        var document = await _collection!
            .Find(filter)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);

        return document?.ToSaga();
    }

    /// <inheritdoc />
    public async ValueTask<Saga?> UpdateStateAsync(
        string sagaId,
        SagaState state,
        string? failureReason = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(sagaId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<SagaDocument>.Filter.Eq(d => d.Id, sagaId);
        var updateDef = Builders<SagaDocument>
            .Update.Set(d => d.State, state.ToString())
            .Set(d => d.FailureReason, failureReason);

        if (state == SagaState.Executing)
        {
            updateDef = updateDef.Set(d => d.StartedAt, DateTime.UtcNow);
        }

        if (
            state
            is SagaState.Completed
                or SagaState.Failed
                or SagaState.Compensated
                or SagaState.CompensationFailed
                or SagaState.Cancelled
        )
        {
            updateDef = updateDef.Set(d => d.CompletedAt, DateTime.UtcNow);
        }

        var options = new FindOneAndUpdateOptions<SagaDocument>
        {
            ReturnDocument = ReturnDocument.After,
        };

        var document = await _collection!
            .FindOneAndUpdateAsync(filter, updateDef, options, cancellationToken)
            .ConfigureAwait(false);

        return document?.ToSaga();
    }

    /// <inheritdoc />
    public async ValueTask<Saga?> UpdateStepStateAsync(
        string sagaId,
        string stepId,
        SagaStepState state,
        string? taskId = null,
        string? compensateTaskId = null,
        object? result = null,
        string? errorMessage = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(sagaId);
        ArgumentException.ThrowIfNullOrEmpty(stepId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        // Find the saga first to get the step index
        var saga = await GetAsync(sagaId, cancellationToken).ConfigureAwait(false);
        if (saga is null)
        {
            return null;
        }

        var stepIndex = saga.Steps.ToList().FindIndex(s => s.Id == stepId);
        if (stepIndex < 0)
        {
            return null;
        }

        var filter = Builders<SagaDocument>.Filter.Eq(d => d.Id, sagaId);
        var updates = new List<UpdateDefinition<SagaDocument>>
        {
            Builders<SagaDocument>.Update.Set($"steps.{stepIndex}.state", state.ToString()),
        };

        if (taskId is not null)
        {
            updates.Add(
                Builders<SagaDocument>.Update.Set($"steps.{stepIndex}.execute_task_id", taskId)
            );
        }

        if (compensateTaskId is not null)
        {
            updates.Add(
                Builders<SagaDocument>.Update.Set(
                    $"steps.{stepIndex}.compensate_task_id",
                    compensateTaskId
                )
            );
        }

        if (result is not null)
        {
            var resultJson = JsonSerializer.Serialize(result, DotCeleryJsonContext.Default.Options);
            updates.Add(Builders<SagaDocument>.Update.Set($"steps.{stepIndex}.result", resultJson));
        }

        if (errorMessage is not null)
        {
            updates.Add(
                Builders<SagaDocument>.Update.Set($"steps.{stepIndex}.error", errorMessage)
            );
        }

        if (state == SagaStepState.Executing)
        {
            updates.Add(
                Builders<SagaDocument>.Update.Set($"steps.{stepIndex}.started_at", DateTime.UtcNow)
            );
        }

        if (
            state
            is SagaStepState.Completed
                or SagaStepState.Failed
                or SagaStepState.Compensated
                or SagaStepState.CompensationFailed
                or SagaStepState.Skipped
        )
        {
            updates.Add(
                Builders<SagaDocument>.Update.Set(
                    $"steps.{stepIndex}.completed_at",
                    DateTime.UtcNow
                )
            );
        }

        var combinedUpdate = Builders<SagaDocument>.Update.Combine(updates);
        var options = new FindOneAndUpdateOptions<SagaDocument>
        {
            ReturnDocument = ReturnDocument.After,
        };

        var document = await _collection!
            .FindOneAndUpdateAsync(filter, combinedUpdate, options, cancellationToken)
            .ConfigureAwait(false);

        return document?.ToSaga();
    }

    /// <inheritdoc />
    public async ValueTask<Saga?> MarkStepCompensatedAsync(
        string sagaId,
        string stepId,
        bool success,
        string? compensateTaskId = null,
        string? errorMessage = null,
        CancellationToken cancellationToken = default
    )
    {
        var state = success ? SagaStepState.Compensated : SagaStepState.CompensationFailed;
        return await UpdateStepStateAsync(
                sagaId,
                stepId,
                state,
                compensateTaskId: compensateTaskId,
                errorMessage: errorMessage,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<Saga?> AdvanceStepAsync(
        string sagaId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(sagaId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<SagaDocument>.Filter.Eq(d => d.Id, sagaId);
        var update = Builders<SagaDocument>.Update.Inc(d => d.CurrentStepIndex, 1);
        var options = new FindOneAndUpdateOptions<SagaDocument>
        {
            ReturnDocument = ReturnDocument.After,
        };

        var document = await _collection!
            .FindOneAndUpdateAsync(filter, update, options, cancellationToken)
            .ConfigureAwait(false);

        return document?.ToSaga();
    }

    /// <inheritdoc />
    public async ValueTask<bool> DeleteAsync(
        string sagaId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(sagaId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<SagaDocument>.Filter.Eq(d => d.Id, sagaId);
        var result = await _collection!
            .DeleteOneAsync(filter, cancellationToken)
            .ConfigureAwait(false);

        return result.DeletedCount > 0;
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetSagaIdForTaskAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        // Search for saga where any step has this taskId as execute_task_id or compensate_task_id
        var filter = Builders<SagaDocument>.Filter.Or(
            Builders<SagaDocument>.Filter.ElemMatch(d => d.Steps, s => s.ExecuteTaskId == taskId),
            Builders<SagaDocument>.Filter.ElemMatch(d => d.Steps, s => s.CompensateTaskId == taskId)
        );

        var projection = Builders<SagaDocument>.Projection.Include(d => d.Id);
        var document = await _collection!
            .Find(filter)
            .Project<SagaDocument>(projection)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);

        return document?.Id;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<Saga> GetByStateAsync(
        SagaState state,
        int limit = 100,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<SagaDocument>.Filter.Eq(d => d.State, state.ToString());
        var sort = Builders<SagaDocument>.Sort.Descending(d => d.CreatedAt);

        using var cursor = await _collection!
            .Find(filter)
            .Sort(sort)
            .Limit(limit)
            .ToCursorAsync(cancellationToken)
            .ConfigureAwait(false);

        while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
        {
            foreach (var document in cursor.Current)
            {
                yield return document.ToSaga();
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await ValueTask.CompletedTask;

        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _initLock.Dispose();
        _logger.LogInformation("MongoDB saga store disposed");
    }

    private async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_initialized)
        {
            return;
        }

        await _initLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_initialized)
            {
                return;
            }

            _collection = _database.GetCollection<SagaDocument>(_options.CollectionName);

            if (_options.AutoCreateIndexes)
            {
                await CreateIndexesAsync(cancellationToken).ConfigureAwait(false);
            }

            _initialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }

    private async Task CreateIndexesAsync(CancellationToken cancellationToken)
    {
        var indexes = new List<CreateIndexModel<SagaDocument>>
        {
            new(
                Builders<SagaDocument>.IndexKeys.Ascending(d => d.State),
                new CreateIndexOptions { Name = "idx_state" }
            ),
            new(
                Builders<SagaDocument>.IndexKeys.Ascending(d => d.CreatedAt),
                new CreateIndexOptions { Name = "idx_created_at" }
            ),
            new(
                Builders<SagaDocument>.IndexKeys.Ascending(d => d.CorrelationId),
                new CreateIndexOptions { Name = "idx_correlation_id", Sparse = true }
            ),
            new(
                Builders<SagaDocument>.IndexKeys.Ascending("steps.execute_task_id"),
                new CreateIndexOptions { Name = "idx_step_execute_task_id", Sparse = true }
            ),
            new(
                Builders<SagaDocument>.IndexKeys.Ascending("steps.compensate_task_id"),
                new CreateIndexOptions { Name = "idx_step_compensate_task_id", Sparse = true }
            ),
        };

        await _collection!
            .Indexes.CreateManyAsync(indexes, cancellationToken)
            .ConfigureAwait(false);
    }
}

internal sealed class SagaDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.String)]
    public string Id { get; set; } = string.Empty;

    [BsonElement("name")]
    public string Name { get; set; } = string.Empty;

    [BsonElement("state")]
    public string State { get; set; } = string.Empty;

    [BsonElement("steps")]
    public List<SagaStepDocument> Steps { get; set; } = [];

    [BsonElement("created_at")]
    public DateTime CreatedAt { get; set; }

    [BsonElement("started_at")]
    [BsonIgnoreIfNull]
    public DateTime? StartedAt { get; set; }

    [BsonElement("completed_at")]
    [BsonIgnoreIfNull]
    public DateTime? CompletedAt { get; set; }

    [BsonElement("correlation_id")]
    [BsonIgnoreIfNull]
    public string? CorrelationId { get; set; }

    [BsonElement("metadata")]
    [BsonIgnoreIfNull]
    public string? MetadataJson { get; set; }

    [BsonElement("current_step_index")]
    public int CurrentStepIndex { get; set; }

    [BsonElement("failure_reason")]
    [BsonIgnoreIfNull]
    public string? FailureReason { get; set; }

    public static SagaDocument FromSaga(Saga saga)
    {
        return new SagaDocument
        {
            Id = saga.Id,
            Name = saga.Name,
            State = saga.State.ToString(),
            Steps = saga.Steps.Select(SagaStepDocument.FromStep).ToList(),
            CreatedAt = saga.CreatedAt.UtcDateTime,
            StartedAt = saga.StartedAt?.UtcDateTime,
            CompletedAt = saga.CompletedAt?.UtcDateTime,
            CorrelationId = saga.CorrelationId,
            MetadataJson = saga.Metadata is not null
                ? JsonSerializer.Serialize(saga.Metadata, DotCeleryJsonContext.Default.Options)
                : null,
            CurrentStepIndex = saga.CurrentStepIndex,
            FailureReason = saga.FailureReason,
        };
    }

    public Saga ToSaga()
    {
        var state = Enum.TryParse<SagaState>(State, out var s) ? s : SagaState.Created;

        IReadOnlyDictionary<string, object>? metadata = null;
        if (!string.IsNullOrEmpty(MetadataJson))
        {
            metadata = JsonSerializer.Deserialize<Dictionary<string, object>>(
                MetadataJson,
                DotCeleryJsonContext.Default.Options
            );
        }

        return new Saga
        {
            Id = Id,
            Name = Name,
            State = state,
            Steps = Steps.Select(s => s.ToStep()).ToList(),
            CreatedAt = new DateTimeOffset(CreatedAt, TimeSpan.Zero),
            StartedAt = StartedAt.HasValue
                ? new DateTimeOffset(StartedAt.Value, TimeSpan.Zero)
                : null,
            CompletedAt = CompletedAt.HasValue
                ? new DateTimeOffset(CompletedAt.Value, TimeSpan.Zero)
                : null,
            CorrelationId = CorrelationId,
            Metadata = metadata,
            CurrentStepIndex = CurrentStepIndex,
            FailureReason = FailureReason,
        };
    }
}

internal sealed class SagaStepDocument
{
    [BsonElement("id")]
    public string Id { get; set; } = string.Empty;

    [BsonElement("name")]
    public string Name { get; set; } = string.Empty;

    [BsonElement("order")]
    public int Order { get; set; }

    [BsonElement("execute_task")]
    public string ExecuteTaskJson { get; set; } = string.Empty;

    [BsonElement("compensate_task")]
    [BsonIgnoreIfNull]
    public string? CompensateTaskJson { get; set; }

    [BsonElement("state")]
    public string State { get; set; } = string.Empty;

    [BsonElement("execute_task_id")]
    [BsonIgnoreIfNull]
    public string? ExecuteTaskId { get; set; }

    [BsonElement("compensate_task_id")]
    [BsonIgnoreIfNull]
    public string? CompensateTaskId { get; set; }

    [BsonElement("result")]
    [BsonIgnoreIfNull]
    public string? ResultJson { get; set; }

    [BsonElement("error")]
    [BsonIgnoreIfNull]
    public string? Error { get; set; }

    [BsonElement("started_at")]
    [BsonIgnoreIfNull]
    public DateTime? StartedAt { get; set; }

    [BsonElement("completed_at")]
    [BsonIgnoreIfNull]
    public DateTime? CompletedAt { get; set; }

    public static SagaStepDocument FromStep(SagaStep step)
    {
        return new SagaStepDocument
        {
            Id = step.Id,
            Name = step.Name,
            Order = step.Order,
            ExecuteTaskJson = JsonSerializer.Serialize(
                step.ExecuteTask,
                DotCeleryJsonContext.Default.Signature
            ),
            CompensateTaskJson = step.CompensateTask is not null
                ? JsonSerializer.Serialize(
                    step.CompensateTask,
                    DotCeleryJsonContext.Default.Signature
                )
                : null,
            State = step.State.ToString(),
            ExecuteTaskId = step.ExecuteTaskId,
            CompensateTaskId = step.CompensateTaskId,
            ResultJson = step.Result is not null
                ? JsonSerializer.Serialize(step.Result, DotCeleryJsonContext.Default.Options)
                : null,
            Error = step.Error,
            StartedAt = step.StartedAt?.UtcDateTime,
            CompletedAt = step.CompletedAt?.UtcDateTime,
        };
    }

    public SagaStep ToStep()
    {
        var state = Enum.TryParse<SagaStepState>(State, out var s) ? s : SagaStepState.Pending;
        var executeTask = JsonSerializer.Deserialize(
            ExecuteTaskJson,
            DotCeleryJsonContext.Default.Signature
        )!;
        Signature? compensateTask = null;
        if (!string.IsNullOrEmpty(CompensateTaskJson))
        {
            compensateTask = JsonSerializer.Deserialize(
                CompensateTaskJson,
                DotCeleryJsonContext.Default.Signature
            );
        }

        object? result = null;
        if (!string.IsNullOrEmpty(ResultJson))
        {
            result = JsonSerializer.Deserialize<object>(
                ResultJson,
                DotCeleryJsonContext.Default.Options
            );
        }

        return new SagaStep
        {
            Id = Id,
            Name = Name,
            Order = Order,
            ExecuteTask = executeTask,
            CompensateTask = compensateTask,
            State = state,
            ExecuteTaskId = ExecuteTaskId,
            CompensateTaskId = CompensateTaskId,
            Result = result,
            Error = Error,
            StartedAt = StartedAt.HasValue
                ? new DateTimeOffset(StartedAt.Value, TimeSpan.Zero)
                : null,
            CompletedAt = CompletedAt.HasValue
                ? new DateTimeOffset(CompletedAt.Value, TimeSpan.Zero)
                : null,
        };
    }
}
