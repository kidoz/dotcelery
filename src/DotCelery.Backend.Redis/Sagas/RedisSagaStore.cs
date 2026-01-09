using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Backend.Redis.Serialization;
using DotCelery.Core.Resilience;
using DotCelery.Core.Sagas;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.Sagas;

/// <summary>
/// Redis implementation of <see cref="ISagaStore"/>.
/// Uses Redis hashes for saga data and sorted sets for state indexing.
/// Uses Lua scripts for atomic read-modify-write operations to prevent race conditions.
/// </summary>
public sealed class RedisSagaStore : ISagaStore
{
    private readonly RedisSagaStoreOptions _options;
    private readonly ILogger<RedisSagaStore> _logger;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly RetryPolicy _retryPolicy;

    // AOT-friendly type info
    private static JsonTypeInfo<Saga> SagaTypeInfo => RedisBackendJsonContext.Default.Saga;

    private ConnectionMultiplexer? _connection;
    private bool _disposed;

    // Lua script for atomic state update with index management
    // KEYS[1] = saga key, KEYS[2] = old state index key, KEYS[3] = new state index key, KEYS[4] = task-to-saga hash key
    // ARGV[1] = saga id, ARGV[2] = new state, ARGV[3] = failure reason (or empty), ARGV[4] = created at score, ARGV[5] = ttl ms (or 0), ARGV[6] = now timestamp
    // Returns: updated saga JSON or nil if not found
    private const string UpdateStateScript = """
        local sagaKey = KEYS[1]
        local oldStateIndexKey = KEYS[2]
        local newStateIndexKey = KEYS[3]

        local sagaId = ARGV[1]
        local newState = ARGV[2]
        local failureReason = ARGV[3]
        local createdAtScore = tonumber(ARGV[4])
        local ttlMs = tonumber(ARGV[5])
        local nowTimestamp = ARGV[6]

        -- Get current saga
        local json = redis.call('GET', sagaKey)
        if not json then
            return nil
        end

        local saga = cjson.decode(json)
        local oldState = saga.State

        -- Update saga fields
        saga.State = newState
        if failureReason ~= '' then
            saga.FailureReason = failureReason
        end

        -- Set CompletedAt for terminal states
        local terminalStates = {Completed=true, Failed=true, Compensated=true, CompensationFailed=true, Cancelled=true}
        if terminalStates[newState] then
            saga.CompletedAt = nowTimestamp
        end

        local updatedJson = cjson.encode(saga)

        -- Update state indexes atomically
        if oldState ~= newState then
            redis.call('ZREM', oldStateIndexKey, sagaId)
            redis.call('ZADD', newStateIndexKey, createdAtScore, sagaId)
        end

        -- Save with TTL if terminal, otherwise without
        if ttlMs > 0 and terminalStates[newState] then
            redis.call('SET', sagaKey, updatedJson, 'PX', ttlMs)
        else
            redis.call('SET', sagaKey, updatedJson)
        end

        return updatedJson
        """;

    // Lua script for atomic step state update
    // KEYS[1] = saga key, KEYS[2] = old state index key, KEYS[3] = task-to-saga hash key
    // ARGV[1] = saga id, ARGV[2] = step id, ARGV[3] = new step state, ARGV[4] = task id (or empty)
    // ARGV[5] = compensate task id (or empty), ARGV[6] = result json (or empty), ARGV[7] = error message (or empty)
    // ARGV[8] = now timestamp, ARGV[9] = state index key prefix, ARGV[10] = ttl ms (or 0)
    // Returns: updated saga JSON or nil if not found
    private const string UpdateStepStateScript = """
        local sagaKey = KEYS[1]
        local taskToSagaKey = KEYS[2]

        local sagaId = ARGV[1]
        local stepId = ARGV[2]
        local newStepState = ARGV[3]
        local taskId = ARGV[4]
        local compensateTaskId = ARGV[5]
        local resultJson = ARGV[6]
        local errorMessage = ARGV[7]
        local nowTimestamp = ARGV[8]
        local stateIndexPrefix = ARGV[9]
        local ttlMs = tonumber(ARGV[10])
        local createdAtScore = tonumber(ARGV[11])

        -- Get current saga
        local json = redis.call('GET', sagaKey)
        if not json then
            return nil
        end

        local saga = cjson.decode(json)
        local oldSagaState = saga.State

        -- Find and update the step
        local stepFound = false
        for i, step in ipairs(saga.Steps) do
            if step.Id == stepId then
                stepFound = true
                step.State = newStepState
                if taskId ~= '' then
                    step.ExecuteTaskId = taskId
                end
                if compensateTaskId ~= '' then
                    step.CompensateTaskId = compensateTaskId
                end
                if resultJson ~= '' then
                    step.Result = cjson.decode(resultJson)
                end
                if errorMessage ~= '' then
                    step.Error = errorMessage
                end
                if newStepState == 'Executing' and not step.StartedAt then
                    step.StartedAt = nowTimestamp
                end
                if newStepState == 'Completed' or newStepState == 'Failed' then
                    step.CompletedAt = nowTimestamp
                end
                break
            end
        end

        if not stepFound then
            return nil
        end

        -- Index new task IDs
        if taskId ~= '' then
            redis.call('HSET', taskToSagaKey, taskId, sagaId)
        end
        if compensateTaskId ~= '' then
            redis.call('HSET', taskToSagaKey, compensateTaskId, sagaId)
        end

        -- Auto-transition saga state on step failure
        if newStepState == 'Failed' then
            local hasCompensableSteps = false
            for i = 1, saga.CurrentStepIndex + 1 do
                local step = saga.Steps[i]
                if step and step.State == 'Completed' and step.CompensateTask then
                    hasCompensableSteps = true
                    break
                end
            end
            if hasCompensableSteps then
                saga.State = 'Compensating'
            else
                saga.State = 'Failed'
            end
            if errorMessage ~= '' then
                saga.FailureReason = errorMessage
            end
        end

        local updatedJson = cjson.encode(saga)

        -- Update state index if saga state changed
        if oldSagaState ~= saga.State then
            local oldIndexKey = stateIndexPrefix .. oldSagaState
            local newIndexKey = stateIndexPrefix .. saga.State
            redis.call('ZREM', oldIndexKey, sagaId)
            redis.call('ZADD', newIndexKey, createdAtScore, sagaId)
        end

        -- Save saga
        local terminalStates = {Completed=true, Failed=true, Compensated=true, CompensationFailed=true, Cancelled=true}
        if ttlMs > 0 and terminalStates[saga.State] then
            redis.call('SET', sagaKey, updatedJson, 'PX', ttlMs)
        else
            redis.call('SET', sagaKey, updatedJson)
        end

        return updatedJson
        """;

    // Lua script for atomic advance step
    // KEYS[1] = saga key, KEYS[2] = task-to-saga hash key
    // ARGV[1] = saga id, ARGV[2] = state index prefix, ARGV[3] = ttl ms, ARGV[4] = created at score, ARGV[5] = now timestamp
    // Returns: updated saga JSON or nil if not found
    private const string AdvanceStepScript = """
        local sagaKey = KEYS[1]

        local sagaId = ARGV[1]
        local stateIndexPrefix = ARGV[2]
        local ttlMs = tonumber(ARGV[3])
        local createdAtScore = tonumber(ARGV[4])
        local nowTimestamp = ARGV[5]

        -- Get current saga
        local json = redis.call('GET', sagaKey)
        if not json then
            return nil
        end

        local saga = cjson.decode(json)
        local oldState = saga.State

        -- Advance step index
        saga.CurrentStepIndex = saga.CurrentStepIndex + 1

        -- Check if saga is complete
        if saga.CurrentStepIndex >= #saga.Steps then
            saga.State = 'Completed'
            saga.CompletedAt = nowTimestamp
        end

        local updatedJson = cjson.encode(saga)

        -- Update state index if changed
        if oldState ~= saga.State then
            local oldIndexKey = stateIndexPrefix .. oldState
            local newIndexKey = stateIndexPrefix .. saga.State
            redis.call('ZREM', oldIndexKey, sagaId)
            redis.call('ZADD', newIndexKey, createdAtScore, sagaId)
        end

        -- Save with TTL if terminal
        local terminalStates = {Completed=true, Failed=true, Compensated=true, CompensationFailed=true, Cancelled=true}
        if ttlMs > 0 and terminalStates[saga.State] then
            redis.call('SET', sagaKey, updatedJson, 'PX', ttlMs)
        else
            redis.call('SET', sagaKey, updatedJson)
        end

        return updatedJson
        """;

    // Lua script for atomic mark step compensated
    // KEYS[1] = saga key, KEYS[2] = task-to-saga hash key
    // ARGV[1] = saga id, ARGV[2] = step id, ARGV[3] = success (1/0), ARGV[4] = compensate task id (or empty)
    // ARGV[5] = error message (or empty), ARGV[6] = state index prefix, ARGV[7] = ttl ms, ARGV[8] = created at score, ARGV[9] = now timestamp
    // Returns: updated saga JSON or nil if not found
    private const string MarkStepCompensatedScript = """
        local sagaKey = KEYS[1]
        local taskToSagaKey = KEYS[2]

        local sagaId = ARGV[1]
        local stepId = ARGV[2]
        local success = ARGV[3] == '1'
        local compensateTaskId = ARGV[4]
        local errorMessage = ARGV[5]
        local stateIndexPrefix = ARGV[6]
        local ttlMs = tonumber(ARGV[7])
        local createdAtScore = tonumber(ARGV[8])
        local nowTimestamp = ARGV[9]

        -- Get current saga
        local json = redis.call('GET', sagaKey)
        if not json then
            return nil
        end

        local saga = cjson.decode(json)
        local oldState = saga.State

        -- Find and update the step
        local stepFound = false
        for i, step in ipairs(saga.Steps) do
            if step.Id == stepId then
                stepFound = true
                step.State = success and 'Compensated' or 'CompensationFailed'
                if compensateTaskId ~= '' then
                    step.CompensateTaskId = compensateTaskId
                end
                if errorMessage ~= '' then
                    step.Error = errorMessage
                end
                break
            end
        end

        if not stepFound then
            return nil
        end

        -- Index compensation task ID
        if compensateTaskId ~= '' then
            redis.call('HSET', taskToSagaKey, compensateTaskId, sagaId)
        end

        -- Check if all compensation is done
        local stepsNeedingCompensation = 0
        local anyCompensationFailed = false
        for i, step in ipairs(saga.Steps) do
            if step.CompensateTask and (step.State == 'Completed' or step.State == 'Compensating') then
                stepsNeedingCompensation = stepsNeedingCompensation + 1
            end
            if step.State == 'CompensationFailed' then
                anyCompensationFailed = true
            end
        end

        if stepsNeedingCompensation == 0 then
            saga.State = anyCompensationFailed and 'CompensationFailed' or 'Compensated'
            saga.CompletedAt = nowTimestamp
        end

        local updatedJson = cjson.encode(saga)

        -- Update state index if changed
        if oldState ~= saga.State then
            local oldIndexKey = stateIndexPrefix .. oldState
            local newIndexKey = stateIndexPrefix .. saga.State
            redis.call('ZREM', oldIndexKey, sagaId)
            redis.call('ZADD', newIndexKey, createdAtScore, sagaId)
        end

        -- Save with TTL if terminal
        local terminalStates = {Completed=true, Failed=true, Compensated=true, CompensationFailed=true, Cancelled=true}
        if ttlMs > 0 and terminalStates[saga.State] then
            redis.call('SET', sagaKey, updatedJson, 'PX', ttlMs)
        else
            redis.call('SET', sagaKey, updatedJson)
        end

        return updatedJson
        """;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisSagaStore"/> class.
    /// </summary>
    public RedisSagaStore(IOptions<RedisSagaStoreOptions> options, ILogger<RedisSagaStore> logger)
    {
        _options = options.Value;
        _logger = logger;
        _retryPolicy = _options.Resilience.CreatePolicy(IsTransientRedisException);
    }

    private static bool IsTransientRedisException(Exception ex)
    {
        // Redis-specific transient exceptions
        if (ex is RedisConnectionException or RedisTimeoutException)
        {
            return true;
        }

        if (ex is RedisServerException serverEx)
        {
            var msg = serverEx.Message;
            return msg.Contains("BUSY", StringComparison.OrdinalIgnoreCase)
                || msg.Contains("LOADING", StringComparison.OrdinalIgnoreCase);
        }

        return false;
    }

    /// <inheritdoc />
    public async ValueTask CreateAsync(Saga saga, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(saga);

        await _retryPolicy
            .ExecuteAsync(
                async ct =>
                {
                    var db = await GetDatabaseAsync(ct).ConfigureAwait(false);
                    var key = GetSagaKey(saga.Id);
                    var json = JsonSerializer.Serialize(saga, SagaTypeInfo);

                    // Use transaction for atomic creation with index updates
                    var transaction = db.CreateTransaction();

#pragma warning disable CA2012 // Use ValueTasks correctly - Redis transaction requires this pattern
                    _ = transaction.StringSetAsync(key, json);
                    _ = transaction.SortedSetAddAsync(
                        GetStateIndexKey(saga.State),
                        saga.Id,
                        saga.CreatedAt.ToUnixTimeMilliseconds()
                    );

                    // Index task IDs with optional TTL
                    foreach (var step in saga.Steps)
                    {
                        if (step.ExecuteTaskId is not null)
                        {
                            _ = transaction.HashSetAsync(
                                _options.TaskToSagaKey,
                                step.ExecuteTaskId,
                                saga.Id
                            );
                        }

                        if (step.CompensateTaskId is not null)
                        {
                            _ = transaction.HashSetAsync(
                                _options.TaskToSagaKey,
                                step.CompensateTaskId,
                                saga.Id
                            );
                        }
                    }
#pragma warning restore CA2012

                    await transaction.ExecuteAsync().ConfigureAwait(false);
                },
                cancellationToken
            )
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

        return await _retryPolicy
            .ExecuteAsync(
                async ct =>
                {
                    var db = await GetDatabaseAsync(ct).ConfigureAwait(false);
                    var json = await db.StringGetAsync(GetSagaKey(sagaId)).ConfigureAwait(false);

                    if (json.IsNullOrEmpty)
                    {
                        return null;
                    }

                    return JsonSerializer.Deserialize((string)json!, SagaTypeInfo);
                },
                cancellationToken
            )
            .ConfigureAwait(false);
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

        // First get the saga to determine old state and created time (has its own retry)
        var existingSaga = await GetAsync(sagaId, cancellationToken).ConfigureAwait(false);
        if (existingSaga is null)
        {
            return null;
        }

        var now = DateTimeOffset.UtcNow;
        var ttlMs = _options.CompletedSagaTtl.HasValue
            ? (long)_options.CompletedSagaTtl.Value.TotalMilliseconds
            : 0;

        var updatedSaga = await _retryPolicy
            .ExecuteAsync(
                async ct =>
                {
                    var db = await GetDatabaseAsync(ct).ConfigureAwait(false);

                    var result = await db.ScriptEvaluateAsync(
                            UpdateStateScript,
                            [
                                GetSagaKey(sagaId),
                                GetStateIndexKey(existingSaga.State),
                                GetStateIndexKey(state),
                            ],
                            [
                                sagaId,
                                state.ToString(),
                                failureReason ?? string.Empty,
                                existingSaga.CreatedAt.ToUnixTimeMilliseconds(),
                                ttlMs,
                                now.ToString("O"),
                            ]
                        )
                        .ConfigureAwait(false);

                    if (result.IsNull)
                    {
                        return null;
                    }

                    var updatedJson = (string)result!;
                    return JsonSerializer.Deserialize(updatedJson, SagaTypeInfo);
                },
                cancellationToken
            )
            .ConfigureAwait(false);

        if (updatedSaga is not null)
        {
            _logger.LogDebug("Updated saga {SagaId} state to {State}", sagaId, state);
        }

        return updatedSaga;
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

        // Get saga to determine created time for index score (has its own retry)
        var existingSaga = await GetAsync(sagaId, cancellationToken).ConfigureAwait(false);
        if (existingSaga is null)
        {
            return null;
        }

        var now = DateTimeOffset.UtcNow;
        var ttlMs = _options.CompletedSagaTtl.HasValue
            ? (long)_options.CompletedSagaTtl.Value.TotalMilliseconds
            : 0;

        var resultJson = result is not null
            ? JsonSerializer.Serialize(result, JsonMessageSerializer.CreateDefaultOptions())
            : string.Empty;

        var updatedSaga = await _retryPolicy
            .ExecuteAsync(
                async ct =>
                {
                    var db = await GetDatabaseAsync(ct).ConfigureAwait(false);

                    var scriptResult = await db.ScriptEvaluateAsync(
                            UpdateStepStateScript,
                            [GetSagaKey(sagaId), _options.TaskToSagaKey],
                            [
                                sagaId,
                                stepId,
                                state.ToString(),
                                taskId ?? string.Empty,
                                compensateTaskId ?? string.Empty,
                                resultJson,
                                errorMessage ?? string.Empty,
                                now.ToString("O"),
                                _options.StateIndexKeyPrefix,
                                ttlMs,
                                existingSaga.CreatedAt.ToUnixTimeMilliseconds(),
                            ]
                        )
                        .ConfigureAwait(false);

                    if (scriptResult.IsNull)
                    {
                        return null;
                    }

                    var updatedJson = (string)scriptResult!;
                    return JsonSerializer.Deserialize(updatedJson, SagaTypeInfo);
                },
                cancellationToken
            )
            .ConfigureAwait(false);

        if (updatedSaga is not null)
        {
            _logger.LogDebug(
                "Updated saga {SagaId} step {StepId} state to {State}",
                sagaId,
                stepId,
                state
            );
        }

        return updatedSaga;
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
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(sagaId);
        ArgumentException.ThrowIfNullOrEmpty(stepId);

        // Get saga to determine created time for index score (has its own retry)
        var existingSaga = await GetAsync(sagaId, cancellationToken).ConfigureAwait(false);
        if (existingSaga is null)
        {
            return null;
        }

        var now = DateTimeOffset.UtcNow;
        var ttlMs = _options.CompletedSagaTtl.HasValue
            ? (long)_options.CompletedSagaTtl.Value.TotalMilliseconds
            : 0;

        var updatedSaga = await _retryPolicy
            .ExecuteAsync(
                async ct =>
                {
                    var db = await GetDatabaseAsync(ct).ConfigureAwait(false);

                    var scriptResult = await db.ScriptEvaluateAsync(
                            MarkStepCompensatedScript,
                            [GetSagaKey(sagaId), _options.TaskToSagaKey],
                            [
                                sagaId,
                                stepId,
                                success ? "1" : "0",
                                compensateTaskId ?? string.Empty,
                                errorMessage ?? string.Empty,
                                _options.StateIndexKeyPrefix,
                                ttlMs,
                                existingSaga.CreatedAt.ToUnixTimeMilliseconds(),
                                now.ToString("O"),
                            ]
                        )
                        .ConfigureAwait(false);

                    if (scriptResult.IsNull)
                    {
                        return null;
                    }

                    var updatedJson = (string)scriptResult!;
                    return JsonSerializer.Deserialize(updatedJson, SagaTypeInfo);
                },
                cancellationToken
            )
            .ConfigureAwait(false);

        if (updatedSaga is not null)
        {
            _logger.LogDebug(
                "Marked saga {SagaId} step {StepId} as {Status}",
                sagaId,
                stepId,
                success ? "compensated" : "compensation failed"
            );
        }

        return updatedSaga;
    }

    /// <inheritdoc />
    public async ValueTask<Saga?> AdvanceStepAsync(
        string sagaId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(sagaId);

        // Get saga to determine created time for index score (has its own retry)
        var existingSaga = await GetAsync(sagaId, cancellationToken).ConfigureAwait(false);
        if (existingSaga is null)
        {
            return null;
        }

        var now = DateTimeOffset.UtcNow;
        var ttlMs = _options.CompletedSagaTtl.HasValue
            ? (long)_options.CompletedSagaTtl.Value.TotalMilliseconds
            : 0;

        var updatedSaga = await _retryPolicy
            .ExecuteAsync(
                async ct =>
                {
                    var db = await GetDatabaseAsync(ct).ConfigureAwait(false);

                    var scriptResult = await db.ScriptEvaluateAsync(
                            AdvanceStepScript,
                            [GetSagaKey(sagaId)],
                            [
                                sagaId,
                                _options.StateIndexKeyPrefix,
                                ttlMs,
                                existingSaga.CreatedAt.ToUnixTimeMilliseconds(),
                                now.ToString("O"),
                            ]
                        )
                        .ConfigureAwait(false);

                    if (scriptResult.IsNull)
                    {
                        return null;
                    }

                    var updatedJson = (string)scriptResult!;
                    return JsonSerializer.Deserialize(updatedJson, SagaTypeInfo);
                },
                cancellationToken
            )
            .ConfigureAwait(false);

        if (updatedSaga is not null)
        {
            _logger.LogDebug(
                "Advanced saga {SagaId} to step {StepIndex}",
                sagaId,
                updatedSaga.CurrentStepIndex
            );
        }

        return updatedSaga;
    }

    /// <inheritdoc />
    public async ValueTask<bool> DeleteAsync(
        string sagaId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(sagaId);

        // GetAsync has its own retry logic
        var saga = await GetAsync(sagaId, cancellationToken).ConfigureAwait(false);
        if (saga is null)
        {
            return false;
        }

        return await _retryPolicy
            .ExecuteAsync(
                async ct =>
                {
                    var db = await GetDatabaseAsync(ct).ConfigureAwait(false);

                    // Use transaction for atomic delete with index cleanup
                    var transaction = db.CreateTransaction();

#pragma warning disable CA2012 // Use ValueTasks correctly - Redis transaction requires this pattern
                    // Remove task mappings
                    foreach (var step in saga.Steps)
                    {
                        if (step.ExecuteTaskId is not null)
                        {
                            _ = transaction.HashDeleteAsync(
                                _options.TaskToSagaKey,
                                step.ExecuteTaskId
                            );
                        }

                        if (step.CompensateTaskId is not null)
                        {
                            _ = transaction.HashDeleteAsync(
                                _options.TaskToSagaKey,
                                step.CompensateTaskId
                            );
                        }
                    }

                    // Remove from state index
                    _ = transaction.SortedSetRemoveAsync(GetStateIndexKey(saga.State), sagaId);

                    // Delete the saga
                    var deleteTask = transaction.KeyDeleteAsync(GetSagaKey(sagaId));
#pragma warning restore CA2012

                    await transaction.ExecuteAsync().ConfigureAwait(false);

                    return await deleteTask.ConfigureAwait(false);
                },
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetSagaIdForTaskAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        return await _retryPolicy
            .ExecuteAsync(
                async ct =>
                {
                    var db = await GetDatabaseAsync(ct).ConfigureAwait(false);
                    var sagaId = await db.HashGetAsync(_options.TaskToSagaKey, taskId)
                        .ConfigureAwait(false);

                    return sagaId.IsNullOrEmpty ? null : (string)sagaId!;
                },
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<Saga> GetByStateAsync(
        SagaState state,
        int limit = 100,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var sagaIds = await db.SortedSetRangeByRankAsync(GetStateIndexKey(state), 0, limit - 1)
            .ConfigureAwait(false);

        foreach (var sagaId in sagaIds)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                yield break;
            }

            var saga = await GetAsync((string)sagaId!, cancellationToken).ConfigureAwait(false);
            if (saga is not null)
            {
                yield return saga;
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        if (_connection is not null)
        {
            await _connection.CloseAsync().ConfigureAwait(false);
            _connection.Dispose();
        }

        _connectionLock.Dispose();
        _logger.LogInformation("Redis saga store disposed");
    }

    private string GetSagaKey(string sagaId) => $"{_options.SagaKeyPrefix}{sagaId}";

    private string GetStateIndexKey(SagaState state) => $"{_options.StateIndexKeyPrefix}{state}";

    private async Task<IConnectionMultiplexer> GetConnectionAsync(
        CancellationToken cancellationToken
    )
    {
        if (_connection?.IsConnected == true)
        {
            return _connection;
        }

        await _connectionLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_connection?.IsConnected == true)
            {
                return _connection;
            }

            var configOptions = ConfigurationOptions.Parse(_options.ConnectionString);
            configOptions.DefaultDatabase = _options.Database;
            configOptions.ConnectTimeout = (int)_options.ConnectTimeout.TotalMilliseconds;
            configOptions.SyncTimeout = (int)_options.SyncTimeout.TotalMilliseconds;
            configOptions.AbortOnConnectFail = _options.AbortOnConnectFail;

            _connection = await ConnectionMultiplexer
                .ConnectAsync(configOptions)
                .ConfigureAwait(false);
            _logger.LogInformation("Connected to Redis for saga store");

            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private async Task<IDatabase> GetDatabaseAsync(CancellationToken cancellationToken)
    {
        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        return connection.GetDatabase(_options.Database);
    }
}
