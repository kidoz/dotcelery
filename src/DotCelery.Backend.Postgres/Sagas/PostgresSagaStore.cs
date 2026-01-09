using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Core.Canvas;
using DotCelery.Core.Sagas;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace DotCelery.Backend.Postgres.Sagas;

/// <summary>
/// PostgreSQL implementation of <see cref="ISagaStore"/>.
/// </summary>
public sealed class PostgresSagaStore : ISagaStore
{
    private readonly PostgresSagaStoreOptions _options;
    private readonly ILogger<PostgresSagaStore> _logger;
    private readonly NpgsqlDataSource _dataSource;
    private readonly bool _ownsDataSource;

    // AOT-friendly type info for registered types
    private static JsonTypeInfo<Signature> SignatureTypeInfo =>
        DotCeleryJsonContext.Default.Signature;

    // Fallback options for types not in AOT context (e.g., metadata Dictionary)
    private static readonly JsonSerializerOptions FallbackOptions =
        JsonMessageSerializer.CreateDefaultOptions();

    private bool _disposed;
    private bool _initialized;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresSagaStore"/> class.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="dataSourceProvider">
    /// Optional shared data source provider. When provided, uses shared connection pool.
    /// When null, creates a dedicated data source (legacy behavior).
    /// </param>
    public PostgresSagaStore(
        IOptions<PostgresSagaStoreOptions> options,
        ILogger<PostgresSagaStore> logger,
        IPostgresDataSourceProvider? dataSourceProvider = null
    )
    {
        _options = options.Value;
        _logger = logger;

        if (dataSourceProvider is not null)
        {
            // Use shared data source from provider
            _dataSource = dataSourceProvider.GetDataSource(_options.ConnectionString);
            _ownsDataSource = false;
        }
        else
        {
            // Create dedicated data source (legacy behavior for backwards compatibility)
            _dataSource = NpgsqlDataSource.Create(_options.ConnectionString);
            _ownsDataSource = true;
        }
    }

    /// <inheritdoc />
    public async ValueTask CreateAsync(Saga saga, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(saga);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(cancellationToken)
            .ConfigureAwait(false);

        try
        {
            var metadataJson = saga.Metadata is not null
                ? JsonSerializer.Serialize(saga.Metadata, FallbackOptions)
                : null;

            // Insert saga
            var sagaSql = $"""
                INSERT INTO {_options.Schema}.{_options.SagasTableName}
                    (id, name, state, current_step_index, failure_reason, correlation_id,
                     metadata, created_at, started_at, completed_at)
                VALUES
                    (@id, @name, @state, @currentStepIndex, @failureReason, @correlationId,
                     @metadata::jsonb, @createdAt, @startedAt, @completedAt)
                """;

            await using var sagaCmd = connection.CreateCommand();
            sagaCmd.Transaction = transaction;
            sagaCmd.CommandText = sagaSql;
            sagaCmd.Parameters.AddWithValue("id", saga.Id);
            sagaCmd.Parameters.AddWithValue("name", saga.Name);
            sagaCmd.Parameters.AddWithValue("state", saga.State.ToString());
            sagaCmd.Parameters.AddWithValue("currentStepIndex", saga.CurrentStepIndex);
            sagaCmd.Parameters.AddWithValue(
                "failureReason",
                saga.FailureReason ?? (object)DBNull.Value
            );
            sagaCmd.Parameters.AddWithValue(
                "correlationId",
                saga.CorrelationId ?? (object)DBNull.Value
            );
            sagaCmd.Parameters.AddWithValue("metadata", metadataJson ?? (object)DBNull.Value);
            sagaCmd.Parameters.AddWithValue("createdAt", saga.CreatedAt.UtcDateTime);
            sagaCmd.Parameters.AddWithValue(
                "startedAt",
                saga.StartedAt?.UtcDateTime ?? (object)DBNull.Value
            );
            sagaCmd.Parameters.AddWithValue(
                "completedAt",
                saga.CompletedAt?.UtcDateTime ?? (object)DBNull.Value
            );

            await sagaCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            // Insert steps
            foreach (var step in saga.Steps)
            {
                await InsertStepAsync(connection, transaction, saga.Id, step, cancellationToken)
                    .ConfigureAwait(false);

                // Add task-to-saga mapping if task ID is set
                if (step.ExecuteTaskId is not null)
                {
                    await AddTaskMappingAsync(
                            connection,
                            transaction,
                            step.ExecuteTaskId,
                            saga.Id,
                            cancellationToken
                        )
                        .ConfigureAwait(false);
                }
            }

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }

        _logger.LogDebug("Created saga {SagaId} with {Count} steps", saga.Id, saga.Steps.Count);
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

        // Get saga
        var sagaSql = $"""
            SELECT id, name, state, current_step_index, failure_reason, correlation_id,
                   metadata, created_at, started_at, completed_at
            FROM {_options.Schema}.{_options.SagasTableName}
            WHERE id = @sagaId
            """;

        await using var sagaCmd = _dataSource.CreateCommand(sagaSql);
        sagaCmd.Parameters.AddWithValue("sagaId", sagaId);

        await using var sagaReader = await sagaCmd
            .ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        if (!await sagaReader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        var saga = ReadSagaHeader(sagaReader);
        await sagaReader.CloseAsync().ConfigureAwait(false);

        // Get steps
        var stepsSql = $"""
            SELECT id, name, step_order, execute_task, compensate_task, state,
                   execute_task_id, compensate_task_id, result, error, started_at, completed_at
            FROM {_options.Schema}.{_options.SagaStepsTableName}
            WHERE saga_id = @sagaId
            ORDER BY step_order
            """;

        await using var stepsCmd = _dataSource.CreateCommand(stepsSql);
        stepsCmd.Parameters.AddWithValue("sagaId", sagaId);

        await using var stepsReader = await stepsCmd
            .ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        var steps = new List<SagaStep>();
        while (await stepsReader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            steps.Add(ReadSagaStep(stepsReader));
        }

        return saga with
        {
            Steps = steps,
        };
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

        var sql = $"""
            UPDATE {_options.Schema}.{_options.SagasTableName}
            SET state = @state,
                failure_reason = @failureReason,
                started_at = CASE WHEN @state = 'Executing' AND started_at IS NULL THEN @now ELSE started_at END,
                completed_at = CASE WHEN @state IN ('Completed', 'Failed', 'Compensated', 'CompensationFailed', 'Cancelled')
                                    THEN @now ELSE completed_at END
            WHERE id = @sagaId
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("sagaId", sagaId);
        cmd.Parameters.AddWithValue("state", state.ToString());
        cmd.Parameters.AddWithValue("failureReason", failureReason ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("now", DateTimeOffset.UtcNow.UtcDateTime);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        return await GetAsync(sagaId, cancellationToken).ConfigureAwait(false);
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

        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(cancellationToken)
            .ConfigureAwait(false);

        try
        {
            var resultJson = result is not null
                ? JsonSerializer.Serialize(result, FallbackOptions)
                : null;

            var now = DateTimeOffset.UtcNow.UtcDateTime;

            var sql = $"""
                UPDATE {_options.Schema}.{_options.SagaStepsTableName}
                SET state = @state,
                    execute_task_id = COALESCE(@taskId, execute_task_id),
                    compensate_task_id = COALESCE(@compensateTaskId, compensate_task_id),
                    result = COALESCE(@result::jsonb, result),
                    error = COALESCE(@error, error),
                    started_at = CASE WHEN @state = 'Executing' AND started_at IS NULL THEN @now ELSE started_at END,
                    completed_at = CASE WHEN @state IN ('Completed', 'Failed', 'Compensated', 'CompensationFailed', 'Skipped')
                                        THEN @now ELSE completed_at END
                WHERE saga_id = @sagaId AND id = @stepId
                """;

            await using var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("sagaId", sagaId);
            cmd.Parameters.AddWithValue("stepId", stepId);
            cmd.Parameters.AddWithValue("state", state.ToString());
            cmd.Parameters.AddWithValue("taskId", taskId ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue(
                "compensateTaskId",
                compensateTaskId ?? (object)DBNull.Value
            );
            cmd.Parameters.AddWithValue("result", resultJson ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("error", errorMessage ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("now", now);

            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            // Add task mapping if task ID is set
            if (taskId is not null)
            {
                await AddTaskMappingAsync(
                        connection,
                        transaction,
                        taskId,
                        sagaId,
                        cancellationToken
                    )
                    .ConfigureAwait(false);
            }

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }

        return await GetAsync(sagaId, cancellationToken).ConfigureAwait(false);
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

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var newState = success ? SagaStepState.Compensated : SagaStepState.CompensationFailed;

        var sql = $"""
            UPDATE {_options.Schema}.{_options.SagaStepsTableName}
            SET compensate_task_id = COALESCE(@compensateTaskId, compensate_task_id),
                error = @error,
                state = @state,
                completed_at = @now
            WHERE saga_id = @sagaId AND id = @stepId
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("sagaId", sagaId);
        cmd.Parameters.AddWithValue("stepId", stepId);
        cmd.Parameters.AddWithValue("compensateTaskId", compensateTaskId ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("error", errorMessage ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("state", newState.ToString());
        cmd.Parameters.AddWithValue("now", DateTimeOffset.UtcNow.UtcDateTime);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        return await GetAsync(sagaId, cancellationToken).ConfigureAwait(false);
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

        var sql = $"""
            UPDATE {_options.Schema}.{_options.SagasTableName}
            SET current_step_index = current_step_index + 1
            WHERE id = @sagaId
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("sagaId", sagaId);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        return await GetAsync(sagaId, cancellationToken).ConfigureAwait(false);
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

        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(cancellationToken)
            .ConfigureAwait(false);

        try
        {
            // Delete task mappings
            var mappingSql = $"""
                DELETE FROM {_options.Schema}.{_options.TaskSagaTableName}
                WHERE saga_id = @sagaId
                """;

            await using var mappingCmd = connection.CreateCommand();
            mappingCmd.Transaction = transaction;
            mappingCmd.CommandText = mappingSql;
            mappingCmd.Parameters.AddWithValue("sagaId", sagaId);
            await mappingCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            // Delete steps
            var stepsSql = $"""
                DELETE FROM {_options.Schema}.{_options.SagaStepsTableName}
                WHERE saga_id = @sagaId
                """;

            await using var stepsCmd = connection.CreateCommand();
            stepsCmd.Transaction = transaction;
            stepsCmd.CommandText = stepsSql;
            stepsCmd.Parameters.AddWithValue("sagaId", sagaId);
            await stepsCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            // Delete saga
            var sagaSql = $"""
                DELETE FROM {_options.Schema}.{_options.SagasTableName}
                WHERE id = @sagaId
                """;

            await using var sagaCmd = connection.CreateCommand();
            sagaCmd.Transaction = transaction;
            sagaCmd.CommandText = sagaSql;
            sagaCmd.Parameters.AddWithValue("sagaId", sagaId);
            var deleted = await sagaCmd
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);

            return deleted > 0;
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }
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

        var sql = $"""
            SELECT saga_id FROM {_options.Schema}.{_options.TaskSagaTableName}
            WHERE task_id = @taskId
            LIMIT 1
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("taskId", taskId);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result as string;
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

        var sql = $"""
            SELECT id FROM {_options.Schema}.{_options.SagasTableName}
            WHERE state = @state
            ORDER BY created_at DESC
            LIMIT @limit
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("state", state.ToString());
        cmd.Parameters.AddWithValue("limit", limit);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        var sagaIds = new List<string>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            sagaIds.Add(reader.GetString(0));
        }
        await reader.CloseAsync().ConfigureAwait(false);

        foreach (var sagaId in sagaIds)
        {
            var saga = await GetAsync(sagaId, cancellationToken).ConfigureAwait(false);
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

        // Only dispose data source if we created it (not using shared provider)
        if (_ownsDataSource)
        {
            await _dataSource.DisposeAsync().ConfigureAwait(false);
        }

        _logger.LogInformation("PostgreSQL saga store disposed");
    }

    private async ValueTask EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_initialized)
        {
            return;
        }

        if (_options.AutoCreateTables)
        {
            await CreateTablesAsync(cancellationToken).ConfigureAwait(false);
        }

        _initialized = true;
    }

    private async Task CreateTablesAsync(CancellationToken cancellationToken)
    {
        var sql = $"""
            CREATE TABLE IF NOT EXISTS {_options.Schema}.{_options.SagasTableName} (
                id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                state VARCHAR(50) NOT NULL,
                current_step_index INTEGER NOT NULL DEFAULT 0,
                failure_reason TEXT,
                correlation_id VARCHAR(255),
                metadata JSONB,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                started_at TIMESTAMP WITH TIME ZONE,
                completed_at TIMESTAMP WITH TIME ZONE
            );

            CREATE INDEX IF NOT EXISTS idx_{_options.SagasTableName}_state
                ON {_options.Schema}.{_options.SagasTableName} (state);

            CREATE INDEX IF NOT EXISTS idx_{_options.SagasTableName}_correlation_id
                ON {_options.Schema}.{_options.SagasTableName} (correlation_id)
                WHERE correlation_id IS NOT NULL;

            CREATE TABLE IF NOT EXISTS {_options.Schema}.{_options.SagaStepsTableName} (
                saga_id VARCHAR(255) NOT NULL,
                id VARCHAR(255) NOT NULL,
                name VARCHAR(255) NOT NULL,
                step_order INTEGER NOT NULL,
                execute_task JSONB NOT NULL,
                compensate_task JSONB,
                state VARCHAR(50) NOT NULL,
                execute_task_id VARCHAR(255),
                compensate_task_id VARCHAR(255),
                result JSONB,
                error TEXT,
                started_at TIMESTAMP WITH TIME ZONE,
                completed_at TIMESTAMP WITH TIME ZONE,
                PRIMARY KEY (saga_id, id),
                FOREIGN KEY (saga_id) REFERENCES {_options.Schema}.{_options.SagasTableName}(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS {_options.Schema}.{_options.TaskSagaTableName} (
                task_id VARCHAR(255) PRIMARY KEY,
                saga_id VARCHAR(255) NOT NULL,
                FOREIGN KEY (saga_id) REFERENCES {_options.Schema}.{_options.SagasTableName}(id) ON DELETE CASCADE
            );
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("PostgreSQL saga store tables created/verified");
    }

    private async Task InsertStepAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string sagaId,
        SagaStep step,
        CancellationToken cancellationToken
    )
    {
        var executeTaskJson = JsonSerializer.Serialize(step.ExecuteTask, SignatureTypeInfo);
        var compensateTaskJson = step.CompensateTask is not null
            ? JsonSerializer.Serialize(step.CompensateTask, SignatureTypeInfo)
            : null;
        var resultJson = step.Result is not null
            ? JsonSerializer.Serialize(step.Result, FallbackOptions)
            : null;

        var sql = $"""
            INSERT INTO {_options.Schema}.{_options.SagaStepsTableName}
                (saga_id, id, name, step_order, execute_task, compensate_task, state,
                 execute_task_id, compensate_task_id, result, error, started_at, completed_at)
            VALUES
                (@sagaId, @id, @name, @stepOrder, @executeTask::jsonb, @compensateTask::jsonb, @state,
                 @executeTaskId, @compensateTaskId, @result::jsonb, @error, @startedAt, @completedAt)
            """;

        await using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("sagaId", sagaId);
        cmd.Parameters.AddWithValue("id", step.Id);
        cmd.Parameters.AddWithValue("name", step.Name);
        cmd.Parameters.AddWithValue("stepOrder", step.Order);
        cmd.Parameters.AddWithValue("executeTask", executeTaskJson);
        cmd.Parameters.AddWithValue("compensateTask", compensateTaskJson ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("state", step.State.ToString());
        cmd.Parameters.AddWithValue("executeTaskId", step.ExecuteTaskId ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue(
            "compensateTaskId",
            step.CompensateTaskId ?? (object)DBNull.Value
        );
        cmd.Parameters.AddWithValue("result", resultJson ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("error", step.Error ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue(
            "startedAt",
            step.StartedAt?.UtcDateTime ?? (object)DBNull.Value
        );
        cmd.Parameters.AddWithValue(
            "completedAt",
            step.CompletedAt?.UtcDateTime ?? (object)DBNull.Value
        );

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task AddTaskMappingAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string taskId,
        string sagaId,
        CancellationToken cancellationToken
    )
    {
        var sql = $"""
            INSERT INTO {_options.Schema}.{_options.TaskSagaTableName}
                (task_id, saga_id)
            VALUES
                (@taskId, @sagaId)
            ON CONFLICT (task_id) DO NOTHING
            """;

        await using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("taskId", taskId);
        cmd.Parameters.AddWithValue("sagaId", sagaId);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static Saga ReadSagaHeader(NpgsqlDataReader reader)
    {
        var stateStr = reader.GetString(2);
        var state = Enum.TryParse<SagaState>(stateStr, out var s) ? s : SagaState.Created;

        IReadOnlyDictionary<string, object>? metadata = null;
        if (!reader.IsDBNull(6))
        {
            var metadataJson = reader.GetString(6);
            metadata = JsonSerializer.Deserialize<Dictionary<string, object>>(
                metadataJson,
                FallbackOptions
            );
        }

        return new Saga
        {
            Id = reader.GetString(0),
            Name = reader.GetString(1),
            State = state,
            CurrentStepIndex = reader.GetInt32(3),
            FailureReason = reader.IsDBNull(4) ? null : reader.GetString(4),
            CorrelationId = reader.IsDBNull(5) ? null : reader.GetString(5),
            Metadata = metadata,
            CreatedAt = new DateTimeOffset(reader.GetDateTime(7), TimeSpan.Zero),
            StartedAt = reader.IsDBNull(8)
                ? null
                : new DateTimeOffset(reader.GetDateTime(8), TimeSpan.Zero),
            CompletedAt = reader.IsDBNull(9)
                ? null
                : new DateTimeOffset(reader.GetDateTime(9), TimeSpan.Zero),
            Steps = [],
        };
    }

    private static SagaStep ReadSagaStep(NpgsqlDataReader reader)
    {
        var stateStr = reader.GetString(5);
        var state = Enum.TryParse<SagaStepState>(stateStr, out var s) ? s : SagaStepState.Pending;

        var executeTaskJson = reader.GetString(3);
        var executeTask =
            JsonSerializer.Deserialize(executeTaskJson, SignatureTypeInfo)
            ?? throw new InvalidOperationException("Failed to deserialize execute task");

        Signature? compensateTask = null;
        if (!reader.IsDBNull(4))
        {
            var compensateTaskJson = reader.GetString(4);
            compensateTask = JsonSerializer.Deserialize(compensateTaskJson, SignatureTypeInfo);
        }

        object? result = null;
        if (!reader.IsDBNull(8))
        {
            var resultJson = reader.GetString(8);
            result = JsonSerializer.Deserialize<object>(resultJson, FallbackOptions);
        }

        return new SagaStep
        {
            Id = reader.GetString(0),
            Name = reader.GetString(1),
            Order = reader.GetInt32(2),
            ExecuteTask = executeTask,
            CompensateTask = compensateTask,
            State = state,
            ExecuteTaskId = reader.IsDBNull(6) ? null : reader.GetString(6),
            CompensateTaskId = reader.IsDBNull(7) ? null : reader.GetString(7),
            Result = result,
            Error = reader.IsDBNull(9) ? null : reader.GetString(9),
            StartedAt = reader.IsDBNull(10)
                ? null
                : new DateTimeOffset(reader.GetDateTime(10), TimeSpan.Zero),
            CompletedAt = reader.IsDBNull(11)
                ? null
                : new DateTimeOffset(reader.GetDateTime(11), TimeSpan.Zero),
        };
    }
}
