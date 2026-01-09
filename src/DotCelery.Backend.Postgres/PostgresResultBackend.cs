using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Text.RegularExpressions;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace DotCelery.Backend.Postgres;

/// <summary>
/// PostgreSQL result backend implementation using Npgsql.
/// </summary>
public sealed partial class PostgresResultBackend : IResultBackend
{
    private readonly PostgresBackendOptions _options;
    private readonly ILogger<PostgresResultBackend> _logger;
    private readonly NpgsqlDataSource _dataSource;
    private readonly ConcurrentDictionary<string, TaskCompletionSource<TaskResult>> _waiters =
        new();
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly CancellationTokenSource _cleanupCts = new();

    // AOT-friendly type info
    private static JsonTypeInfo<TaskResult> TaskResultTypeInfo =>
        DotCeleryJsonContext.Default.TaskResult;

    private static JsonTypeInfo<TaskExceptionInfo> TaskExceptionInfoTypeInfo =>
        DotCeleryJsonContext.Default.TaskExceptionInfo;

    // Fallback options for types that cannot be AOT-compiled (e.g., Dictionary<string, object>)
    private static readonly JsonSerializerOptions FallbackOptions =
        JsonMessageSerializer.CreateDefaultOptions();

    private Task? _cleanupTask;
    private bool _initialized;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresResultBackend"/> class.
    /// </summary>
    /// <param name="options">The backend options.</param>
    /// <param name="logger">The logger.</param>
    public PostgresResultBackend(
        IOptions<PostgresBackendOptions> options,
        ILogger<PostgresResultBackend> logger
    )
    {
        _options = options.Value;
        _logger = logger;

        var dataSourceBuilder = new NpgsqlDataSourceBuilder(_options.ConnectionString);
        dataSourceBuilder.EnableDynamicJson();
        _dataSource = dataSourceBuilder.Build();
    }

    /// <inheritdoc />
    public async ValueTask StoreResultAsync(
        TaskResult result,
        TimeSpan? expiry = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(result);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var effectiveExpiry = expiry ?? _options.DefaultExpiry;
        var expiresAt = DateTimeOffset.UtcNow.Add(effectiveExpiry);

        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(cancellationToken)
            .ConfigureAwait(false);

        try
        {
            await using var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandTimeout = (int)_options.CommandTimeout.TotalSeconds;
            cmd.CommandText =
                $@"
                INSERT INTO {GetFullTableName()}
                    (task_id, state, result, content_type, exception, completed_at,
                     duration_ms, retries, worker, metadata, expires_at)
                VALUES
                    ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10::jsonb, $11)
                ON CONFLICT (task_id) DO UPDATE SET
                    state = EXCLUDED.state,
                    result = EXCLUDED.result,
                    content_type = EXCLUDED.content_type,
                    exception = EXCLUDED.exception,
                    completed_at = EXCLUDED.completed_at,
                    duration_ms = EXCLUDED.duration_ms,
                    retries = EXCLUDED.retries,
                    worker = EXCLUDED.worker,
                    metadata = EXCLUDED.metadata,
                    expires_at = EXCLUDED.expires_at";

            cmd.Parameters.AddWithValue(result.TaskId);
            cmd.Parameters.AddWithValue(result.State.ToString());
            cmd.Parameters.AddWithValue(result.Result ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue(result.ContentType ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue(
                result.Exception is not null
                    ? JsonSerializer.Serialize(result.Exception, TaskExceptionInfoTypeInfo)
                    : DBNull.Value
            );
            cmd.Parameters.AddWithValue(result.CompletedAt.UtcDateTime);
            cmd.Parameters.AddWithValue((long)result.Duration.TotalMilliseconds);
            cmd.Parameters.AddWithValue(result.Retries);
            cmd.Parameters.AddWithValue(result.Worker ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue(
                result.Metadata is not null
                    ? JsonSerializer.Serialize(result.Metadata, FallbackOptions)
                    : DBNull.Value
            );
            cmd.Parameters.AddWithValue(expiresAt.UtcDateTime);

            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            // Send NOTIFY using pg_notify() function with parameters to prevent SQL injection
            if (_options.UseListenNotify)
            {
                var channel = GetNotifyChannel(result.TaskId);
                var payload = JsonSerializer.Serialize(result, TaskResultTypeInfo);

                await using var notifyCmd = connection.CreateCommand();
                notifyCmd.Transaction = transaction;
                // Use pg_notify() function with parameters instead of NOTIFY command
                // This prevents SQL injection in the payload
                notifyCmd.CommandText = "SELECT pg_notify(@channel, @payload)";
                notifyCmd.Parameters.AddWithValue("channel", channel);
                notifyCmd.Parameters.AddWithValue("payload", payload);
                await notifyCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }

        _logger.LogDebug(
            "Stored result for task {TaskId} with state {State}",
            result.TaskId,
            result.State
        );

        // Notify local waiters
        if (_waiters.TryRemove(result.TaskId, out var tcs))
        {
            tcs.TrySetResult(result);
        }
    }

    /// <inheritdoc />
    public async ValueTask<TaskResult?> GetResultAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.CommandTimeout = (int)_options.CommandTimeout.TotalSeconds;
        cmd.CommandText =
            $@"
            SELECT task_id, state, result, content_type, exception, completed_at,
                   duration_ms, retries, worker, metadata
            FROM {GetFullTableName()}
            WHERE task_id = $1 AND (expires_at IS NULL OR expires_at > NOW())";
        cmd.Parameters.AddWithValue(taskId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        return ReadTaskResult(reader);
    }

    /// <inheritdoc />
    public async Task<TaskResult> WaitForResultAsync(
        string taskId,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        // Check if result already exists
        var existing = await GetResultAsync(taskId, cancellationToken).ConfigureAwait(false);
        if (existing is not null)
        {
            return existing;
        }

        // Create or get existing waiter
        var tcs = _waiters.GetOrAdd(taskId, _ => new TaskCompletionSource<TaskResult>());

        // Check again after adding waiter (race condition)
        existing = await GetResultAsync(taskId, cancellationToken).ConfigureAwait(false);
        if (existing is not null)
        {
            _waiters.TryRemove(taskId, out _);
            return existing;
        }

        // Create a dedicated connection for this wait operation if LISTEN/NOTIFY is enabled
        NpgsqlConnection? dedicatedListenerConnection = null;
        Task? dedicatedListenerTask = null;
        var dedicatedListenerCts = new CancellationTokenSource();

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            if (timeout.HasValue)
            {
                cts.CancelAfter(timeout.Value);
            }

            await using var registration = cts.Token.Register(() => tcs.TrySetCanceled(cts.Token));

            // Subscribe to LISTEN/NOTIFY if enabled using a dedicated connection
            if (_options.UseListenNotify)
            {
                dedicatedListenerConnection = await _dataSource
                    .OpenConnectionAsync(cancellationToken)
                    .ConfigureAwait(false);

                dedicatedListenerConnection.Notification += (_, e) =>
                {
                    if (
                        e.Channel.StartsWith(_options.NotifyChannelPrefix, StringComparison.Ordinal)
                    )
                    {
                        var notifiedTaskId = e.Channel[_options.NotifyChannelPrefix.Length..];

                        if (
                            notifiedTaskId == SanitizeChannelName(taskId)
                            && _waiters.TryRemove(taskId, out var waiter)
                        )
                        {
                            try
                            {
                                var result = JsonSerializer.Deserialize(
                                    e.Payload,
                                    TaskResultTypeInfo
                                );
                                if (result is not null)
                                {
                                    waiter.TrySetResult(result);
                                }
                            }
                            catch (Exception ex)
                            {
                                _logger.LogWarning(
                                    ex,
                                    "Failed to deserialize notification payload for task {TaskId}",
                                    taskId
                                );
                            }
                        }
                    }
                };

                var channel = GetNotifyChannel(taskId);

                // Validate channel name is safe (should only contain alphanumeric, underscore after sanitization)
                // LISTEN doesn't support parameterized channel names, so we must validate
                if (!IsValidChannelName(channel))
                {
                    throw new InvalidOperationException($"Invalid LISTEN channel name: {channel}");
                }

                await using var cmd = dedicatedListenerConnection.CreateCommand();
                cmd.CommandText = $"LISTEN {channel}";
                await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                // Start listening for notifications on this dedicated connection
                var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                    cts.Token,
                    dedicatedListenerCts.Token
                );
                dedicatedListenerTask = Task.Run(
                    async () =>
                    {
                        try
                        {
                            while (!linkedCts.Token.IsCancellationRequested)
                            {
                                await dedicatedListenerConnection
                                    .WaitAsync(linkedCts.Token)
                                    .ConfigureAwait(false);
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            // Expected during cancellation
                        }
                        catch (Exception ex)
                        {
                            _logger.LogDebug(
                                ex,
                                "Dedicated listener connection ended for task {TaskId}",
                                taskId
                            );
                        }
                    },
                    linkedCts.Token
                );
            }

            // Also poll in case notification is missed
            _ = Task.Run(
                async () =>
                {
                    try
                    {
                        while (!tcs.Task.IsCompleted && !cts.Token.IsCancellationRequested)
                        {
                            await Task.Delay(_options.PollingInterval, cts.Token)
                                .ConfigureAwait(false);

                            var result = await GetResultAsync(taskId, cts.Token)
                                .ConfigureAwait(false);
                            if (result is not null && _waiters.TryRemove(taskId, out var waiter))
                            {
                                waiter.TrySetResult(result);
                                break;
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Expected during cancellation
                    }
                },
                cts.Token
            );

            return await tcs.Task.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (timeout.HasValue && !cancellationToken.IsCancellationRequested)
        {
            _waiters.TryRemove(taskId, out _);
            throw new TimeoutException(
                $"Timeout waiting for task {taskId} result after {timeout.Value}"
            );
        }
        finally
        {
            // Cancel and clean up the dedicated listener
            await dedicatedListenerCts.CancelAsync().ConfigureAwait(false);

            if (dedicatedListenerTask is not null)
            {
                try
                {
                    await dedicatedListenerTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Expected
                }
            }

            if (dedicatedListenerConnection is not null)
            {
                await dedicatedListenerConnection.DisposeAsync().ConfigureAwait(false);
            }

            dedicatedListenerCts.Dispose();
        }
    }

    /// <inheritdoc />
    public async ValueTask UpdateStateAsync(
        string taskId,
        TaskState state,
        object? metadata = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.CommandTimeout = (int)_options.CommandTimeout.TotalSeconds;

        var expiresAt = DateTimeOffset.UtcNow.Add(_options.DefaultExpiry);

        cmd.CommandText =
            $@"
            INSERT INTO {GetFullTableName()}
                (task_id, state, completed_at, duration_ms, expires_at)
            VALUES ($1, $2, $3, 0, $4)
            ON CONFLICT (task_id) DO UPDATE SET state = EXCLUDED.state";

        cmd.Parameters.AddWithValue(taskId);
        cmd.Parameters.AddWithValue(state.ToString());
        cmd.Parameters.AddWithValue(DateTimeOffset.UtcNow.UtcDateTime);
        cmd.Parameters.AddWithValue(expiresAt.UtcDateTime);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogDebug("Updated state for task {TaskId} to {State}", taskId, state);
    }

    /// <inheritdoc />
    public async ValueTask<TaskState?> GetStateAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.CommandTimeout = (int)_options.CommandTimeout.TotalSeconds;
        cmd.CommandText =
            $@"
            SELECT state FROM {GetFullTableName()}
            WHERE task_id = $1 AND (expires_at IS NULL OR expires_at > NOW())";
        cmd.Parameters.AddWithValue(taskId);

        var value = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        if (value is null or DBNull)
        {
            return null;
        }

        return Enum.TryParse<TaskState>((string)value, ignoreCase: true, out var state)
            ? state
            : null;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        // Cancel cleanup task
        await _cleanupCts.CancelAsync().ConfigureAwait(false);

        // Cancel all waiters
        foreach (var tcs in _waiters.Values)
        {
            tcs.TrySetCanceled();
        }
        _waiters.Clear();

        // Wait for cleanup task to complete
        if (_cleanupTask is not null)
        {
            try
            {
                await _cleanupTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        }

        // Dispose data source
        await _dataSource.DisposeAsync().ConfigureAwait(false);

        _initLock.Dispose();
        _cleanupCts.Dispose();

        _logger.LogInformation("PostgreSQL backend disposed");
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

            if (_options.AutoCreateTables)
            {
                await CreateTablesAsync(cancellationToken).ConfigureAwait(false);
            }

            // Note: LISTEN/NOTIFY connections are created per-wait in WaitForResultAsync
            // to avoid holding a connection open indefinitely from the pool

            if (_options.CleanupInterval.HasValue)
            {
                _cleanupTask = RunCleanupLoopAsync(_cleanupCts.Token);
            }

            _initialized = true;
            _logger.LogInformation("PostgreSQL backend initialized");
        }
        finally
        {
            _initLock.Release();
        }
    }

    private async Task CreateTablesAsync(CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText =
            $@"
            CREATE TABLE IF NOT EXISTS {GetFullTableName()} (
                task_id VARCHAR(255) PRIMARY KEY,
                state VARCHAR(20) NOT NULL,
                result BYTEA,
                content_type VARCHAR(100),
                exception JSONB,
                completed_at TIMESTAMPTZ NOT NULL,
                duration_ms BIGINT NOT NULL DEFAULT 0,
                retries INT NOT NULL DEFAULT 0,
                worker VARCHAR(255),
                metadata JSONB,
                expires_at TIMESTAMPTZ
            );
            CREATE INDEX IF NOT EXISTS idx_{_options.TableName}_expires_at
                ON {GetFullTableName()}(expires_at) WHERE expires_at IS NOT NULL;";

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        _logger.LogDebug("Created/verified table {TableName}", GetFullTableName());
    }

    private async Task RunCleanupLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_options.CleanupInterval!.Value, cancellationToken)
                    .ConfigureAwait(false);
                await CleanupExpiredResultsAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during cleanup of expired results");
            }
        }
    }

    private async Task CleanupExpiredResultsAsync(CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.CommandTimeout = (int)_options.CommandTimeout.TotalSeconds;
        cmd.CommandText =
            $@"
            DELETE FROM {GetFullTableName()}
            WHERE task_id IN (
                SELECT task_id FROM {GetFullTableName()}
                WHERE expires_at IS NOT NULL AND expires_at < NOW()
                LIMIT $1
            )";
        cmd.Parameters.AddWithValue(_options.CleanupBatchSize);

        var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        if (deleted > 0)
        {
            _logger.LogInformation("Cleaned up {Count} expired task results", deleted);
        }
    }

    private static TaskResult ReadTaskResult(NpgsqlDataReader reader)
    {
        var taskId = reader.GetString(0);
        var stateStr = reader.GetString(1);
        var result = reader.IsDBNull(2) ? null : (byte[])reader[2];
        var contentType = reader.IsDBNull(3) ? null : reader.GetString(3);
        var exceptionJson = reader.IsDBNull(4) ? null : reader.GetString(4);
        var completedAt = reader.GetDateTime(5);
        var durationMs = reader.GetInt64(6);
        var retries = reader.GetInt32(7);
        var worker = reader.IsDBNull(8) ? null : reader.GetString(8);
        var metadataJson = reader.IsDBNull(9) ? null : reader.GetString(9);

        TaskExceptionInfo? exception = null;
        if (exceptionJson is not null)
        {
            exception = JsonSerializer.Deserialize(exceptionJson, TaskExceptionInfoTypeInfo);
        }

        // Note: Dictionary<string, object> cannot be fully AOT-compiled, use reflection-based fallback
        IReadOnlyDictionary<string, object>? metadata = null;
        if (metadataJson is not null)
        {
            metadata = JsonSerializer.Deserialize<Dictionary<string, object>>(
                metadataJson,
                FallbackOptions
            );
        }

        var state = Enum.TryParse<TaskState>(stateStr, ignoreCase: true, out var s)
            ? s
            : TaskState.Pending;

        return new TaskResult
        {
            TaskId = taskId,
            State = state,
            Result = result,
            ContentType = contentType,
            Exception = exception,
            CompletedAt = new DateTimeOffset(completedAt, TimeSpan.Zero),
            Duration = TimeSpan.FromMilliseconds(durationMs),
            Retries = retries,
            Worker = worker,
            Metadata = metadata,
        };
    }

    private string GetFullTableName() => $"{_options.Schema}.{_options.TableName}";

    /// <summary>
    /// Gets a safe PostgreSQL NOTIFY channel name for a task ID.
    /// Uses SHA256 hash truncated to 16 characters to ensure valid identifier format.
    /// </summary>
    private string GetNotifyChannel(string taskId) =>
        $"{_options.NotifyChannelPrefix}{SanitizeChannelName(taskId)}";

    /// <summary>
    /// Sanitizes a string to be a valid PostgreSQL identifier.
    /// Uses SHA256 hash for unpredictable or potentially unsafe input.
    /// </summary>
    private static string SanitizeChannelName(string name)
    {
        // If the name is already safe (alphanumeric + underscore only), use simple sanitization
        if (SafeIdentifierRegex().IsMatch(name))
        {
            var sanitized = name.Replace("-", "_", StringComparison.Ordinal)
                .Replace(".", "_", StringComparison.Ordinal);

            // Ensure starts with letter or underscore (prepend 't_' if starts with digit)
            if (char.IsDigit(sanitized[0]))
            {
                sanitized = "t_" + sanitized;
            }

            return sanitized;
        }

        // For unsafe input, use a hash to ensure safe identifier
        // Prefix with 'h_' to ensure valid identifier (hash may start with digit)
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(name));
        return "h_" + Convert.ToHexString(hash)[..16].ToLowerInvariant();
    }

    /// <summary>
    /// Validates that a channel name is safe for use in LISTEN command.
    /// Channel names must start with letter/underscore and contain only alphanumeric/underscore.
    /// </summary>
    private static bool IsValidChannelName(string channel)
    {
        if (string.IsNullOrEmpty(channel) || channel.Length > 63)
        {
            return false;
        }

        return ValidChannelNameRegex().IsMatch(channel);
    }

    /// <summary>
    /// Regex pattern for safe PostgreSQL identifiers (alphanumeric, underscore, hyphen, dot).
    /// Used for input validation before sanitization.
    /// </summary>
    [GeneratedRegex(@"^[a-zA-Z0-9_.\-]+$", RegexOptions.Compiled)]
    private static partial Regex SafeIdentifierRegex();

    /// <summary>
    /// Regex pattern for valid PostgreSQL channel names after sanitization.
    /// Must start with letter or underscore, contain only alphanumeric and underscore.
    /// </summary>
    [GeneratedRegex(@"^[a-zA-Z_][a-zA-Z0-9_]*$", RegexOptions.Compiled)]
    private static partial Regex ValidChannelNameRegex();
}
