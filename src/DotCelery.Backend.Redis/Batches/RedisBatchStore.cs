using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Backend.Redis.Serialization;
using DotCelery.Core.Batches;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.Batches;

/// <summary>
/// Redis implementation of <see cref="IBatchStore"/>.
/// Uses Redis hashes for batch data and sorted sets for task-to-batch indexing.
/// </summary>
public sealed class RedisBatchStore : IBatchStore
{
    private readonly RedisBatchStoreOptions _options;
    private readonly ILogger<RedisBatchStore> _logger;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    // AOT-friendly type info
    private static JsonTypeInfo<Batch> BatchTypeInfo => RedisBackendJsonContext.Default.Batch;

    private ConnectionMultiplexer? _connection;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisBatchStore"/> class.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <param name="logger">The logger.</param>
    public RedisBatchStore(
        IOptions<RedisBatchStoreOptions> options,
        ILogger<RedisBatchStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async ValueTask CreateAsync(Batch batch, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(batch);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var key = GetBatchKey(batch.Id);
        var json = JsonSerializer.Serialize(batch, BatchTypeInfo);

        await db.StringSetAsync(key, json).ConfigureAwait(false);

        // Index all task IDs
        foreach (var taskId in batch.TaskIds)
        {
            await db.HashSetAsync(_options.TaskToBatchKey, taskId, batch.Id).ConfigureAwait(false);
        }

        _logger.LogDebug(
            "Created batch {BatchId} with {TaskCount} tasks",
            batch.Id,
            batch.TaskIds.Count
        );
    }

    /// <inheritdoc />
    public async ValueTask<Batch?> GetAsync(
        string batchId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(batchId);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var key = GetBatchKey(batchId);

        var json = await db.StringGetAsync(key).ConfigureAwait(false);
        if (json.IsNullOrEmpty)
        {
            return null;
        }

        return JsonSerializer.Deserialize((string)json!, BatchTypeInfo);
    }

    /// <inheritdoc />
    public async ValueTask UpdateStateAsync(
        string batchId,
        BatchState state,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(batchId);

        var batch = await GetAsync(batchId, cancellationToken).ConfigureAwait(false);
        if (batch is null)
        {
            return;
        }

        var completedAt = state
            is BatchState.Completed
                or BatchState.Failed
                or BatchState.PartiallyCompleted
                or BatchState.Cancelled
            ? DateTimeOffset.UtcNow
            : batch.CompletedAt;

        var updatedBatch = batch with { State = state, CompletedAt = completedAt };

        await SaveBatchAsync(updatedBatch, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<Batch?> MarkTaskCompletedAsync(
        string batchId,
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(batchId);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        var batch = await GetAsync(batchId, cancellationToken).ConfigureAwait(false);
        if (batch is null)
        {
            return null;
        }

        var completedIds = batch.CompletedTaskIds.ToList();
        if (!completedIds.Contains(taskId))
        {
            completedIds.Add(taskId);
        }

        var updatedBatch = batch with { CompletedTaskIds = completedIds };

        // Update state if all tasks completed
        if (updatedBatch.IsFinished)
        {
            var newState =
                updatedBatch.FailedCount > 0 ? BatchState.PartiallyCompleted : BatchState.Completed;

            updatedBatch = updatedBatch with
            {
                State = newState,
                CompletedAt = DateTimeOffset.UtcNow,
            };
        }
        else if (batch.State == BatchState.Pending)
        {
            updatedBatch = updatedBatch with { State = BatchState.Processing };
        }

        await SaveBatchAsync(updatedBatch, cancellationToken).ConfigureAwait(false);
        return updatedBatch;
    }

    /// <inheritdoc />
    public async ValueTask<Batch?> MarkTaskFailedAsync(
        string batchId,
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(batchId);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        var batch = await GetAsync(batchId, cancellationToken).ConfigureAwait(false);
        if (batch is null)
        {
            return null;
        }

        var failedIds = batch.FailedTaskIds.ToList();
        if (!failedIds.Contains(taskId))
        {
            failedIds.Add(taskId);
        }

        var updatedBatch = batch with { FailedTaskIds = failedIds };

        // Update state if all tasks completed
        if (updatedBatch.IsFinished)
        {
            var newState =
                updatedBatch.CompletedCount > 0 ? BatchState.PartiallyCompleted : BatchState.Failed;

            updatedBatch = updatedBatch with
            {
                State = newState,
                CompletedAt = DateTimeOffset.UtcNow,
            };
        }
        else if (batch.State == BatchState.Pending)
        {
            updatedBatch = updatedBatch with { State = BatchState.Processing };
        }

        await SaveBatchAsync(updatedBatch, cancellationToken).ConfigureAwait(false);
        return updatedBatch;
    }

    /// <inheritdoc />
    public async ValueTask<bool> DeleteAsync(
        string batchId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(batchId);

        var batch = await GetAsync(batchId, cancellationToken).ConfigureAwait(false);
        if (batch is null)
        {
            return false;
        }

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var key = GetBatchKey(batchId);

        // Remove task mappings
        foreach (var taskId in batch.TaskIds)
        {
            await db.HashDeleteAsync(_options.TaskToBatchKey, taskId).ConfigureAwait(false);
        }

        // Delete the batch
        var deleted = await db.KeyDeleteAsync(key).ConfigureAwait(false);
        return deleted;
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetBatchIdForTaskAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var batchId = await db.HashGetAsync(_options.TaskToBatchKey, taskId).ConfigureAwait(false);

        return batchId.IsNullOrEmpty ? null : (string)batchId!;
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

        _logger.LogInformation("Redis batch store disposed");
    }

    private string GetBatchKey(string batchId) => $"{_options.BatchKeyPrefix}{batchId}";

    private async Task SaveBatchAsync(Batch batch, CancellationToken cancellationToken)
    {
        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var key = GetBatchKey(batch.Id);
        var json = JsonSerializer.Serialize(batch, BatchTypeInfo);

        if (batch.IsFinished && _options.BatchTtl.HasValue)
        {
            await db.StringSetAsync(key, json, _options.BatchTtl.Value).ConfigureAwait(false);
        }
        else
        {
            await db.StringSetAsync(key, json).ConfigureAwait(false);
        }
    }

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
            _logger.LogInformation(
                "Connected to Redis for batch store at {ConnectionString}",
                _options.ConnectionString
            );

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
