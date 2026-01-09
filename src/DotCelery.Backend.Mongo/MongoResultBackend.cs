using System.Collections.Concurrent;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo;

/// <summary>
/// MongoDB result backend implementation using the official MongoDB driver.
/// </summary>
public sealed class MongoResultBackend : IResultBackend
{
    private readonly MongoBackendOptions _options;
    private readonly ILogger<MongoResultBackend> _logger;
    private readonly MongoClient _client;
    private readonly IMongoDatabase _database;
    private readonly ConcurrentDictionary<string, TaskCompletionSource<TaskResult>> _waiters =
        new();
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly CancellationTokenSource _disposeCts = new();

    private IMongoCollection<TaskResultDocument>? _collection;
    private Task? _changeStreamTask;
    private bool _initialized;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoResultBackend"/> class.
    /// </summary>
    /// <param name="options">The backend options.</param>
    /// <param name="logger">The logger.</param>
    public MongoResultBackend(
        IOptions<MongoBackendOptions> options,
        ILogger<MongoResultBackend> logger
    )
    {
        _options = options.Value;
        _logger = logger;

        var settings = MongoClientSettings.FromConnectionString(_options.ConnectionString);
        settings.ServerSelectionTimeout = _options.ServerSelectionTimeout;
        settings.ConnectTimeout = _options.ConnectTimeout;

        _client = new MongoClient(settings);
        _database = _client.GetDatabase(_options.DatabaseName);
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
        var expiresAt = DateTime.UtcNow.Add(effectiveExpiry);

        var document = TaskResultDocument.FromTaskResult(result, expiresAt);

        var filter = Builders<TaskResultDocument>.Filter.Eq(d => d.TaskId, result.TaskId);
        var options = new ReplaceOptions { IsUpsert = true };

        await _collection!
            .ReplaceOneAsync(filter, document, options, cancellationToken)
            .ConfigureAwait(false);

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

        var filter = Builders<TaskResultDocument>.Filter.And(
            Builders<TaskResultDocument>.Filter.Eq(d => d.TaskId, taskId),
            Builders<TaskResultDocument>.Filter.Or(
                Builders<TaskResultDocument>.Filter.Eq(d => d.ExpiresAt, null),
                Builders<TaskResultDocument>.Filter.Gt(d => d.ExpiresAt, DateTime.UtcNow)
            )
        );

        var document = await _collection!
            .Find(filter)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);

        return document?.ToTaskResult();
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

        // Create or get existing waiter with RunContinuationsAsynchronously to prevent inline continuations
        var tcs = _waiters.GetOrAdd(
            taskId,
            _ => new TaskCompletionSource<TaskResult>(
                TaskCreationOptions.RunContinuationsAsynchronously
            )
        );

        // Check again after adding waiter (race condition)
        existing = await GetResultAsync(taskId, cancellationToken).ConfigureAwait(false);
        if (existing is not null)
        {
            _waiters.TryRemove(taskId, out _);
            return existing;
        }

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            if (timeout.HasValue)
            {
                cts.CancelAfter(timeout.Value);
            }

            await using var registration = cts.Token.Register(() => tcs.TrySetCanceled(cts.Token));

            // Poll for result with proper exception handling
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
                        // Expected during cancellation or timeout
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error in polling handler for task {TaskId}", taskId);
                        // Signal the waiter with the exception so it doesn't hang indefinitely
                        if (_waiters.TryRemove(taskId, out var waiter))
                        {
                            waiter.TrySetException(ex);
                        }
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

        var filter = Builders<TaskResultDocument>.Filter.Eq(d => d.TaskId, taskId);
        var update = Builders<TaskResultDocument>
            .Update.Set(d => d.State, state.ToString())
            .SetOnInsert(d => d.TaskId, taskId)
            .SetOnInsert(d => d.CompletedAt, DateTime.UtcNow)
            .SetOnInsert(d => d.DurationMs, 0)
            .SetOnInsert(d => d.ExpiresAt, DateTime.UtcNow.Add(_options.DefaultExpiry));

        var options = new UpdateOptions { IsUpsert = true };

        await _collection!
            .UpdateOneAsync(filter, update, options, cancellationToken)
            .ConfigureAwait(false);

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

        var filter = Builders<TaskResultDocument>.Filter.And(
            Builders<TaskResultDocument>.Filter.Eq(d => d.TaskId, taskId),
            Builders<TaskResultDocument>.Filter.Or(
                Builders<TaskResultDocument>.Filter.Eq(d => d.ExpiresAt, null),
                Builders<TaskResultDocument>.Filter.Gt(d => d.ExpiresAt, DateTime.UtcNow)
            )
        );

        var projection = Builders<TaskResultDocument>.Projection.Include(d => d.State);
        var document = await _collection!
            .Find(filter)
            .Project<TaskResultDocument>(projection)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);

        if (document is null)
        {
            return null;
        }

        return Enum.TryParse<TaskState>(document.State, ignoreCase: true, out var state)
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

        // Cancel background tasks
        await _disposeCts.CancelAsync().ConfigureAwait(false);

        // Cancel all waiters
        foreach (var tcs in _waiters.Values)
        {
            tcs.TrySetCanceled();
        }
        _waiters.Clear();

        // Wait for change stream task
        if (_changeStreamTask is not null)
        {
            try
            {
                await _changeStreamTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        }

        _initLock.Dispose();
        _disposeCts.Dispose();

        _logger.LogInformation("MongoDB backend disposed");
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

            _collection = _database.GetCollection<TaskResultDocument>(_options.CollectionName);

            if (_options.AutoCreateIndexes)
            {
                await CreateIndexesAsync(cancellationToken).ConfigureAwait(false);
            }

            if (_options.UseChangeStreams)
            {
                _changeStreamTask = WatchChangeStreamAsync(_disposeCts.Token);
            }

            _initialized = true;
            _logger.LogInformation("MongoDB backend initialized");
        }
        finally
        {
            _initLock.Release();
        }
    }

    private async Task CreateIndexesAsync(CancellationToken cancellationToken)
    {
        var indexes = new List<CreateIndexModel<TaskResultDocument>>
        {
            // TTL index for automatic expiry
            new(
                Builders<TaskResultDocument>.IndexKeys.Ascending(d => d.ExpiresAt),
                new CreateIndexOptions { Name = "idx_expires_at", ExpireAfter = TimeSpan.Zero }
            ),
            // State index for queries
            new(
                Builders<TaskResultDocument>.IndexKeys.Ascending(d => d.State),
                new CreateIndexOptions { Name = "idx_state" }
            ),
        };

        await _collection!
            .Indexes.CreateManyAsync(indexes, cancellationToken)
            .ConfigureAwait(false);
        _logger.LogDebug("Created indexes on collection {CollectionName}", _options.CollectionName);
    }

    private async Task WatchChangeStreamAsync(CancellationToken cancellationToken)
    {
        var retryDelay = TimeSpan.FromSeconds(1);
        var maxRetryDelay = TimeSpan.FromMinutes(1);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var pipeline = new EmptyPipelineDefinition<
                    ChangeStreamDocument<TaskResultDocument>
                >().Match(change =>
                    change.OperationType == ChangeStreamOperationType.Insert
                    || change.OperationType == ChangeStreamOperationType.Replace
                    || change.OperationType == ChangeStreamOperationType.Update
                );

                using var cursor = await _collection!
                    .WatchAsync(pipeline, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);

                // Reset retry delay on successful connection
                retryDelay = TimeSpan.FromSeconds(1);
                _logger.LogDebug("Change stream connected successfully");

                await cursor
                    .ForEachAsync(
                        change =>
                        {
                            if (change.FullDocument is not null)
                            {
                                var taskId = change.FullDocument.TaskId;
                                if (_waiters.TryRemove(taskId, out var tcs))
                                {
                                    var result = change.FullDocument.ToTaskResult();
                                    tcs.TrySetResult(result);
                                }
                            }
                        },
                        cancellationToken
                    )
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Expected during shutdown
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Change stream error, will retry in {RetryDelay}",
                    retryDelay
                );

                try
                {
                    await Task.Delay(retryDelay, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                // Exponential backoff with max delay
                retryDelay = TimeSpan.FromTicks(
                    Math.Min(retryDelay.Ticks * 2, maxRetryDelay.Ticks)
                );
            }
        }
    }
}

/// <summary>
/// MongoDB document representation of a task result.
/// </summary>
internal sealed class TaskResultDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.String)]
    public string TaskId { get; set; } = string.Empty;

    [BsonElement("state")]
    public string State { get; set; } = string.Empty;

    [BsonElement("result")]
    [BsonIgnoreIfNull]
    public byte[]? Result { get; set; }

    [BsonElement("content_type")]
    [BsonIgnoreIfNull]
    public string? ContentType { get; set; }

    [BsonElement("exception")]
    [BsonIgnoreIfNull]
    public TaskExceptionDocument? Exception { get; set; }

    [BsonElement("completed_at")]
    public DateTime CompletedAt { get; set; }

    [BsonElement("duration_ms")]
    public long DurationMs { get; set; }

    [BsonElement("retries")]
    public int Retries { get; set; }

    [BsonElement("worker")]
    [BsonIgnoreIfNull]
    public string? Worker { get; set; }

    [BsonElement("metadata")]
    [BsonIgnoreIfNull]
    public Dictionary<string, object>? Metadata { get; set; }

    [BsonElement("expires_at")]
    [BsonIgnoreIfNull]
    public DateTime? ExpiresAt { get; set; }

    public static TaskResultDocument FromTaskResult(TaskResult result, DateTime expiresAt)
    {
        return new TaskResultDocument
        {
            TaskId = result.TaskId,
            State = result.State.ToString(),
            Result = result.Result,
            ContentType = result.ContentType,
            Exception = result.Exception is not null
                ? TaskExceptionDocument.FromTaskExceptionInfo(result.Exception)
                : null,
            CompletedAt = result.CompletedAt.UtcDateTime,
            DurationMs = (long)result.Duration.TotalMilliseconds,
            Retries = result.Retries,
            Worker = result.Worker,
            Metadata = result.Metadata?.ToDictionary(kv => kv.Key, kv => kv.Value),
            ExpiresAt = expiresAt,
        };
    }

    public TaskResult ToTaskResult()
    {
        var state = Enum.TryParse<TaskState>(State, ignoreCase: true, out var s)
            ? s
            : TaskState.Pending;

        return new TaskResult
        {
            TaskId = TaskId,
            State = state,
            Result = Result,
            ContentType = ContentType,
            Exception = Exception?.ToTaskExceptionInfo(),
            CompletedAt = new DateTimeOffset(CompletedAt, TimeSpan.Zero),
            Duration = TimeSpan.FromMilliseconds(DurationMs),
            Retries = Retries,
            Worker = Worker,
            Metadata = Metadata,
        };
    }
}

/// <summary>
/// MongoDB document representation of task exception info.
/// </summary>
internal sealed class TaskExceptionDocument
{
    [BsonElement("type")]
    public string Type { get; set; } = string.Empty;

    [BsonElement("message")]
    public string Message { get; set; } = string.Empty;

    [BsonElement("stack_trace")]
    [BsonIgnoreIfNull]
    public string? StackTrace { get; set; }

    [BsonElement("inner_exception")]
    [BsonIgnoreIfNull]
    public TaskExceptionDocument? InnerException { get; set; }

    public static TaskExceptionDocument FromTaskExceptionInfo(TaskExceptionInfo info)
    {
        return new TaskExceptionDocument
        {
            Type = info.Type,
            Message = info.Message,
            StackTrace = info.StackTrace,
            InnerException = info.InnerException is not null
                ? FromTaskExceptionInfo(info.InnerException)
                : null,
        };
    }

    public TaskExceptionInfo ToTaskExceptionInfo()
    {
        return new TaskExceptionInfo
        {
            Type = Type,
            Message = Message,
            StackTrace = StackTrace,
            InnerException = InnerException?.ToTaskExceptionInfo(),
        };
    }
}
