using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.Metrics;

/// <summary>
/// MongoDB implementation of <see cref="IQueueMetrics"/>.
/// </summary>
public sealed class MongoQueueMetrics : IQueueMetrics, IAsyncDisposable
{
    private readonly MongoQueueMetricsOptions _options;
    private readonly ILogger<MongoQueueMetrics> _logger;
    private readonly MongoClient _client;
    private readonly IMongoDatabase _database;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    private IMongoCollection<QueueMetricsDocument>? _metricsCollection;
    private IMongoCollection<RunningTaskDocument>? _runningTasksCollection;
    private bool _initialized;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoQueueMetrics"/> class.
    /// </summary>
    public MongoQueueMetrics(
        IOptions<MongoQueueMetricsOptions> options,
        ILogger<MongoQueueMetrics> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _client = new MongoClient(_options.ConnectionString);
        _database = _client.GetDatabase(_options.DatabaseName);
    }

    /// <inheritdoc />
    public async ValueTask<long> GetWaitingCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(queue);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var metrics = await GetOrCreateMetricsAsync(queue, cancellationToken).ConfigureAwait(false);
        return metrics?.WaitingCount ?? 0;
    }

    /// <inheritdoc />
    public async ValueTask<long> GetRunningCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(queue);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<RunningTaskDocument>.Filter.Eq(d => d.Queue, queue);
        return await _runningTasksCollection!
            .CountDocumentsAsync(filter, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<long> GetProcessedCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(queue);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var metrics = await GetOrCreateMetricsAsync(queue, cancellationToken).ConfigureAwait(false);
        return metrics?.ProcessedCount ?? 0;
    }

    /// <inheritdoc />
    public async ValueTask<int> GetConsumerCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(queue);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var metrics = await GetOrCreateMetricsAsync(queue, cancellationToken).ConfigureAwait(false);
        return metrics?.ConsumerCount ?? 0;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<string>> GetQueuesAsync(
        CancellationToken cancellationToken = default
    )
    {
        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var projection = Builders<QueueMetricsDocument>.Projection.Include(d => d.Queue);
        var documents = await _metricsCollection!
            .Find(FilterDefinition<QueueMetricsDocument>.Empty)
            .Project<QueueMetricsDocument>(projection)
            .ToListAsync(cancellationToken)
            .ConfigureAwait(false);

        return documents.Select(d => d.Queue).ToList();
    }

    /// <inheritdoc />
    public async ValueTask<QueueMetricsData> GetMetricsAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(queue);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var metrics = await GetOrCreateMetricsAsync(queue, cancellationToken).ConfigureAwait(false);
        var runningCount = await GetRunningCountAsync(queue, cancellationToken)
            .ConfigureAwait(false);

        return new QueueMetricsData
        {
            Queue = queue,
            WaitingCount = metrics?.WaitingCount ?? 0,
            RunningCount = runningCount,
            ProcessedCount = metrics?.ProcessedCount ?? 0,
            SuccessCount = metrics?.SuccessCount ?? 0,
            FailureCount = metrics?.FailureCount ?? 0,
            ConsumerCount = metrics?.ConsumerCount ?? 0,
            AverageDuration =
                metrics?.TotalDurationMs > 0 && metrics.ProcessedCount > 0
                    ? TimeSpan.FromMilliseconds(metrics.TotalDurationMs / metrics.ProcessedCount)
                    : null,
            LastEnqueuedAt =
                metrics?.LastEnqueuedAt.HasValue == true
                    ? new DateTimeOffset(metrics.LastEnqueuedAt.Value, TimeSpan.Zero)
                    : null,
            LastCompletedAt =
                metrics?.LastCompletedAt.HasValue == true
                    ? new DateTimeOffset(metrics.LastCompletedAt.Value, TimeSpan.Zero)
                    : null,
        };
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, QueueMetricsData>> GetAllMetricsAsync(
        CancellationToken cancellationToken = default
    )
    {
        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var queues = await GetQueuesAsync(cancellationToken).ConfigureAwait(false);
        var result = new Dictionary<string, QueueMetricsData>();

        foreach (var queue in queues)
        {
            result[queue] = await GetMetricsAsync(queue, cancellationToken).ConfigureAwait(false);
        }

        return result;
    }

    /// <inheritdoc />
    public async ValueTask RecordStartedAsync(
        string queue,
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(queue);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var runningTask = new RunningTaskDocument
        {
            TaskId = taskId,
            Queue = queue,
            StartedAt = DateTime.UtcNow,
        };

        var filter = Builders<RunningTaskDocument>.Filter.Eq(d => d.TaskId, taskId);
        var replaceOptions = new ReplaceOptions { IsUpsert = true };

        await _runningTasksCollection!
            .ReplaceOneAsync(filter, runningTask, replaceOptions, cancellationToken)
            .ConfigureAwait(false);

        // Decrement waiting count
        var metricsFilter = Builders<QueueMetricsDocument>.Filter.Eq(d => d.Queue, queue);
        var update = Builders<QueueMetricsDocument>.Update.Inc(d => d.WaitingCount, -1);

        await _metricsCollection!
            .UpdateOneAsync(metricsFilter, update, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        _logger.LogDebug("Recorded task {TaskId} started in queue {Queue}", taskId, queue);
    }

    /// <inheritdoc />
    public async ValueTask RecordCompletedAsync(
        string queue,
        string taskId,
        bool success,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(queue);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        // Remove from running tasks
        var runningFilter = Builders<RunningTaskDocument>.Filter.Eq(d => d.TaskId, taskId);
        await _runningTasksCollection!
            .DeleteOneAsync(runningFilter, cancellationToken)
            .ConfigureAwait(false);

        // Update metrics
        var metricsFilter = Builders<QueueMetricsDocument>.Filter.Eq(d => d.Queue, queue);
        var updateBuilder = Builders<QueueMetricsDocument>
            .Update.Inc(d => d.ProcessedCount, 1)
            .Inc(d => d.TotalDurationMs, duration.TotalMilliseconds)
            .Set(d => d.LastCompletedAt, DateTime.UtcNow);

        if (success)
        {
            updateBuilder = updateBuilder.Inc(d => d.SuccessCount, 1);
        }
        else
        {
            updateBuilder = updateBuilder.Inc(d => d.FailureCount, 1);
        }

        await _metricsCollection!
            .UpdateOneAsync(
                metricsFilter,
                updateBuilder,
                new UpdateOptions { IsUpsert = true },
                cancellationToken
            )
            .ConfigureAwait(false);

        _logger.LogDebug(
            "Recorded task {TaskId} completed in queue {Queue} (success={Success}, duration={Duration}ms)",
            taskId,
            queue,
            success,
            duration.TotalMilliseconds
        );
    }

    /// <inheritdoc />
    public async ValueTask RecordEnqueuedAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(queue);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<QueueMetricsDocument>.Filter.Eq(d => d.Queue, queue);
        var update = Builders<QueueMetricsDocument>
            .Update.Inc(d => d.WaitingCount, 1)
            .Set(d => d.LastEnqueuedAt, DateTime.UtcNow)
            .SetOnInsert(d => d.Queue, queue);

        await _metricsCollection!
            .UpdateOneAsync(
                filter,
                update,
                new UpdateOptions { IsUpsert = true },
                cancellationToken
            )
            .ConfigureAwait(false);

        _logger.LogDebug("Recorded message enqueued in queue {Queue}", queue);
    }

    private async Task<QueueMetricsDocument?> GetOrCreateMetricsAsync(
        string queue,
        CancellationToken cancellationToken
    )
    {
        var filter = Builders<QueueMetricsDocument>.Filter.Eq(d => d.Queue, queue);
        return await _metricsCollection!
            .Find(filter)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);
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

            _metricsCollection = _database.GetCollection<QueueMetricsDocument>(
                _options.MetricsCollectionName
            );
            _runningTasksCollection = _database.GetCollection<RunningTaskDocument>(
                _options.RunningTasksCollectionName
            );

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
        var runningTaskIndexes = new List<CreateIndexModel<RunningTaskDocument>>
        {
            new(
                Builders<RunningTaskDocument>.IndexKeys.Ascending(d => d.Queue),
                new CreateIndexOptions { Name = "idx_queue" }
            ),
        };

        await _runningTasksCollection!
            .Indexes.CreateManyAsync(runningTaskIndexes, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return ValueTask.CompletedTask;
        }

        _disposed = true;
        _initLock.Dispose();
        _logger.LogInformation("MongoDB queue metrics disposed");

        return ValueTask.CompletedTask;
    }
}

internal sealed class QueueMetricsDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.String)]
    public string Queue { get; set; } = string.Empty;

    [BsonElement("waiting_count")]
    public long WaitingCount { get; set; }

    [BsonElement("processed_count")]
    public long ProcessedCount { get; set; }

    [BsonElement("success_count")]
    public long SuccessCount { get; set; }

    [BsonElement("failure_count")]
    public long FailureCount { get; set; }

    [BsonElement("consumer_count")]
    public int ConsumerCount { get; set; }

    [BsonElement("total_duration_ms")]
    public double TotalDurationMs { get; set; }

    [BsonElement("last_enqueued_at")]
    [BsonIgnoreIfNull]
    public DateTime? LastEnqueuedAt { get; set; }

    [BsonElement("last_completed_at")]
    [BsonIgnoreIfNull]
    public DateTime? LastCompletedAt { get; set; }
}

internal sealed class RunningTaskDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.String)]
    public string TaskId { get; set; } = string.Empty;

    [BsonElement("queue")]
    public string Queue { get; set; } = string.Empty;

    [BsonElement("started_at")]
    public DateTime StartedAt { get; set; }
}
