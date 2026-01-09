using System.Runtime.CompilerServices;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Dashboard;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.Historical;

/// <summary>
/// MongoDB implementation of <see cref="IHistoricalDataStore"/>.
/// </summary>
public sealed class MongoHistoricalDataStore : IHistoricalDataStore
{
    private readonly MongoHistoricalDataStoreOptions _options;
    private readonly ILogger<MongoHistoricalDataStore> _logger;
    private readonly MongoClient _client;
    private readonly IMongoDatabase _database;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    private IMongoCollection<MetricsSnapshotDocument>? _collection;
    private bool _initialized;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoHistoricalDataStore"/> class.
    /// </summary>
    public MongoHistoricalDataStore(
        IOptions<MongoHistoricalDataStoreOptions> options,
        ILogger<MongoHistoricalDataStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _client = new MongoClient(_options.ConnectionString);
        _database = _client.GetDatabase(_options.DatabaseName);
    }

    /// <inheritdoc />
    public async ValueTask RecordMetricsAsync(
        MetricsSnapshot snapshot,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(snapshot);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var document = MetricsSnapshotDocument.FromSnapshot(snapshot, _options.RetentionPeriod);

        await _collection!
            .InsertOneAsync(document, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        _logger.LogDebug("Recorded metrics snapshot at {Timestamp}", snapshot.Timestamp);
    }

    /// <inheritdoc />
    public async ValueTask<AggregatedMetrics> GetMetricsAsync(
        DateTimeOffset from,
        DateTimeOffset until,
        MetricsGranularity granularity = MetricsGranularity.Hour,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<MetricsSnapshotDocument>.Filter.And(
            Builders<MetricsSnapshotDocument>.Filter.Gte(d => d.Timestamp, from.UtcDateTime),
            Builders<MetricsSnapshotDocument>.Filter.Lte(d => d.Timestamp, until.UtcDateTime)
        );

        var documents = await _collection!
            .Find(filter)
            .ToListAsync(cancellationToken)
            .ConfigureAwait(false);

        var totalProcessed = documents.Sum(d => d.SuccessCount + d.FailureCount);
        var successCount = documents.Sum(d => d.SuccessCount);
        var failureCount = documents.Sum(d => d.FailureCount);
        var retryCount = documents.Sum(d => d.RetryCount);
        var revokedCount = documents.Sum(d => d.RevokedCount);

        var avgExecutionMs = documents
            .Where(d => d.AverageExecutionTimeMs.HasValue)
            .Select(d => d.AverageExecutionTimeMs!.Value)
            .DefaultIfEmpty()
            .Average();

        var periodSeconds = (until - from).TotalSeconds;
        var tasksPerSecond = periodSeconds > 0 ? totalProcessed / periodSeconds : 0;

        return new AggregatedMetrics
        {
            From = from,
            To = until,
            Granularity = granularity,
            TotalProcessed = totalProcessed,
            SuccessCount = successCount,
            FailureCount = failureCount,
            RetryCount = retryCount,
            RevokedCount = revokedCount,
            AverageExecutionTime =
                avgExecutionMs > 0 ? TimeSpan.FromMilliseconds(avgExecutionMs) : null,
            TasksPerSecond = tasksPerSecond,
        };
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<MetricsDataPoint> GetTimeSeriesAsync(
        DateTimeOffset from,
        DateTimeOffset until,
        MetricsGranularity granularity = MetricsGranularity.Hour,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<MetricsSnapshotDocument>.Filter.And(
            Builders<MetricsSnapshotDocument>.Filter.Gte(d => d.Timestamp, from.UtcDateTime),
            Builders<MetricsSnapshotDocument>.Filter.Lte(d => d.Timestamp, until.UtcDateTime)
        );

        var sort = Builders<MetricsSnapshotDocument>.Sort.Ascending(d => d.Timestamp);

        using var cursor = await _collection!
            .Find(filter)
            .Sort(sort)
            .ToCursorAsync(cancellationToken)
            .ConfigureAwait(false);

        var granularitySeconds = GetGranularitySeconds(granularity);
        var buckets = new Dictionary<DateTime, List<MetricsSnapshotDocument>>();

        while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
        {
            foreach (var doc in cursor.Current)
            {
                var bucketTime = TruncateToGranularity(doc.Timestamp, granularitySeconds);
                if (!buckets.TryGetValue(bucketTime, out var bucket))
                {
                    bucket = [];
                    buckets[bucketTime] = bucket;
                }
                bucket.Add(doc);
            }
        }

        foreach (var kvp in buckets.OrderBy(k => k.Key))
        {
            var docs = kvp.Value;
            var successCount = docs.Sum(d => d.SuccessCount);
            var failureCount = docs.Sum(d => d.FailureCount);
            var retryCount = docs.Sum(d => d.RetryCount);
            var totalProcessed = successCount + failureCount;

            var avgExecutionMs = docs.Where(d => d.AverageExecutionTimeMs.HasValue)
                .Select(d => d.AverageExecutionTimeMs!.Value)
                .DefaultIfEmpty()
                .Average();

            var tasksPerSecond =
                granularitySeconds > 0 ? (double)totalProcessed / granularitySeconds : 0;

            yield return new MetricsDataPoint
            {
                Timestamp = new DateTimeOffset(kvp.Key, TimeSpan.Zero),
                SuccessCount = successCount,
                FailureCount = failureCount,
                RetryCount = retryCount,
                TasksPerSecond = tasksPerSecond,
                AverageExecutionTime =
                    avgExecutionMs > 0 ? TimeSpan.FromMilliseconds(avgExecutionMs) : null,
            };
        }
    }

    /// <inheritdoc />
    public async ValueTask<
        IReadOnlyDictionary<string, TaskMetricsSummary>
    > GetMetricsByTaskNameAsync(
        DateTimeOffset from,
        DateTimeOffset until,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var filter = Builders<MetricsSnapshotDocument>.Filter.And(
            Builders<MetricsSnapshotDocument>.Filter.Gte(d => d.Timestamp, from.UtcDateTime),
            Builders<MetricsSnapshotDocument>.Filter.Lte(d => d.Timestamp, until.UtcDateTime),
            Builders<MetricsSnapshotDocument>.Filter.Ne(d => d.TaskName, null)
        );

        var documents = await _collection!
            .Find(filter)
            .ToListAsync(cancellationToken)
            .ConfigureAwait(false);

        var groupedByTask = documents.Where(d => d.TaskName is not null).GroupBy(d => d.TaskName!);

        var result = new Dictionary<string, TaskMetricsSummary>();

        foreach (var group in groupedByTask)
        {
            var taskName = group.Key;
            var docs = group.ToList();

            var successCount = docs.Sum(d => d.SuccessCount);
            var failureCount = docs.Sum(d => d.FailureCount);
            var totalCount = successCount + failureCount;

            var executionTimes = docs.Where(d => d.AverageExecutionTimeMs.HasValue)
                .Select(d => d.AverageExecutionTimeMs!.Value)
                .ToList();

            double? avgMs = null;
            double? minMs = null;
            double? maxMs = null;

            if (executionTimes.Count > 0)
            {
                avgMs = executionTimes.Average();
                minMs = executionTimes.Min();
                maxMs = executionTimes.Max();
            }

            result[taskName] = new TaskMetricsSummary
            {
                TaskName = taskName,
                TotalCount = totalCount,
                SuccessCount = successCount,
                FailureCount = failureCount,
                AverageExecutionTime = avgMs.HasValue
                    ? TimeSpan.FromMilliseconds(avgMs.Value)
                    : null,
                MinExecutionTime = minMs.HasValue ? TimeSpan.FromMilliseconds(minMs.Value) : null,
                MaxExecutionTime = maxMs.HasValue ? TimeSpan.FromMilliseconds(maxMs.Value) : null,
            };
        }

        return result;
    }

    /// <inheritdoc />
    public async ValueTask<long> ApplyRetentionAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var cutoff = DateTime.UtcNow - _options.RetentionPeriod;
        var filter = Builders<MetricsSnapshotDocument>.Filter.Lt(d => d.Timestamp, cutoff);

        var result = await _collection!
            .DeleteManyAsync(filter, cancellationToken)
            .ConfigureAwait(false);

        if (result.DeletedCount > 0)
        {
            _logger.LogInformation(
                "Applied retention policy, removed {Count} historical records",
                result.DeletedCount
            );
        }

        return result.DeletedCount;
    }

    /// <inheritdoc />
    public async ValueTask<long> GetSnapshotCountAsync(
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        return await _collection!
            .CountDocumentsAsync(
                FilterDefinition<MetricsSnapshotDocument>.Empty,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _initLock.Dispose();
        _logger.LogInformation("MongoDB historical data store disposed");
    }

    private static int GetGranularitySeconds(MetricsGranularity granularity)
    {
        return granularity switch
        {
            MetricsGranularity.Minute => 60,
            MetricsGranularity.Hour => 3600,
            MetricsGranularity.Day => 86400,
            MetricsGranularity.Week => 604800,
            _ => 3600,
        };
    }

    private static DateTime TruncateToGranularity(DateTime timestamp, int granularitySeconds)
    {
        var unixSeconds = (long)(timestamp - DateTime.UnixEpoch).TotalSeconds;
        var truncatedSeconds = unixSeconds - (unixSeconds % granularitySeconds);
        return DateTime.UnixEpoch.AddSeconds(truncatedSeconds);
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

            _collection = _database.GetCollection<MetricsSnapshotDocument>(_options.CollectionName);

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
        var indexes = new List<CreateIndexModel<MetricsSnapshotDocument>>
        {
            new(
                Builders<MetricsSnapshotDocument>.IndexKeys.Ascending(d => d.Timestamp),
                new CreateIndexOptions { Name = "idx_timestamp" }
            ),
            new(
                Builders<MetricsSnapshotDocument>.IndexKeys.Ascending(d => d.ExpiresAt),
                new CreateIndexOptions { Name = "idx_expires_at", ExpireAfter = TimeSpan.Zero }
            ),
            new(
                Builders<MetricsSnapshotDocument>.IndexKeys.Ascending(d => d.TaskName),
                new CreateIndexOptions { Name = "idx_task_name", Sparse = true }
            ),
            new(
                Builders<MetricsSnapshotDocument>.IndexKeys.Ascending(d => d.Queue),
                new CreateIndexOptions { Name = "idx_queue", Sparse = true }
            ),
        };

        await _collection!
            .Indexes.CreateManyAsync(indexes, cancellationToken)
            .ConfigureAwait(false);
    }
}

internal sealed class MetricsSnapshotDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public string? Id { get; set; }

    [BsonElement("timestamp")]
    public DateTime Timestamp { get; set; }

    [BsonElement("task_name")]
    [BsonIgnoreIfNull]
    public string? TaskName { get; set; }

    [BsonElement("queue")]
    [BsonIgnoreIfNull]
    public string? Queue { get; set; }

    [BsonElement("success_count")]
    public long SuccessCount { get; set; }

    [BsonElement("failure_count")]
    public long FailureCount { get; set; }

    [BsonElement("retry_count")]
    public long RetryCount { get; set; }

    [BsonElement("revoked_count")]
    public long RevokedCount { get; set; }

    [BsonElement("average_execution_time_ms")]
    [BsonIgnoreIfNull]
    public double? AverageExecutionTimeMs { get; set; }

    [BsonElement("expires_at")]
    public DateTime ExpiresAt { get; set; }

    public static MetricsSnapshotDocument FromSnapshot(MetricsSnapshot snapshot, TimeSpan retention)
    {
        return new MetricsSnapshotDocument
        {
            Timestamp = snapshot.Timestamp.UtcDateTime,
            TaskName = snapshot.TaskName,
            Queue = snapshot.Queue,
            SuccessCount = snapshot.SuccessCount,
            FailureCount = snapshot.FailureCount,
            RetryCount = snapshot.RetryCount,
            RevokedCount = snapshot.RevokedCount,
            AverageExecutionTimeMs = snapshot.AverageExecutionTime?.TotalMilliseconds,
            ExpiresAt = DateTime.UtcNow.Add(retention),
        };
    }
}
