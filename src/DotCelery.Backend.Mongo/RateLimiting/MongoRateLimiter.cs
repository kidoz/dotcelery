using DotCelery.Core.Abstractions;
using DotCelery.Core.RateLimiting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace DotCelery.Backend.Mongo.RateLimiting;

/// <summary>
/// MongoDB implementation of <see cref="IRateLimiter"/>.
/// Uses a sliding window algorithm with MongoDB atomic operations.
/// </summary>
public sealed class MongoRateLimiter : IRateLimiter
{
    private readonly MongoRateLimiterOptions _options;
    private readonly ILogger<MongoRateLimiter> _logger;
    private readonly MongoClient _client;
    private readonly IMongoDatabase _database;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    private IMongoCollection<RateLimitDocument>? _collection;
    private bool _initialized;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoRateLimiter"/> class.
    /// </summary>
    public MongoRateLimiter(
        IOptions<MongoRateLimiterOptions> options,
        ILogger<MongoRateLimiter> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _client = new MongoClient(_options.ConnectionString);
        _database = _client.GetDatabase(_options.DatabaseName);
    }

    /// <inheritdoc />
    public async ValueTask<RateLimitLease> TryAcquireAsync(
        string resourceKey,
        RateLimitPolicy policy,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(resourceKey);
        ArgumentNullException.ThrowIfNull(policy);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var now = DateTimeOffset.UtcNow;
        var windowStart = now - policy.Window;
        var windowEnd = now + policy.Window;

        // Create a composite key for resource + window size
        var documentId = $"{resourceKey}:{(int)policy.Window.TotalSeconds}";

        // Clean up old timestamps and add new one if under limit - atomic operation
        var filter = Builders<RateLimitDocument>.Filter.Eq(d => d.Id, documentId);

        // First, get the current document
        var currentDoc = await _collection!
            .Find(filter)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);

        // Clean old timestamps from in-memory list, then update
        if (currentDoc is not null)
        {
            var validTimestamps = currentDoc
                .Timestamps.Where(t => t > windowStart.UtcDateTime)
                .ToList();

            var cleanUpdate = Builders<RateLimitDocument>.Update.Set(
                d => d.Timestamps,
                validTimestamps
            );
            await _collection!
                .UpdateOneAsync(filter, cleanUpdate, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            // Refresh the doc
            currentDoc = await _collection!
                .Find(filter)
                .FirstOrDefaultAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        var currentCount = currentDoc?.Timestamps.Count(t => t > windowStart.UtcDateTime) ?? 0;

        if (currentCount >= policy.Limit)
        {
            // Rate limited
            var oldestInWindow =
                currentDoc
                    ?.Timestamps.Where(t => t > windowStart.UtcDateTime)
                    .OrderBy(t => t)
                    .FirstOrDefault()
                ?? DateTime.UtcNow;

            var retryAfter = oldestInWindow.Add(policy.Window) - DateTime.UtcNow;
            if (retryAfter < TimeSpan.Zero)
            {
                retryAfter = TimeSpan.FromMilliseconds(100);
            }

            _logger.LogDebug(
                "Rate limit exceeded for {ResourceKey}: {Current}/{Limit}",
                resourceKey,
                currentCount,
                policy.Limit
            );

            return RateLimitLease.RateLimited(retryAfter, now + retryAfter);
        }

        // Add the timestamp
        var pushUpdate = Builders<RateLimitDocument>
            .Update.Push(d => d.Timestamps, DateTime.UtcNow)
            .SetOnInsert(d => d.ResourceKey, resourceKey)
            .SetOnInsert(d => d.WindowSeconds, (int)policy.Window.TotalSeconds);

        await _collection!
            .UpdateOneAsync(
                filter,
                pushUpdate,
                new UpdateOptions { IsUpsert = true },
                cancellationToken
            )
            .ConfigureAwait(false);

        var remaining = policy.Limit - currentCount - 1;
        _logger.LogDebug(
            "Acquired rate limit permit for {ResourceKey}: {Remaining}/{Limit} remaining",
            resourceKey,
            remaining,
            policy.Limit
        );

        return RateLimitLease.Acquired(Math.Max(0, remaining), windowEnd);
    }

    /// <inheritdoc />
    public async ValueTask<TimeSpan?> GetRetryAfterAsync(
        string resourceKey,
        RateLimitPolicy policy,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(resourceKey);
        ArgumentNullException.ThrowIfNull(policy);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var usage = await GetUsageAsync(resourceKey, policy, cancellationToken)
            .ConfigureAwait(false);

        if (!usage.IsLimited)
        {
            return null;
        }

        return usage.ResetAt - DateTimeOffset.UtcNow;
    }

    /// <inheritdoc />
    public async ValueTask<RateLimitUsage> GetUsageAsync(
        string resourceKey,
        RateLimitPolicy policy,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(resourceKey);
        ArgumentNullException.ThrowIfNull(policy);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var now = DateTimeOffset.UtcNow;
        var windowStart = now - policy.Window;
        var documentId = $"{resourceKey}:{(int)policy.Window.TotalSeconds}";

        var filter = Builders<RateLimitDocument>.Filter.Eq(d => d.Id, documentId);
        var document = await _collection!
            .Find(filter)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);

        var used = document?.Timestamps.Count(t => t > windowStart.UtcDateTime) ?? 0;

        var oldestInWindow = document
            ?.Timestamps.Where(t => t > windowStart.UtcDateTime)
            .OrderBy(t => t)
            .FirstOrDefault();

        var resetAt = oldestInWindow.HasValue
            ? new DateTimeOffset(oldestInWindow.Value, TimeSpan.Zero) + policy.Window
            : now + policy.Window;

        return new RateLimitUsage
        {
            Used = used,
            Limit = policy.Limit,
            ResetAt = resetAt,
        };
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
        _logger.LogInformation("MongoDB rate limiter disposed");
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

            _collection = _database.GetCollection<RateLimitDocument>(_options.CollectionName);

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
        var indexes = new List<CreateIndexModel<RateLimitDocument>>
        {
            new(
                Builders<RateLimitDocument>.IndexKeys.Ascending(d => d.ResourceKey),
                new CreateIndexOptions { Name = "idx_resource_key" }
            ),
        };

        await _collection!
            .Indexes.CreateManyAsync(indexes, cancellationToken)
            .ConfigureAwait(false);
    }
}

internal sealed class RateLimitDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.String)]
    public string Id { get; set; } = string.Empty;

    [BsonElement("resource_key")]
    public string ResourceKey { get; set; } = string.Empty;

    [BsonElement("window_seconds")]
    public int WindowSeconds { get; set; }

    [BsonElement("timestamps")]
    public List<DateTime> Timestamps { get; set; } = [];
}
