using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace DotCelery.Backend.Postgres.Revocation;

/// <summary>
/// PostgreSQL implementation of <see cref="IRevocationStore"/>.
/// Uses LISTEN/NOTIFY for real-time revocation events.
/// </summary>
public sealed class PostgresRevocationStore : IRevocationStore
{
    private readonly PostgresRevocationStoreOptions _options;
    private readonly ILogger<PostgresRevocationStore> _logger;
    private readonly NpgsqlDataSource _dataSource;
    private readonly Channel<RevocationEvent> _eventChannel;
    private readonly CancellationTokenSource _listenerCts = new();

    private NpgsqlConnection? _listenerConnection;
    private Task? _listenerTask;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresRevocationStore"/> class.
    /// </summary>
    public PostgresRevocationStore(
        IOptions<PostgresRevocationStoreOptions> options,
        IPostgresDataSourceProvider dataSources,
        ILogger<PostgresRevocationStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _dataSource = dataSources.GetDataSource(_options.ConnectionString);
        _eventChannel = Channel.CreateUnbounded<RevocationEvent>();
    }

    /// <inheritdoc />
    public async ValueTask RevokeAsync(
        string taskId,
        RevokeOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        options ??= new RevokeOptions();
        var now = DateTimeOffset.UtcNow;
        var expiresAt = now.Add(options.Expiry ?? _options.DefaultRetention);

        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(cancellationToken)
            .ConfigureAwait(false);

        try
        {
            var sql = $"""
                INSERT INTO {_options.Schema}.{_options.TableName}
                    (task_id, terminate, signal, revoked_at, expires_at)
                VALUES
                    (@taskId, @terminate, @signal, @revokedAt, @expiresAt)
                ON CONFLICT (task_id) DO UPDATE SET
                    terminate = @terminate,
                    signal = @signal,
                    revoked_at = @revokedAt,
                    expires_at = @expiresAt
                """;

            await using var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("taskId", taskId);
            cmd.Parameters.AddWithValue("terminate", options.Terminate);
            cmd.Parameters.AddWithValue("signal", options.Signal.ToString());
            cmd.Parameters.AddWithValue("revokedAt", now.UtcDateTime);
            cmd.Parameters.AddWithValue("expiresAt", expiresAt.UtcDateTime);

            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            // Send notification
            var payload = JsonSerializer.Serialize(
                new
                {
                    TaskId = taskId,
                    Terminate = options.Terminate,
                    Signal = options.Signal.ToString(),
                    Timestamp = now,
                }
            );

            await using var notifyCmd = connection.CreateCommand();
            notifyCmd.Transaction = transaction;
            notifyCmd.CommandText = "SELECT pg_notify(@channel, @payload)";
            notifyCmd.Parameters.AddWithValue("channel", _options.NotifyChannel);
            notifyCmd.Parameters.AddWithValue("payload", payload);
            await notifyCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }

        _logger.LogInformation("Revoked task {TaskId}", taskId);
    }

    /// <inheritdoc />
    public async ValueTask RevokeAsync(
        IEnumerable<string> taskIds,
        RevokeOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(taskIds);

        foreach (var taskId in taskIds)
        {
            await RevokeAsync(taskId, options, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsRevokedAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        var sql = $"""
            SELECT EXISTS(
                SELECT 1 FROM {_options.Schema}.{_options.TableName}
                WHERE task_id = @taskId AND expires_at > NOW()
            )
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("taskId", taskId);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is bool b && b;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> GetRevokedTaskIdsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var sql = $"""
            SELECT task_id FROM {_options.Schema}.{_options.TableName}
            WHERE expires_at > NOW()
            ORDER BY revoked_at DESC
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            yield return reader.GetString(0);
        }
    }

    /// <inheritdoc />
    public async ValueTask<long> CleanupAsync(
        TimeSpan maxAge,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var cutoff = DateTimeOffset.UtcNow - maxAge;

        var sql = $"""
            DELETE FROM {_options.Schema}.{_options.TableName}
            WHERE revoked_at < @cutoff OR expires_at < NOW()
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("cutoff", cutoff.UtcDateTime);

        var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        if (deleted > 0)
        {
            _logger.LogInformation("Cleaned up {Count} expired revocations", deleted);
        }

        return deleted;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RevocationEvent> SubscribeAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await StartListenerAsync(cancellationToken).ConfigureAwait(false);

        await foreach (var evt in _eventChannel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return evt;
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

        await _listenerCts.CancelAsync().ConfigureAwait(false);
        _eventChannel.Writer.TryComplete();

        if (_listenerTask is not null)
        {
            try
            {
                await _listenerTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        }

        if (_listenerConnection is not null)
        {
            await _listenerConnection.DisposeAsync().ConfigureAwait(false);
        }

        _listenerCts.Dispose();

        _logger.LogInformation("PostgreSQL revocation store disposed");
    }

    private async Task StartListenerAsync(CancellationToken cancellationToken)
    {
        if (_listenerTask is not null)
        {
            return;
        }

        _listenerConnection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);

        _listenerConnection.Notification += (_, e) =>
        {
            if (e.Channel == _options.NotifyChannel)
            {
                try
                {
                    var payload = JsonSerializer.Deserialize<RevocationPayload>(e.Payload);
                    if (payload is not null)
                    {
                        var signal = Enum.TryParse<CancellationSignal>(payload.Signal, out var s)
                            ? s
                            : CancellationSignal.Graceful;

                        var evt = new RevocationEvent
                        {
                            TaskId = payload.TaskId,
                            Options = new RevokeOptions
                            {
                                Terminate = payload.Terminate,
                                Signal = signal,
                            },
                            Timestamp = payload.Timestamp,
                        };
                        _eventChannel.Writer.TryWrite(evt);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to deserialize revocation notification");
                }
            }
        };

        await using var cmd = _listenerConnection.CreateCommand();
        cmd.CommandText = $"LISTEN {_options.NotifyChannel}";
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _listenerTask = Task.Run(
            async () =>
            {
                try
                {
                    while (!_listenerCts.Token.IsCancellationRequested)
                    {
                        await _listenerConnection
                            .WaitAsync(_listenerCts.Token)
                            .ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException)
                {
                    // Expected
                }
            },
            _listenerCts.Token
        );
    }

    private sealed record RevocationPayload
    {
        public required string TaskId { get; init; }
        public bool Terminate { get; init; }
        public string Signal { get; init; } = "Graceful";
        public DateTimeOffset Timestamp { get; init; }
    }
}
