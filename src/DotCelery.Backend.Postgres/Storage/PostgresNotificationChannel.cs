using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Channels;
using DotCelery.Core.Storage;
using Npgsql;

namespace DotCelery.Backend.Postgres.Storage;

/// <summary>
/// <see cref="INotificationChannel"/> over PostgreSQL LISTEN and NOTIFY.
/// </summary>
/// <remarks>
/// Each subscription holds a connection. If the connection is lost, the subscription ends
/// with an exception; callers poll as well, as the notification contract requires.
/// </remarks>
internal sealed class PostgresNotificationChannel : INotificationChannel
{
    // PostgreSQL rejects NOTIFY payloads of 8000 bytes or more
    private const int MaxMessageBytes = 7999;

    private readonly NpgsqlDataSource _dataSource;

    public PostgresNotificationChannel(NpgsqlDataSource dataSource)
    {
        _dataSource = dataSource;
    }

    public async ValueTask PublishAsync(
        string channel,
        string message,
        CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(channel);
        ArgumentNullException.ThrowIfNull(message);

        if (Encoding.UTF8.GetByteCount(message) > MaxMessageBytes)
        {
            throw new ArgumentException(
                $"Notification messages must be at most {MaxMessageBytes} bytes.",
                nameof(message)
            );
        }

        await using var command = _dataSource.CreateCommand("SELECT pg_notify(@channel, @message)");
        command.Parameters.AddWithValue("channel", ChannelName(channel));
        command.Parameters.AddWithValue("message", message);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async IAsyncEnumerable<string> SubscribeAsync(
        string channel,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        StorageGuard.ThrowIfInvalidName(channel);

        var name = ChannelName(channel);
        var messages = Channel.CreateUnbounded<string>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = true }
        );

        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);

        connection.Notification += (_, e) =>
        {
            if (e.Channel == name)
            {
                messages.Writer.TryWrite(e.Payload);
            }
        };

        await using (var listen = new NpgsqlCommand($"LISTEN \"{name}\"", connection))
        {
            await listen.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var receive = ReceiveAsync(connection, messages.Writer, stop.Token);

        try
        {
            await foreach (
                var message in messages.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false)
            )
            {
                yield return message;
            }
        }
        finally
        {
            await stop.CancelAsync().ConfigureAwait(false);
            await receive.ConfigureAwait(false);
        }
    }

    // Channel names must be identifiers of at most 63 bytes, so arbitrary names are hashed
    private static string ChannelName(string channel) =>
        "dotcelery_"
        + Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(channel)))[..32];

    private static async Task ReceiveAsync(
        NpgsqlConnection connection,
        ChannelWriter<string> writer,
        CancellationToken cancellationToken
    )
    {
        try
        {
            while (true)
            {
                await connection.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            writer.TryComplete();
        }
        catch (Exception ex)
        {
            writer.TryComplete(ex);
        }
    }
}
