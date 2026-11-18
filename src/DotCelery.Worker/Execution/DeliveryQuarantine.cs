using System.Text.Json;
using DotCelery.Core.Abstractions;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage;
using DotCelery.Core.Storage.Stores;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Execution;

internal sealed class DeliveryQuarantine(
    IServiceProvider services,
    IResultBackend resultBackend,
    TimeProvider clock
)
{
    public async ValueTask WriteAsync(BrokerMessage delivery, CancellationToken cancellationToken)
    {
        // Unauthenticated deliveries must never write into a tenant's namespace. This
        // operator-only collection is intentionally outside tenant dashboard/query stores.
        var storage =
            services.GetService<IStorageProvider>()
            ?? (resultBackend as IStorageBackedStore)?.Storage
            ?? throw new InvalidOperationException(
                "Durable storage is required to quarantine an untrusted delivery."
            );
        var options =
            services.GetService<IOptions<StorageStoreOptions>>()?.Value
            ?? new StorageStoreOptions();
        var deadLetters =
            services.GetService<IOptions<DeadLetterOptions>>()?.Value ?? new DeadLetterOptions();
        var now = clock.GetUtcNow();
        var record = new DeadLetterMessage
        {
            Id = Guid.NewGuid().ToString("N"),
            TaskId = delivery.Message.Id,
            TaskName = delivery.Message.Task,
            Queue = delivery.Queue,
            Reason = DeadLetterReason.Rejected,
            OriginalMessage =
                delivery.RawBody
                ?? JsonSerializer.SerializeToUtf8Bytes(
                    delivery.Message,
                    DotCeleryJsonContext.Default.TaskMessage
                ),
            ExceptionMessage = "Tenant authentication or delivery policy rejected this message.",
            Timestamp = now,
            ExpiresAt = now + deadLetters.RetentionPeriod,
        };
        await storage
            .Documents.UpsertAsync(
                options.Prefix + ".quarantine",
                record.Id,
                record,
                DotCeleryJsonContext.Default.DeadLetterMessage,
                new DocumentWriteOptions
                {
                    TimeToLive = deadLetters.RetentionPeriod,
                    SortKey = now,
                },
                cancellationToken
            )
            .ConfigureAwait(false);
    }
}
