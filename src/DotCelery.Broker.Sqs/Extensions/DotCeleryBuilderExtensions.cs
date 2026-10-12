using DotCelery.Core.Abstractions;
using DotCelery.Core.Extensions;
using Microsoft.Extensions.DependencyInjection;

namespace DotCelery.Broker.Sqs.Extensions;

/// <summary>
/// Extension methods for configuring the Amazon SQS broker with DotCeleryBuilder.
/// </summary>
public static class DotCeleryBuilderExtensions
{
    /// <summary>
    /// Configures the Amazon SQS message broker.
    /// </summary>
    /// <param name="builder">The DotCelery builder.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The builder for chaining.</returns>
    public static DotCeleryBuilder UseSqs(
        this DotCeleryBuilder builder,
        Action<SqsBrokerOptions>? configure = null
    )
    {
        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        // Remove any existing broker registration
        var existingBroker = builder.Services.FirstOrDefault(d =>
            d.ServiceType == typeof(IMessageBroker)
        );
        if (existingBroker is not null)
        {
            builder.Services.Remove(existingBroker);
        }

        builder.Services.AddSingleton<IMessageBroker, SqsBroker>();

        return builder;
    }
}
