using DotCelery.Core.Abstractions;
using DotCelery.Core.Extensions;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Unit.Serialization;

public sealed class SerializerRegistrationTests
{
    [Fact]
    public void DefaultSerializer_UsesTheOptionsRegisteredInDependencyInjection()
    {
        var services = new ServiceCollection();
        _ = new DotCeleryBuilder(services);
        services.Configure<JsonMessageSerializerOptions>(options =>
        {
            options.EnforceDeserializationTypeAllowlist = true;
            options.AllowedDeserializationTypes = new HashSet<Type> { typeof(AllowedInput) };
        });

        using var provider = services.BuildServiceProvider();
        var serializer = provider.GetRequiredService<IMessageSerializer>();

        // The allowlist only takes effect when the serializer reads it from DI
        Assert.Throws<InvalidOperationException>(() =>
            serializer.Deserialize<DisallowedInput>("{}"u8)
        );
    }

    [Fact]
    public void DefaultSerializer_WithoutOptions_DeserializesAnyType()
    {
        var services = new ServiceCollection();
        _ = new DotCeleryBuilder(services);

        using var provider = services.BuildServiceProvider();
        var serializer = provider.GetRequiredService<IMessageSerializer>();

        serializer.Deserialize<DisallowedInput>("{}"u8);
    }

    private sealed class AllowedInput { }

    private sealed class DisallowedInput { }
}
