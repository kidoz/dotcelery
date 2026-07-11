using DotCelery.Core.Security;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace DotCelery.Tests.Unit.Security;

public sealed class InsecureDefaultsGuardTests
{
    [Fact]
    public void Logs_Warning_When_Production_Using_DevDefault()
    {
        var logger = Substitute.For<ILogger>();
        var env = Substitute.For<IHostEnvironment>();
        env.EnvironmentName.Returns("Production");

        InsecureDefaultsGuard.WarnIfDevelopmentDefault(
            logger,
            env,
            componentName: "Test broker",
            configuredValue: "amqp://guest:guest@localhost:5672/",
            developmentDefault: "amqp://guest:guest@localhost:5672/"
        );

        logger
            .Received(1)
            .Log(
                LogLevel.Warning,
                Arg.Any<EventId>(),
                Arg.Any<object>(),
                Arg.Any<Exception>(),
                Arg.Any<Func<object, Exception?, string>>()
            );
    }

    [Fact]
    public void DoesNot_Log_When_Production_With_Custom_Value()
    {
        var logger = Substitute.For<ILogger>();
        var env = Substitute.For<IHostEnvironment>();
        env.EnvironmentName.Returns("Production");

        InsecureDefaultsGuard.WarnIfDevelopmentDefault(
            logger,
            env,
            componentName: "Test broker",
            configuredValue: "amqps://prod-user:secret@rabbit.internal:5671/",
            developmentDefault: "amqp://guest:guest@localhost:5672/"
        );

        logger
            .DidNotReceive()
            .Log(
                Arg.Any<LogLevel>(),
                Arg.Any<EventId>(),
                Arg.Any<object>(),
                Arg.Any<Exception>(),
                Arg.Any<Func<object, Exception?, string>>()
            );
    }

    [Fact]
    public void DoesNot_Log_When_Development_Even_With_DevDefault()
    {
        var logger = Substitute.For<ILogger>();
        var env = Substitute.For<IHostEnvironment>();
        env.EnvironmentName.Returns("Development");

        InsecureDefaultsGuard.WarnIfDevelopmentDefault(
            logger,
            env,
            componentName: "Test broker",
            configuredValue: "amqp://guest:guest@localhost:5672/",
            developmentDefault: "amqp://guest:guest@localhost:5672/"
        );

        logger
            .DidNotReceive()
            .Log(
                Arg.Any<LogLevel>(),
                Arg.Any<EventId>(),
                Arg.Any<object>(),
                Arg.Any<Exception>(),
                Arg.Any<Func<object, Exception?, string>>()
            );
    }

    [Fact]
    public void Logs_Warning_When_Staging_Using_DevDefault()
    {
        // Anything that isn't "Development" should be considered risky.
        var logger = Substitute.For<ILogger>();
        var env = Substitute.For<IHostEnvironment>();
        env.EnvironmentName.Returns("Staging");

        InsecureDefaultsGuard.WarnIfDevelopmentDefault(
            logger,
            env,
            componentName: "Redis backend",
            configuredValue: "localhost:6379",
            developmentDefault: "localhost:6379"
        );

        logger
            .Received(1)
            .Log(
                LogLevel.Warning,
                Arg.Any<EventId>(),
                Arg.Any<object>(),
                Arg.Any<Exception>(),
                Arg.Any<Func<object, Exception?, string>>()
            );
    }

    [Fact]
    public void Throws_When_Required_Arguments_Missing()
    {
        var logger = Substitute.For<ILogger>();
        var env = Substitute.For<IHostEnvironment>();
        env.EnvironmentName.Returns("Production");

        Assert.Throws<ArgumentException>(() =>
            InsecureDefaultsGuard.WarnIfDevelopmentDefault(
                logger,
                env,
                componentName: string.Empty,
                configuredValue: "x",
                developmentDefault: "x"
            )
        );

        Assert.Throws<ArgumentException>(() =>
            InsecureDefaultsGuard.WarnIfDevelopmentDefault(
                logger,
                env,
                componentName: "x",
                configuredValue: "x",
                developmentDefault: string.Empty
            )
        );
    }
}
