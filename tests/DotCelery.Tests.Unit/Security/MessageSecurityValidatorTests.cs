using System.Security.Cryptography;
using System.Text;
using DotCelery.Core.Extensions;
using DotCelery.Core.Models;
using DotCelery.Core.Security;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;

namespace DotCelery.Tests.Unit.Security;

/// <summary>
/// Tests for message signing in <see cref="MessageSecurityValidator"/>.
/// </summary>
public class MessageSecurityValidatorTests
{
    [Fact]
    public void SignAndVerify_ConcurrentCalls_ProduceCorrectSignatures()
    {
        // The validator is a singleton shared by publishers and consumers, so signing
        // must be safe to call from many threads at once.
        var key = RandomNumberGenerator.GetBytes(32);
        using var validator = CreateValidator(key);

        var payloads = Enumerable
            .Range(0, 2_000)
            .Select(i => Encoding.UTF8.GetBytes($"payload-{i}-{new string('x', i % 512)}"))
            .ToArray();
        var mismatches = 0;

        Parallel.For(
            0,
            payloads.Length,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = Math.Max(4, Environment.ProcessorCount),
            },
            i =>
            {
                var expected = Convert.ToBase64String(HMACSHA256.HashData(key, payloads[i]));
                var signature = validator.Sign(payloads[i]);

                if (
                    signature is null
                    || signature != expected
                    || !validator.VerifySignature(payloads[i], signature)
                )
                {
                    Interlocked.Increment(ref mismatches);
                }
            }
        );

        Assert.Equal(0, mismatches);
    }

    [Fact]
    public void Sign_AfterDispose_Throws()
    {
        var validator = CreateValidator(RandomNumberGenerator.GetBytes(32));
        validator.Dispose();

        Assert.Throws<ObjectDisposedException>(() => validator.Sign([1, 2, 3]));
    }

    [Fact]
    public void Sign_SigningDisabled_ReturnsNull()
    {
        using var validator = new MessageSecurityValidator(
            Options.Create(new MessageSecurityOptions()),
            NullLogger<MessageSecurityValidator>.Instance
        );

        Assert.Null(validator.Sign([1, 2, 3]));
    }

    [Fact]
    public void Validate_MessageOlderThanTheMaximumAge_IsRefused()
    {
        var now = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        using var validator = CreateValidator(maxMessageAge: TimeSpan.FromMinutes(5), now: now);
        var message = CreateMessage(now.AddMinutes(-6));

        var result = validator.Validate(message);

        Assert.False(result.IsValid);
        Assert.Equal(MessageValidationError.MessageExpired, result.ErrorCode);
    }

    [Fact]
    public void Validate_MessageWithinTheMaximumAge_IsAccepted()
    {
        var now = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        using var validator = CreateValidator(maxMessageAge: TimeSpan.FromMinutes(5), now: now);

        Assert.True(validator.Validate(CreateMessage(now.AddMinutes(-1))).IsValid);
    }

    [Fact]
    public void Validate_WithoutAMaximumAge_AcceptsAnOldMessage()
    {
        var now = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        using var validator = CreateValidator(now: now);

        // A redelivered message keeps the timestamp it was sent with
        Assert.True(validator.Validate(CreateMessage(now.AddDays(-1))).IsValid);
    }

    [Fact]
    public void VerifySignature_RotationAcceptsOldAndLegacySignaturesUntilTheKeyIsRemoved()
    {
        var oldKey = MessageSecurityOptions.GenerateKey();
        using var old = CreateValidator(oldKey);
        using var oldWithId = new MessageSecurityValidator(
            Options.Create(
                new MessageSecurityOptions
                {
                    EnableMessageSigning = true,
                    SigningKey = oldKey,
                    SigningKeyId = "old",
                }
            ),
            NullLogger<MessageSecurityValidator>.Instance
        );
        var payload = Encoding.UTF8.GetBytes("task");
        var oldSignature = Assert.IsType<string>(oldWithId.Sign(payload));
        var legacySignature = Assert.IsType<string>(old.Sign(payload));
        var options = new MessageSecurityOptions
        {
            EnableMessageSigning = true,
            SigningKey = MessageSecurityOptions.GenerateKey(),
            SigningKeyId = "new",
            LegacyVerificationKeyId = "old",
            AdditionalVerificationKeys = { ["old"] = oldKey },
        };
        using var overlapping = new MessageSecurityValidator(
            Options.Create(options),
            NullLogger<MessageSecurityValidator>.Instance
        );
        var newSignature = Assert.IsType<string>(overlapping.Sign(payload));
        Assert.StartsWith("new:", newSignature, StringComparison.Ordinal);
        Assert.True(overlapping.VerifySignature(payload, oldSignature));
        Assert.True(overlapping.VerifySignature(payload, legacySignature));

        options.AdditionalVerificationKeys.Clear();
        using var rotated = new MessageSecurityValidator(
            Options.Create(options),
            NullLogger<MessageSecurityValidator>.Instance
        );
        Assert.True(rotated.VerifySignature(payload, newSignature));
        Assert.False(rotated.VerifySignature(payload, oldSignature));
        Assert.False(rotated.VerifySignature(payload, legacySignature));
    }

    [Fact]
    public void Transport_ReceivesSignedDeliveryWhenVerificationIsDisabled()
    {
        using var signer = CreateValidator(MessageSecurityOptions.GenerateKey());
        using var receiver = CreateValidator();
        var payload = Encoding.UTF8.GetBytes("task");
        var signature = Assert.IsType<string>(signer.Sign(payload));
        Assert.True(receiver.IsSignatureValid(payload, signature));
        Assert.False(receiver.VerifySignature(payload, signature));
    }

    [Theory]
    [InlineData(5, true)]
    [InlineData(6, false)]
    public void Validate_ChecksFutureClockSkew(int minutes, bool accepted)
    {
        var now = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        using var validator = CreateValidator(maxMessageAge: TimeSpan.FromMinutes(10), now: now);
        Assert.Equal(accepted, validator.Validate(CreateMessage(now.AddMinutes(minutes))).IsValid);
    }

    [Theory]
    [InlineData(15, false)]
    [InlineData(16, true)]
    [InlineData(31, true)]
    [InlineData(32, true)]
    public void Validate_PreservesLegacyKeyLengthsDuringMigration(int length, bool accepted)
    {
        var options = new MessageSecurityOptions
        {
            EnableMessageSigning = true,
            SigningKey = new byte[length],
        };
        if (accepted)
            options.Validate();
        else
            Assert.Throws<InvalidOperationException>(options.Validate);
        Assert.Equal(32, MessageSecurityOptions.GenerateKey().Length);
    }

    [Fact]
    public async Task ProducerHost_ValidatesFinalOptionsAtStartup()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDotCelery(_ => { });
        builder.Services.PostConfigure<MessageSecurityOptions>(options =>
            options.EnableMessageSigning = true
        );
        using var host = builder.Build();
        var exception = await Assert.ThrowsAsync<OptionsValidationException>(() =>
            host.StartAsync(TestContext.Current.CancellationToken)
        );
        Assert.Contains(
            "SigningKey is not set or is too short",
            exception.Message,
            StringComparison.Ordinal
        );
    }

    private static TaskMessage CreateMessage(DateTimeOffset timestamp) =>
        new()
        {
            Id = "task-1",
            Task = "tests.task",
            Args = [1, 2, 3],
            ContentType = "application/json",
            Timestamp = timestamp,
        };

    private static MessageSecurityValidator CreateValidator(
        byte[]? key = null,
        TimeSpan? maxMessageAge = null,
        DateTimeOffset? now = null
    ) =>
        new(
            Options.Create(
                new MessageSecurityOptions
                {
                    EnableMessageSigning = key is not null,
                    SigningKey = key,
                    MaxMessageAge = maxMessageAge,
                }
            ),
            NullLogger<MessageSecurityValidator>.Instance,
            now is null ? null : new FakeTimeProvider(now.Value)
        );

    private static MessageSecurityValidator CreateValidator(byte[] key) =>
        new(
            Options.Create(
                new MessageSecurityOptions { EnableMessageSigning = true, SigningKey = key }
            ),
            NullLogger<MessageSecurityValidator>.Instance
        );
}
