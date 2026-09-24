using System.Security.Cryptography;
using System.Text;
using DotCelery.Core.Security;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

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

    private static MessageSecurityValidator CreateValidator(byte[] key) =>
        new(
            Options.Create(
                new MessageSecurityOptions { EnableMessageSigning = true, SigningKey = key }
            ),
            NullLogger<MessageSecurityValidator>.Instance
        );
}
