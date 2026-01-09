using System.Numerics;
using System.Runtime.CompilerServices;

namespace DotCelery.Cron;

/// <summary>
/// Represents a cron field using a bitfield for O(1) matching.
/// Each bit position indicates whether that value is valid for the field.
/// </summary>
internal readonly struct CronField
{
    private readonly ulong _bits;

    /// <summary>
    /// Gets the bitfield representing valid values.
    /// </summary>
    public ulong Bits => _bits;

    /// <summary>
    /// Gets whether any value is valid (at least one bit set).
    /// </summary>
    public bool HasAnyValue => _bits != 0;

    private CronField(ulong bits)
    {
        _bits = bits;
    }

    /// <summary>
    /// Creates a field with a single value.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static CronField FromValue(int value)
    {
        return new CronField(1UL << value);
    }

    /// <summary>
    /// Creates a field with all values in a range.
    /// </summary>
    public static CronField FromRange(int start, int end)
    {
        if (start <= end)
        {
            // Normal range: create mask from start to end
            var mask = ((1UL << (end - start + 1)) - 1) << start;
            return new CronField(mask);
        }
        else
        {
            throw new CronFormatException(
                $"Invalid range: {start}-{end}. Start must be less than or equal to end."
            );
        }
    }

    /// <summary>
    /// Creates a field with a wrapped range (e.g., SAT-MON for day of week: 6-1).
    /// </summary>
    /// <param name="start">Start value (higher than end for wrapped range)</param>
    /// <param name="end">End value (lower than start for wrapped range)</param>
    /// <param name="min">Minimum value for the field</param>
    /// <param name="max">Maximum value for the field</param>
    /// <param name="step">Step value (default 1)</param>
    public static CronField FromWrappedRange(int start, int end, int min, int max, int step = 1)
    {
        ulong bits = 0;

        // From start to max
        for (var i = start; i <= max; i += step)
        {
            bits |= 1UL << i;
        }

        // From min to end (continue stepping from where we left off)
        var offset = (max - start + step) % step;
        var startOfSecondPart = min + (step - offset) % step;
        if (startOfSecondPart < min)
        {
            startOfSecondPart = min;
        }

        for (var i = startOfSecondPart; i <= end; i += step)
        {
            bits |= 1UL << i;
        }

        // If step is 1, simpler logic
        if (step == 1)
        {
            bits = 0;
            for (var i = start; i <= max; i++)
            {
                bits |= 1UL << i;
            }
            for (var i = min; i <= end; i++)
            {
                bits |= 1UL << i;
            }
        }

        return new CronField(bits);
    }

    /// <summary>
    /// Creates a field with all values from start to max with a step.
    /// </summary>
    public static CronField FromStep(int start, int max, int step)
    {
        if (step <= 0)
        {
            throw new CronFormatException($"Step value must be positive, got: {step}");
        }

        ulong bits = 0;
        for (var i = start; i <= max; i += step)
        {
            bits |= 1UL << i;
        }

        return new CronField(bits);
    }

    /// <summary>
    /// Creates a field with all values in range.
    /// </summary>
    public static CronField All(int min, int max)
    {
        return FromRange(min, max);
    }

    /// <summary>
    /// Creates a field from raw bits.
    /// </summary>
    public static CronField FromBits(ulong bits)
    {
        return new CronField(bits);
    }

    /// <summary>
    /// Combines this field with another using OR (union).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public CronField Or(CronField other)
    {
        return new CronField(_bits | other._bits);
    }

    /// <summary>
    /// Checks if the specified value matches this field.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Contains(int value)
    {
        return (_bits & (1UL << value)) != 0;
    }

    /// <summary>
    /// Gets the first (lowest) value in this field.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetFirst()
    {
        return BitOperations.TrailingZeroCount(_bits);
    }

    /// <summary>
    /// Gets the next value greater than or equal to the specified value.
    /// Returns -1 if no such value exists.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetNext(int fromValue)
    {
        // Mask off bits below fromValue
        var masked = _bits & ~((1UL << fromValue) - 1);
        if (masked == 0)
        {
            return -1;
        }

        return BitOperations.TrailingZeroCount(masked);
    }

    /// <summary>
    /// Gets the next value strictly greater than the specified value.
    /// Returns -1 if no such value exists.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetNextAfter(int afterValue)
    {
        return GetNext(afterValue + 1);
    }
}
