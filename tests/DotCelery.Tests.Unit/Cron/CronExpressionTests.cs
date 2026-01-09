using DotCelery.Cron;

namespace DotCelery.Tests.Unit.Cron;

public class CronExpressionTests
{
    [Fact]
    public void Parse_ValidStandardExpression_Succeeds()
    {
        // Arrange & Act
        var cron = CronExpression.Parse("*/5 * * * *");

        // Assert
        Assert.NotNull(cron);
        Assert.Equal("*/5 * * * *", cron.ToString());
    }

    [Fact]
    public void Parse_ValidExpressionWithSeconds_Succeeds()
    {
        // Arrange & Act
        var cron = CronExpression.Parse("0 */5 * * * *", CronFormat.IncludeSeconds);

        // Assert
        Assert.NotNull(cron);
        Assert.Equal("0 */5 * * * *", cron.ToString());
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("* * * *")]
    [InlineData("* * * * * *")]
    public void Parse_InvalidFieldCount_ThrowsCronFormatException(string expression)
    {
        // Act & Assert
        Assert.Throws<CronFormatException>(() => CronExpression.Parse(expression));
    }

    [Fact]
    public void Parse_NullExpression_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => CronExpression.Parse(null!));
    }

    [Theory]
    [InlineData("60 * * * *")] // Invalid minute
    [InlineData("* 24 * * *")] // Invalid hour
    [InlineData("* * 32 * *")] // Invalid day
    [InlineData("* * * 13 *")] // Invalid month
    [InlineData("* * * * 8")] // Invalid day of week
    public void Parse_OutOfRangeValue_ThrowsCronFormatException(string expression)
    {
        // Act & Assert
        Assert.Throws<CronFormatException>(() => CronExpression.Parse(expression));
    }

    [Fact]
    public void TryParse_ValidExpression_ReturnsTrue()
    {
        // Arrange & Act
        var result = CronExpression.TryParse("0 0 * * *", out var cron);

        // Assert
        Assert.True(result);
        Assert.NotNull(cron);
    }

    [Fact]
    public void TryParse_InvalidExpression_ReturnsFalse()
    {
        // Arrange & Act
        var result = CronExpression.TryParse("invalid", out var cron);

        // Assert
        Assert.False(result);
        Assert.Null(cron);
    }

    [Theory]
    [InlineData("0 * * * *", 0)] // At minute 0
    [InlineData("30 * * * *", 30)] // At minute 30
    [InlineData("59 * * * *", 59)] // At minute 59
    public void Parse_SingleMinuteValue_MatchesCorrectly(string expression, int expectedMinute)
    {
        // Arrange
        var cron = CronExpression.Parse(expression);
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(expectedMinute, next.Value.Minute);
    }

    [Fact]
    public void Parse_MinuteRange_MatchesAllValuesInRange()
    {
        // Arrange
        var cron = CronExpression.Parse("10-15 * * * *");
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act - Get occurrences for the first hour
        var occurrences = cron.GetOccurrences(from, from.AddHours(1)).ToList();

        // Assert
        Assert.Equal(6, occurrences.Count); // Minutes 10, 11, 12, 13, 14, 15
        Assert.Equal(10, occurrences[0].Minute);
        Assert.Equal(15, occurrences[5].Minute);
    }

    [Fact]
    public void Parse_MinuteList_MatchesListedValues()
    {
        // Arrange
        var cron = CronExpression.Parse("0,15,30,45 * * * *");
        // Start from before minute 0 to include it in results
        var from = new DateTimeOffset(2024, 12, 31, 23, 59, 59, TimeSpan.Zero);

        // Act
        var occurrences = cron.GetOccurrences(from, from.AddHours(2)).Take(4).ToList();

        // Assert
        Assert.Equal(4, occurrences.Count);
        Assert.Equal(0, occurrences[0].Minute);
        Assert.Equal(15, occurrences[1].Minute);
        Assert.Equal(30, occurrences[2].Minute);
        Assert.Equal(45, occurrences[3].Minute);
    }

    [Theory]
    [InlineData("*/5 * * * *", 12)] // Every 5 minutes = 12 per hour
    [InlineData("*/10 * * * *", 6)] // Every 10 minutes = 6 per hour
    [InlineData("*/15 * * * *", 4)] // Every 15 minutes = 4 per hour
    [InlineData("*/30 * * * *", 2)] // Every 30 minutes = 2 per hour
    public void Parse_MinuteStep_MatchesCorrectCount(string expression, int expectedCount)
    {
        // Arrange
        var cron = CronExpression.Parse(expression);
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var occurrences = cron.GetOccurrences(from, from.AddHours(1)).ToList();

        // Assert
        Assert.Equal(expectedCount, occurrences.Count);
    }

    [Fact]
    public void Parse_StepWithOffset_StartsFromOffset()
    {
        // Arrange - every 15 minutes starting from minute 5
        var cron = CronExpression.Parse("5/15 * * * *");
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var occurrences = cron.GetOccurrences(from, from.AddHours(1)).ToList();

        // Assert - should fire at 5, 20, 35, 50
        Assert.Equal(4, occurrences.Count);
        Assert.Equal(5, occurrences[0].Minute);
        Assert.Equal(20, occurrences[1].Minute);
        Assert.Equal(35, occurrences[2].Minute);
        Assert.Equal(50, occurrences[3].Minute);
    }

    [Theory]
    [InlineData("0 0 * * *", 0)] // At midnight
    [InlineData("0 12 * * *", 12)] // At noon
    [InlineData("0 23 * * *", 23)] // At 11 PM
    public void Parse_SingleHourValue_MatchesCorrectly(string expression, int expectedHour)
    {
        // Arrange
        var cron = CronExpression.Parse(expression);
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(expectedHour, next.Value.Hour);
    }

    [Fact]
    public void Parse_HourRange_MatchesAllValuesInRange()
    {
        // Arrange - 9 AM to 5 PM
        var cron = CronExpression.Parse("0 9-17 * * *");
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var occurrences = cron.GetOccurrences(from, from.AddDays(1)).ToList();

        // Assert - 9 hours: 9, 10, 11, 12, 13, 14, 15, 16, 17
        Assert.Equal(9, occurrences.Count);
        Assert.Equal(9, occurrences.First().Hour);
        Assert.Equal(17, occurrences.Last().Hour);
    }

    [Theory]
    [InlineData("0 0 1 * *", 1)] // First day of month
    [InlineData("0 0 15 * *", 15)] // 15th of month
    [InlineData("0 0 28 * *", 28)] // 28th of month
    public void Parse_SingleDayOfMonth_MatchesCorrectly(string expression, int expectedDay)
    {
        // Arrange
        var cron = CronExpression.Parse(expression);
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(expectedDay, next.Value.Day);
    }

    [Fact]
    public void Parse_DayOfMonth31_SkipsMonthsWithout31Days()
    {
        // Arrange
        var cron = CronExpression.Parse("0 0 31 * *");
        var from = new DateTimeOffset(2025, 2, 1, 0, 0, 0, TimeSpan.Zero); // February

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert - Should skip to March 31
        Assert.NotNull(next);
        Assert.Equal(3, next.Value.Month); // March
        Assert.Equal(31, next.Value.Day);
    }

    [Theory]
    [InlineData("0 0 1 1 *", 1)] // January
    [InlineData("0 0 1 6 *", 6)] // June
    [InlineData("0 0 1 12 *", 12)] // December
    public void Parse_SingleMonthValue_MatchesCorrectly(string expression, int expectedMonth)
    {
        // Arrange
        var cron = CronExpression.Parse(expression);
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(expectedMonth, next.Value.Month);
    }

    [Theory]
    [InlineData("0 0 1 JAN *", 1)]
    [InlineData("0 0 1 FEB *", 2)]
    [InlineData("0 0 1 MAR *", 3)]
    [InlineData("0 0 1 APR *", 4)]
    [InlineData("0 0 1 MAY *", 5)]
    [InlineData("0 0 1 JUN *", 6)]
    [InlineData("0 0 1 JUL *", 7)]
    [InlineData("0 0 1 AUG *", 8)]
    [InlineData("0 0 1 SEP *", 9)]
    [InlineData("0 0 1 OCT *", 10)]
    [InlineData("0 0 1 NOV *", 11)]
    [InlineData("0 0 1 DEC *", 12)]
    public void Parse_MonthName_MatchesCorrectly(string expression, int expectedMonth)
    {
        // Arrange
        var cron = CronExpression.Parse(expression);
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(expectedMonth, next.Value.Month);
    }

    [Theory]
    [InlineData("0 0 * * 0", DayOfWeek.Sunday)]
    [InlineData("0 0 * * 1", DayOfWeek.Monday)]
    [InlineData("0 0 * * 2", DayOfWeek.Tuesday)]
    [InlineData("0 0 * * 3", DayOfWeek.Wednesday)]
    [InlineData("0 0 * * 4", DayOfWeek.Thursday)]
    [InlineData("0 0 * * 5", DayOfWeek.Friday)]
    [InlineData("0 0 * * 6", DayOfWeek.Saturday)]
    public void Parse_SingleDayOfWeek_MatchesCorrectly(string expression, DayOfWeek expectedDay)
    {
        // Arrange
        var cron = CronExpression.Parse(expression);
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero); // Wednesday

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(expectedDay, next.Value.DayOfWeek);
    }

    [Theory]
    [InlineData("0 0 * * SUN", DayOfWeek.Sunday)]
    [InlineData("0 0 * * MON", DayOfWeek.Monday)]
    [InlineData("0 0 * * TUE", DayOfWeek.Tuesday)]
    [InlineData("0 0 * * WED", DayOfWeek.Wednesday)]
    [InlineData("0 0 * * THU", DayOfWeek.Thursday)]
    [InlineData("0 0 * * FRI", DayOfWeek.Friday)]
    [InlineData("0 0 * * SAT", DayOfWeek.Saturday)]
    public void Parse_DayOfWeekName_MatchesCorrectly(string expression, DayOfWeek expectedDay)
    {
        // Arrange
        var cron = CronExpression.Parse(expression);
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(expectedDay, next.Value.DayOfWeek);
    }

    [Fact]
    public void Parse_Weekdays_MatchesMondayToFriday()
    {
        // Arrange - Monday to Friday
        var cron = CronExpression.Parse("0 0 * * 1-5");
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero); // Wednesday

        // Act - Get next 7 occurrences
        var occurrences = cron.GetOccurrences(from, from.AddDays(14)).Take(7).ToList();

        // Assert - All should be weekdays
        Assert.All(
            occurrences,
            o => Assert.True(o.DayOfWeek >= DayOfWeek.Monday && o.DayOfWeek <= DayOfWeek.Friday)
        );
    }

    [Fact]
    public void GetNextOccurrence_EveryMinute_ReturnsNextMinute()
    {
        // Arrange
        var cron = CronExpression.Parse("* * * * *");
        var from = new DateTimeOffset(2025, 1, 1, 12, 30, 30, TimeSpan.Zero);

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(new DateTimeOffset(2025, 1, 1, 12, 31, 0, TimeSpan.Zero), next.Value);
    }

    [Fact]
    public void GetNextOccurrence_Hourly_ReturnsNextHour()
    {
        // Arrange
        var cron = CronExpression.Parse("0 * * * *");
        var from = new DateTimeOffset(2025, 1, 1, 12, 30, 0, TimeSpan.Zero);

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(new DateTimeOffset(2025, 1, 1, 13, 0, 0, TimeSpan.Zero), next.Value);
    }

    [Fact]
    public void GetNextOccurrence_Daily_ReturnsNextMidnight()
    {
        // Arrange
        var cron = CronExpression.Parse("0 0 * * *");
        var from = new DateTimeOffset(2025, 1, 1, 12, 0, 0, TimeSpan.Zero);

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(new DateTimeOffset(2025, 1, 2, 0, 0, 0, TimeSpan.Zero), next.Value);
    }

    [Fact]
    public void GetNextOccurrence_CrossesYearBoundary()
    {
        // Arrange - Every December 31st
        var cron = CronExpression.Parse("0 0 31 12 *");
        var from = new DateTimeOffset(2025, 12, 31, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(2026, next.Value.Year);
        Assert.Equal(12, next.Value.Month);
        Assert.Equal(31, next.Value.Day);
    }

    [Fact]
    public void GetNextOccurrence_LeapYear_HandlesFeb29()
    {
        // Arrange
        var cron = CronExpression.Parse("0 0 29 2 *");
        var from = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero); // 2024 is leap year

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(2024, next.Value.Year);
        Assert.Equal(2, next.Value.Month);
        Assert.Equal(29, next.Value.Day);
    }

    [Fact]
    public void GetNextOccurrence_NonLeapYear_SkipsToNextLeapYear()
    {
        // Arrange
        var cron = CronExpression.Parse("0 0 29 2 *");
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero); // 2025 is not leap year

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(2028, next.Value.Year); // Next leap year
        Assert.Equal(2, next.Value.Month);
        Assert.Equal(29, next.Value.Day);
    }

    [Fact]
    public void GetOccurrences_ReturnsCorrectCount()
    {
        // Arrange
        var cron = CronExpression.Parse("*/5 * * * *"); // Every 5 minutes
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var to = from.AddHours(1);

        // Act
        var occurrences = cron.GetOccurrences(from, to).ToList();

        // Assert
        Assert.Equal(12, occurrences.Count);
    }

    [Fact]
    public void GetOccurrences_EmptyRange_ReturnsEmpty()
    {
        // Arrange
        var cron = CronExpression.Parse("0 0 * * *"); // Daily
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var to = from.AddHours(1); // Only 1 hour range

        // Act
        var occurrences = cron.GetOccurrences(from, to).ToList();

        // Assert
        Assert.Empty(occurrences);
    }

    [Fact]
    public void EveryMinute_FiresEveryMinute()
    {
        // Arrange
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var occurrences = CronExpression
            .EveryMinute.GetOccurrences(from, from.AddMinutes(5))
            .ToList();

        // Assert
        Assert.Equal(5, occurrences.Count);
    }

    [Fact]
    public void Hourly_FiresAtMinuteZero()
    {
        // Arrange
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = CronExpression.Hourly.GetNextOccurrence(from);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(0, next.Value.Minute);
        Assert.Equal(1, next.Value.Hour);
    }

    [Fact]
    public void Daily_FiresAtMidnight()
    {
        // Arrange
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = CronExpression.Daily.GetNextOccurrence(from);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(0, next.Value.Hour);
        Assert.Equal(0, next.Value.Minute);
        Assert.Equal(2, next.Value.Day);
    }

    [Fact]
    public void Weekly_FiresOnSunday()
    {
        // Arrange
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero); // Wednesday

        // Act
        var next = CronExpression.Weekly.GetNextOccurrence(from);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(DayOfWeek.Sunday, next.Value.DayOfWeek);
    }

    [Fact]
    public void Monthly_FiresOnFirstDay()
    {
        // Arrange
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = CronExpression.Monthly.GetNextOccurrence(from);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(1, next.Value.Day);
        Assert.Equal(2, next.Value.Month); // February
    }

    [Fact]
    public void Yearly_FiresOnJanuary1()
    {
        // Arrange
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = CronExpression.Yearly.GetNextOccurrence(from);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(1, next.Value.Month);
        Assert.Equal(1, next.Value.Day);
        Assert.Equal(2026, next.Value.Year);
    }

    [Fact]
    public void Parse_QuestionMark_TreatedAsWildcard()
    {
        // Arrange & Act
        var cron = CronExpression.Parse("0 0 ? * *");

        // Assert - Should match any day
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var next = cron.GetNextOccurrence(from);
        Assert.NotNull(next);
    }

    [Fact]
    public void GetNextOccurrence_FromExactMatch_ReturnsNextOccurrence()
    {
        // Arrange - Every minute
        var cron = CronExpression.Parse("* * * * *");
        var from = new DateTimeOffset(2025, 1, 1, 12, 0, 0, TimeSpan.Zero);

        // Act - From exact match should return next minute
        var next = cron.GetNextOccurrence(from);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(1, next.Value.Minute); // Next minute
    }

    [Fact]
    public void Parse_CaseInsensitive_MonthNames()
    {
        // Arrange & Act
        var cron1 = CronExpression.Parse("0 0 1 jan *");
        var cron2 = CronExpression.Parse("0 0 1 JAN *");
        var cron3 = CronExpression.Parse("0 0 1 Jan *");

        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Assert - All should match January
        Assert.Equal(1, cron1.GetNextOccurrence(from)?.Month);
        Assert.Equal(1, cron2.GetNextOccurrence(from)?.Month);
        Assert.Equal(1, cron3.GetNextOccurrence(from)?.Month);
    }

    [Fact]
    public void Parse_CaseInsensitive_DayNames()
    {
        // Arrange & Act
        var cron1 = CronExpression.Parse("0 0 * * mon");
        var cron2 = CronExpression.Parse("0 0 * * MON");
        var cron3 = CronExpression.Parse("0 0 * * Mon");

        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Assert - All should match Monday
        Assert.Equal(DayOfWeek.Monday, cron1.GetNextOccurrence(from)?.DayOfWeek);
        Assert.Equal(DayOfWeek.Monday, cron2.GetNextOccurrence(from)?.DayOfWeek);
        Assert.Equal(DayOfWeek.Monday, cron3.GetNextOccurrence(from)?.DayOfWeek);
    }

    // Phase 2: L Modifier Tests

    [Fact]
    public void Parse_LastDayOfMonth_MatchesLastDay()
    {
        // Arrange
        var cron = CronExpression.Parse("0 0 L * *");
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert - January has 31 days
        Assert.NotNull(next);
        Assert.Equal(31, next.Value.Day);
        Assert.Equal(1, next.Value.Month);
    }

    [Fact]
    public void Parse_LastDayOfMonth_HandlesDifferentMonths()
    {
        // Arrange
        var cron = CronExpression.Parse("0 0 L * *");

        // Test February in non-leap year
        var fromFeb = new DateTimeOffset(2025, 2, 1, 0, 0, 0, TimeSpan.Zero);
        var nextFeb = cron.GetNextOccurrence(fromFeb);
        Assert.Equal(28, nextFeb?.Day);

        // Test February in leap year
        var fromLeap = new DateTimeOffset(2024, 2, 1, 0, 0, 0, TimeSpan.Zero);
        var nextLeap = cron.GetNextOccurrence(fromLeap);
        Assert.Equal(29, nextLeap?.Day);

        // Test April (30 days)
        var fromApr = new DateTimeOffset(2025, 4, 1, 0, 0, 0, TimeSpan.Zero);
        var nextApr = cron.GetNextOccurrence(fromApr);
        Assert.Equal(30, nextApr?.Day);
    }

    [Fact]
    public void Parse_LastDayOfMonthWithOffset_MatchesCorrectDay()
    {
        // Arrange - L-3 means 3 days before the last day
        var cron = CronExpression.Parse("0 0 L-3 * *");
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert - January has 31 days, L-3 = 28
        Assert.NotNull(next);
        Assert.Equal(28, next.Value.Day);
    }

    [Fact]
    public void Parse_LastWeekday_MatchesLastWeekdayOfMonth()
    {
        // Arrange - LW means last weekday of month
        var cron = CronExpression.Parse("0 0 LW * *");

        // January 2025 ends on Friday the 31st
        var fromJan = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var nextJan = cron.GetNextOccurrence(fromJan);
        Assert.Equal(31, nextJan?.Day); // Last weekday is Friday 31st

        // November 2025 ends on Sunday the 30th, so last weekday is Friday 28th
        var fromNov = new DateTimeOffset(2025, 11, 1, 0, 0, 0, TimeSpan.Zero);
        var nextNov = cron.GetNextOccurrence(fromNov);
        Assert.Equal(28, nextNov?.Day);
    }

    // Phase 2: W Modifier Tests

    [Fact]
    public void Parse_NearestWeekday_MatchesWeekday()
    {
        // Arrange - 15W means nearest weekday to the 15th
        var cron = CronExpression.Parse("0 0 15W * *");

        // January 2025: 15th is Wednesday (weekday) - stays at 15
        var fromJan = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var nextJan = cron.GetNextOccurrence(fromJan);
        Assert.Equal(15, nextJan?.Day);
    }

    [Fact]
    public void Parse_NearestWeekday_Saturday_MoveToFriday()
    {
        // Arrange - 15W with 15th on Saturday moves to Friday 14th
        var cron = CronExpression.Parse("0 0 15W * *");

        // February 2025: 15th is Saturday, nearest weekday is Friday 14th
        var fromFeb = new DateTimeOffset(2025, 2, 1, 0, 0, 0, TimeSpan.Zero);
        var nextFeb = cron.GetNextOccurrence(fromFeb);
        Assert.Equal(14, nextFeb?.Day);
    }

    [Fact]
    public void Parse_NearestWeekday_Sunday_MoveToMonday()
    {
        // Arrange - 16W with 16th on Sunday moves to Monday 17th
        var cron = CronExpression.Parse("0 0 16W * *");

        // February 2025: 16th is Sunday, nearest weekday is Monday 17th
        var fromFeb = new DateTimeOffset(2025, 2, 1, 0, 0, 0, TimeSpan.Zero);
        var nextFeb = cron.GetNextOccurrence(fromFeb);
        Assert.Equal(17, nextFeb?.Day);
    }

    [Fact]
    public void Parse_NearestWeekday_FirstOfMonth_Saturday()
    {
        // Arrange - 1W with 1st on Saturday moves to Monday 3rd (not Friday in previous month)
        var cron = CronExpression.Parse("0 0 1W * *");

        // February 2025: 1st is Saturday, moves to Monday 3rd
        var fromFeb = new DateTimeOffset(2025, 2, 1, 0, 0, 0, TimeSpan.Zero);
        var nextFeb = cron.GetNextOccurrence(fromFeb);
        Assert.Equal(3, nextFeb?.Day);
    }

    // Phase 2: # Modifier Tests (Nth day of week)

    [Fact]
    public void Parse_NthDayOfWeek_FirstFriday()
    {
        // Arrange - 5#1 means first Friday of month
        var cron = CronExpression.Parse("0 0 * * 5#1");
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert - First Friday of January 2025 is the 3rd
        Assert.NotNull(next);
        Assert.Equal(3, next.Value.Day);
        Assert.Equal(DayOfWeek.Friday, next.Value.DayOfWeek);
    }

    [Fact]
    public void Parse_NthDayOfWeek_SecondMonday()
    {
        // Arrange - 1#2 means second Monday of month
        var cron = CronExpression.Parse("0 0 * * 1#2");
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert - Second Monday of January 2025 is the 13th
        Assert.NotNull(next);
        Assert.Equal(13, next.Value.Day);
        Assert.Equal(DayOfWeek.Monday, next.Value.DayOfWeek);
    }

    [Fact]
    public void Parse_NthDayOfWeek_ThirdWednesday()
    {
        // Arrange - 3#3 means third Wednesday
        var cron = CronExpression.Parse("0 0 * * 3#3");
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert - Third Wednesday of January 2025 is the 15th
        Assert.NotNull(next);
        Assert.Equal(15, next.Value.Day);
        Assert.Equal(DayOfWeek.Wednesday, next.Value.DayOfWeek);
    }

    [Fact]
    public void Parse_NthDayOfWeek_FifthOccurrence_SkipsIfNotExists()
    {
        // Arrange - 1#5 means fifth Monday (may not exist)
        var cron = CronExpression.Parse("0 0 * * 1#5");
        var from = new DateTimeOffset(2025, 2, 1, 0, 0, 0, TimeSpan.Zero); // February 2025 has no 5th Monday

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert - Should skip to a month with 5 Mondays (March 2025 has 5 Mondays)
        Assert.NotNull(next);
        Assert.Equal(3, next.Value.Month); // March
        Assert.Equal(31, next.Value.Day); // 5th Monday is March 31
    }

    // Phase 2: nL Modifier Tests (Last occurrence of day)

    [Fact]
    public void Parse_LastDayOfWeekInMonth_LastFriday()
    {
        // Arrange - 5L means last Friday of month
        var cron = CronExpression.Parse("0 0 * * 5L");
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert - Last Friday of January 2025 is the 31st
        Assert.NotNull(next);
        Assert.Equal(31, next.Value.Day);
        Assert.Equal(DayOfWeek.Friday, next.Value.DayOfWeek);
    }

    [Fact]
    public void Parse_LastDayOfWeekInMonth_LastSunday()
    {
        // Arrange - 0L means last Sunday of month
        var cron = CronExpression.Parse("0 0 * * 0L");
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert - Last Sunday of January 2025 is the 26th
        Assert.NotNull(next);
        Assert.Equal(26, next.Value.Day);
        Assert.Equal(DayOfWeek.Sunday, next.Value.DayOfWeek);
    }

    // Phase 4: Reverse Ranges

    [Fact]
    public void Parse_ReverseDayOfWeekRange_SaturdayToMonday()
    {
        // Arrange - 6-1 (SAT-MON) means Saturday, Sunday, Monday
        var cron = CronExpression.Parse("0 0 * * 6-1");
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero); // Wednesday

        // Act - Get first 3 occurrences
        var occurrences = cron.GetOccurrences(from, from.AddDays(10)).Take(3).ToList();

        // Assert
        Assert.Equal(DayOfWeek.Saturday, occurrences[0].DayOfWeek);
        Assert.Equal(DayOfWeek.Sunday, occurrences[1].DayOfWeek);
        Assert.Equal(DayOfWeek.Monday, occurrences[2].DayOfWeek);
    }

    [Fact]
    public void Parse_ReverseDayOfWeekRange_FridayToTuesday()
    {
        // Arrange - FRI-TUE (5-2) means Fri, Sat, Sun, Mon, Tue
        var cron = CronExpression.Parse("0 0 * * FRI-TUE");
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero); // Wednesday

        // Act - Get first 5 occurrences
        var occurrences = cron.GetOccurrences(from, from.AddDays(14)).Take(5).ToList();

        // Assert - Should be Fri, Sat, Sun, Mon, Tue
        Assert.Equal(DayOfWeek.Friday, occurrences[0].DayOfWeek);
        Assert.Equal(DayOfWeek.Saturday, occurrences[1].DayOfWeek);
        Assert.Equal(DayOfWeek.Sunday, occurrences[2].DayOfWeek);
        Assert.Equal(DayOfWeek.Monday, occurrences[3].DayOfWeek);
        Assert.Equal(DayOfWeek.Tuesday, occurrences[4].DayOfWeek);
    }

    // Phase 4: 7-field format with year

    [Fact]
    public void Parse_SevenFieldFormat_WithYear()
    {
        // Arrange - 7-field: sec min hour dom month dow year
        var cron = CronExpression.Parse("0 0 0 1 1 * 2026", CronFormat.IncludeSecondsAndYear);
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert - Should fire on Jan 1, 2026
        Assert.NotNull(next);
        Assert.Equal(2026, next.Value.Year);
        Assert.Equal(1, next.Value.Month);
        Assert.Equal(1, next.Value.Day);
    }

    [Fact]
    public void Parse_SixFieldFormat_WithYear()
    {
        // Arrange - 6-field with year: min hour dom month dow year
        var cron = CronExpression.Parse("0 0 1 1 * 2027", CronFormat.IncludeYear);
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert - Should fire on Jan 1, 2027
        Assert.NotNull(next);
        Assert.Equal(2027, next.Value.Year);
        Assert.Equal(1, next.Value.Month);
        Assert.Equal(1, next.Value.Day);
    }

    [Fact]
    public void Parse_YearRange_MatchesYearsInRange()
    {
        // Arrange - Every January 1st from 2025-2027
        var cron = CronExpression.Parse("0 0 1 1 * 2025-2027", CronFormat.IncludeYear);
        var from = new DateTimeOffset(2024, 12, 31, 0, 0, 0, TimeSpan.Zero);
        var to = new DateTimeOffset(2027, 1, 2, 0, 0, 0, TimeSpan.Zero);

        // Act
        var occurrences = cron.GetOccurrences(from, to).ToList();

        // Assert - Should get Jan 1 2025, Jan 1 2026, Jan 1 2027
        Assert.Equal(3, occurrences.Count);
        Assert.All(occurrences, o => Assert.Equal(1, o.Day));
        Assert.All(occurrences, o => Assert.Equal(1, o.Month));
        Assert.All(occurrences, o => Assert.Equal(0, o.Hour));
        Assert.All(occurrences, o => Assert.Equal(0, o.Minute));
        Assert.Contains(occurrences, o => o.Year == 2025);
        Assert.Contains(occurrences, o => o.Year == 2026);
        Assert.Contains(occurrences, o => o.Year == 2027);
    }

    [Fact]
    public void Parse_YearInPast_ReturnsNull()
    {
        // Arrange - Year 2020 is in the past
        var cron = CronExpression.Parse("0 0 1 1 * 2020", CronFormat.IncludeYear);
        var from = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var next = cron.GetNextOccurrence(from);

        // Assert
        Assert.Null(next);
    }

    // Phase 4: Human-readable description

    [Fact]
    public void ToDescription_EveryMinute_ReturnsCorrectDescription()
    {
        var cron = CronExpression.Parse("* * * * *");
        Assert.Equal("Every minute", cron.ToDescription());
    }

    [Fact]
    public void ToDescription_EveryHour_ReturnsCorrectDescription()
    {
        var cron = CronExpression.Parse("0 * * * *");
        Assert.Equal("Every hour", cron.ToDescription());
    }

    [Fact]
    public void ToDescription_DailyAtMidnight_ReturnsCorrectDescription()
    {
        var cron = CronExpression.Parse("0 0 * * *");
        Assert.Equal("Every day at midnight", cron.ToDescription());
    }

    [Fact]
    public void ToDescription_WeeklyOnSunday_ReturnsCorrectDescription()
    {
        var cron = CronExpression.Parse("0 0 * * 0");
        Assert.Equal("Every Sunday at midnight", cron.ToDescription());
    }

    [Fact]
    public void ToDescription_MonthlyOnFirstDay_ReturnsCorrectDescription()
    {
        var cron = CronExpression.Parse("0 0 1 * *");
        Assert.Equal("First day of every month at midnight", cron.ToDescription());
    }

    [Fact]
    public void ToDescription_YearlyOnJanuary1_ReturnsCorrectDescription()
    {
        var cron = CronExpression.Parse("0 0 1 1 *");
        Assert.Equal("Every January 1st at midnight", cron.ToDescription());
    }

    [Fact]
    public void ToDescription_SpecificDays_IncludesDayNames()
    {
        var cron = CronExpression.Parse("0 9 * * 1,3,5");
        var desc = cron.ToDescription();
        Assert.Contains("Monday", desc);
        Assert.Contains("Wednesday", desc);
        Assert.Contains("Friday", desc);
    }

    [Fact]
    public void ToDescription_LastDayOfMonth_DescribesL()
    {
        var cron = CronExpression.Parse("0 0 L * *");
        var desc = cron.ToDescription();
        Assert.Contains("last day", desc);
    }

    [Fact]
    public void ToDescription_LastWeekday_DescribesLW()
    {
        var cron = CronExpression.Parse("0 0 LW * *");
        var desc = cron.ToDescription();
        Assert.Contains("last weekday", desc);
    }

    [Fact]
    public void ToDescription_NearestWeekday_DescribesW()
    {
        var cron = CronExpression.Parse("0 0 15W * *");
        var desc = cron.ToDescription();
        Assert.Contains("nearest weekday", desc);
    }

    [Fact]
    public void ToDescription_NthOccurrence_DescribesHash()
    {
        var cron = CronExpression.Parse("0 0 * * 5#3");
        var desc = cron.ToDescription();
        Assert.Contains("3rd", desc);
        Assert.Contains("Friday", desc);
    }

    [Fact]
    public void ToDescription_LastDayOfWeekInMonth_DescribesNL()
    {
        var cron = CronExpression.Parse("0 0 * * 5L");
        var desc = cron.ToDescription();
        Assert.Contains("last", desc);
        Assert.Contains("Friday", desc);
    }

    // Phase 3: DST Handling

    [Fact]
    public void GetNextOccurrence_WithTimezone_ReturnsCorrectOffset()
    {
        // Arrange
        var cron = CronExpression.Parse("0 9 * * *"); // 9 AM
        var utcTime = new DateTimeOffset(2025, 7, 15, 0, 0, 0, TimeSpan.Zero);
        var eastUs = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");

        // Act
        var next = cron.GetNextOccurrence(utcTime, eastUs);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(9, next.Value.Hour);
        // In July, Eastern is UTC-4 (daylight saving)
        Assert.Equal(TimeSpan.FromHours(-4), next.Value.Offset);
    }

    [Fact]
    public void GetNextOccurrence_StandardTime_ReturnsCorrectOffset()
    {
        // Arrange
        var cron = CronExpression.Parse("0 9 * * *"); // 9 AM
        var utcTime = new DateTimeOffset(2025, 1, 15, 0, 0, 0, TimeSpan.Zero);
        var eastUs = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");

        // Act
        var next = cron.GetNextOccurrence(utcTime, eastUs);

        // Assert
        Assert.NotNull(next);
        Assert.Equal(9, next.Value.Hour);
        // In January, Eastern is UTC-5 (standard time)
        Assert.Equal(TimeSpan.FromHours(-5), next.Value.Offset);
    }

    // Additional edge case tests

    [Fact]
    public void Parse_InvalidHashSyntax_ThrowsCronFormatException()
    {
        // 8 is invalid day of week (valid: 0-6)
        Assert.Throws<CronFormatException>(() => CronExpression.Parse("0 0 * * 8#1"));
    }

    [Theory]
    [InlineData("0 0 32W * *")] // Day 32 doesn't exist
    [InlineData("0 0 0W * *")] // Day 0 doesn't exist
    public void Parse_InvalidWDay_ThrowsCronFormatException(string expression)
    {
        Assert.Throws<CronFormatException>(() => CronExpression.Parse(expression));
    }

    [Fact]
    public void Parse_LWithInvalidOffset_ThrowsCronFormatException()
    {
        Assert.Throws<CronFormatException>(() => CronExpression.Parse("0 0 L-abc * *"));
    }

    [Fact]
    public void Parse_InvalidHashOccurrence_ThrowsCronFormatException()
    {
        // Occurrence must be 1-5
        Assert.Throws<CronFormatException>(() => CronExpression.Parse("0 0 * * 5#0"));
        Assert.Throws<CronFormatException>(() => CronExpression.Parse("0 0 * * 5#6"));
    }
}
