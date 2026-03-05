using System.Globalization;
using Apache.Arrow.Types;

namespace DataFusionSharp.Tests;

public sealed class DataFrameParameterTests : IDisposable
{
    private readonly DataFusionRuntime _runtime = DataFusionRuntime.Create();
    private readonly SessionContext _context;

    public DataFrameParameterTests() => _context = _runtime.CreateSessionContext();

    public static TheoryData<ScalarValue, ArrowTypeId, string> SupportedParameters => new()
    {
        { new ScalarValue.Boolean(true), ArrowTypeId.Boolean, "true" },
        { true, ArrowTypeId.Boolean, "true" },
        { ScalarValue.Boolean.Null, ArrowTypeId.Boolean, "" },
        { new ScalarValue.Float32(3.14f), ArrowTypeId.Float, "3.14" },
        { 3.14f, ArrowTypeId.Float, "3.14" },
        { ScalarValue.Float32.Null, ArrowTypeId.Float, "" },
        { new ScalarValue.Float64(2.71828), ArrowTypeId.Double, "2.71828" },
        { 2.71828, ArrowTypeId.Double, "2.71828" },
        { ScalarValue.Float64.Null, ArrowTypeId.Double, "" },
        { new ScalarValue.Int8(-1), ArrowTypeId.Int8, "-1" },
        { (sbyte)-1, ArrowTypeId.Int8, "-1" },
        { ScalarValue.Int8.Null, ArrowTypeId.Int8, "" },
        { new ScalarValue.Int16(1000), ArrowTypeId.Int16, "1000" },
        { (short)1000, ArrowTypeId.Int16, "1000" },
        { ScalarValue.Int16.Null, ArrowTypeId.Int16, "" },
        { new ScalarValue.Int32(42), ArrowTypeId.Int32, "42" },
        { 42, ArrowTypeId.Int32, "42" },
        { ScalarValue.Int32.Null, ArrowTypeId.Int32, "" },
        { new ScalarValue.Int64(1_000_000L), ArrowTypeId.Int64, "1000000" },
        { 1_000_000L, ArrowTypeId.Int64, "1000000" },
        { ScalarValue.Int64.Null, ArrowTypeId.Int64, "" },
        { new ScalarValue.UInt8(255), ArrowTypeId.UInt8, "255" },
        { (byte)255, ArrowTypeId.UInt8, "255" },
        { ScalarValue.UInt8.Null, ArrowTypeId.UInt8, "" },
        { new ScalarValue.UInt16(65535), ArrowTypeId.UInt16, "65535" },
        { (ushort)65535, ArrowTypeId.UInt16, "65535" },
        { ScalarValue.UInt16.Null, ArrowTypeId.UInt16, "" },
        { new ScalarValue.UInt32(uint.MaxValue), ArrowTypeId.UInt32, "4294967295" },
        { uint.MaxValue, ArrowTypeId.UInt32, "4294967295" },
        { ScalarValue.UInt32.Null, ArrowTypeId.UInt32, "" },
        { new ScalarValue.UInt64(ulong.MaxValue), ArrowTypeId.UInt64, "18446744073709551615" },
        { ulong.MaxValue, ArrowTypeId.UInt64, "18446744073709551615" },
        { ScalarValue.UInt64.Null, ArrowTypeId.UInt64, "" },
        { new ScalarValue.Utf8("hello"), ArrowTypeId.String, "hello" },
        { "hello", ArrowTypeId.String, "hello" },
        { ScalarValue.Utf8.Null, ArrowTypeId.String, "" },
        { new ScalarValue.LargeUtf8("large"), ArrowTypeId.LargeString, "large" },
        { ScalarValue.LargeUtf8.Null, ArrowTypeId.LargeString, "" },
        { new ScalarValue.Utf8View("world"), ArrowTypeId.StringView, "world" },
        { ScalarValue.Utf8View.Null, ArrowTypeId.StringView, "" },
        { new ScalarValue.Binary([1, 2, 3]), ArrowTypeId.Binary, "010203" },
        { new byte[] {1, 2, 3}, ArrowTypeId.Binary, "010203" },
        { ScalarValue.Binary.Null, ArrowTypeId.Binary, "" },
        { new ScalarValue.LargeBinary([4, 5]), ArrowTypeId.LargeBinary, "0405" },
        { ScalarValue.LargeBinary.Null, ArrowTypeId.LargeBinary, "" },
        { new ScalarValue.BinaryView([6]), ArrowTypeId.BinaryView, "06" },
        { ScalarValue.BinaryView.Null, ArrowTypeId.BinaryView, "" },
        { new ScalarValue.FixedSizeBinary(4, [1, 2, 3, 4]), ArrowTypeId.FixedSizedBinary, "01020304" },
        { new ScalarValue.Date32(100), ArrowTypeId.Date32, "1970-04-11" },
        { DateOnly.Parse("1970-04-11", CultureInfo.InvariantCulture), ArrowTypeId.Date32, "1970-04-11" },
        { ScalarValue.Date32.Null, ArrowTypeId.Date32, "" },
        { new ScalarValue.Date64(86_400_000L), ArrowTypeId.Date64, "1970-01-02T00:00:00" },
        { ScalarValue.Date64.Null, ArrowTypeId.Date64, "" },
        { new ScalarValue.Decimal128(-1000m, 10, 0), ArrowTypeId.Decimal128, "-1000" },
        { -1000m, ArrowTypeId.Decimal128, "-1000" },
        { ScalarValue.Decimal128.Null, ArrowTypeId.Decimal128, "" },
        { new ScalarValue.Decimal256(-1000m, 10, 0), ArrowTypeId.Decimal256, "-1000" },
        { ScalarValue.Decimal256.Null, ArrowTypeId.Decimal256, "" },
        { new ScalarValue.IntervalYearMonth(12), ArrowTypeId.Interval, "1 years 0 mons" },
        { ScalarValue.IntervalYearMonth.Null, ArrowTypeId.Interval, "" },
        { new ScalarValue.IntervalDayTime(new IntervalDayTimeValue(3, 500)), ArrowTypeId.Interval, "3 days 0.500 secs" },
        { ScalarValue.IntervalDayTime.Null, ArrowTypeId.Interval, "" },
        { new ScalarValue.IntervalMonthDayNano(new IntervalMonthDayNanoValue(1, 15, 1000)), ArrowTypeId.Interval, "1 mons 15 days 0.000001000 secs" },
        { ScalarValue.IntervalMonthDayNano.Null, ArrowTypeId.Interval, "" },
        { new ScalarValue.DurationSecond(60L), ArrowTypeId.Duration, "0 days 0 hours 1 mins 0 secs" },
        { ScalarValue.DurationSecond.Null, ArrowTypeId.Duration, "" },
        { new ScalarValue.DurationMillisecond(1000L), ArrowTypeId.Duration, "0 days 0 hours 0 mins 1.000 secs" },
        { ScalarValue.DurationMillisecond.Null, ArrowTypeId.Duration, "" },
        { new ScalarValue.DurationMicrosecond(1_000_000L), ArrowTypeId.Duration, "0 days 0 hours 0 mins 1.000000 secs" },
        { ScalarValue.DurationMicrosecond.Null, ArrowTypeId.Duration, "" },
        { new ScalarValue.DurationNanosecond(1_000_000_000L), ArrowTypeId.Duration, "0 days 0 hours 0 mins 1.000000000 secs" },
        { ScalarValue.DurationNanosecond.Null, ArrowTypeId.Duration, "" },
        { new ScalarValue.Time32Second(3600), ArrowTypeId.Time32, "01:00:00" },
        { ScalarValue.Time32Second.Null, ArrowTypeId.Time32, "" },
        { new ScalarValue.Time32Millisecond(3_600_000), ArrowTypeId.Time32, "01:00:00" },
        { ScalarValue.Time32Millisecond.Null, ArrowTypeId.Time32, "" },
        { new ScalarValue.Time64Microsecond(3_600_000_000L), ArrowTypeId.Time64, "01:00:00" },
        { TimeOnly.FromTimeSpan(TimeSpan.FromMicroseconds(3_600_000_000L)), ArrowTypeId.Time64, "01:00:00" },
        { ScalarValue.Time64Microsecond.Null, ArrowTypeId.Time64, "" },
        { new ScalarValue.Time64Nanosecond(3_600_000_000_000L), ArrowTypeId.Time64, "01:00:00" },
        { ScalarValue.Time64Nanosecond.Null, ArrowTypeId.Time64, "" },
        { new ScalarValue.TimestampSecond(1_000_000L, "UTC"), ArrowTypeId.Timestamp, "1970-01-12T13:46:40Z" },
        { ScalarValue.TimestampSecond.Null, ArrowTypeId.Timestamp, "" },
        { new ScalarValue.TimestampMillisecond(1_000_000L, null), ArrowTypeId.Timestamp, "1970-01-01T00:16:40" },
        { ScalarValue.TimestampMillisecond.Null, ArrowTypeId.Timestamp, "" },
        { new ScalarValue.TimestampMicrosecond(1_000_000L, "UTC"), ArrowTypeId.Timestamp, "1970-01-01T00:00:01Z" },
        { ScalarValue.TimestampMicrosecond.Null, ArrowTypeId.Timestamp, "" },
        { new ScalarValue.TimestampNanosecond(10_000_000L, null), ArrowTypeId.Timestamp, "1970-01-01T00:00:00.010" },
        { ScalarValue.TimestampNanosecond.Null, ArrowTypeId.Timestamp, "" }
    };

    [Theory]
    [MemberData(nameof(SupportedParameters))]
    public async Task SelectParam_ReturnsCorrectTypeAndValue(ScalarValue param, ArrowTypeId expectedType, string expectedOutput)
    {
        // Act
        using var df = await _context.SqlAsync("SELECT $p AS val", [("p", param)]);
        var schema = await df.GetSchemaAsync();
        var str = await df.ToStringAsync();
        
        // Assert
        Assert.Equal(expectedType, schema.FieldsList[0].DataType.TypeId);
        
        var valueStr = str.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries)[^2].Trim(' ', '|');
        Assert.Equal(expectedOutput, valueStr);
    }
    
    public void Dispose()
    {
        _context.Dispose();
        _runtime.Dispose();
    }
}
