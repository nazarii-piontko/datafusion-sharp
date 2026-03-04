using System.Runtime.InteropServices;
using Google.Protobuf;

namespace DataFusionSharp.Tests;

public sealed class ProtoScalarValueExtensionsTests
{
    #region Non-null simple scalars

    public static TheoryData<ScalarValue, Proto.ScalarValue> NonNullScalars => new()
    {
        { new ScalarValue.Boolean(true), new Proto.ScalarValue { BoolValue = true } },
        { new ScalarValue.Float32(3.14f), new Proto.ScalarValue { Float32Value = 3.14f } },
        { new ScalarValue.Float64(2.71828), new Proto.ScalarValue { Float64Value = 2.71828 } },
        { new ScalarValue.Int8(-1), new Proto.ScalarValue { Int8Value = -1 } },
        { new ScalarValue.Int16(1000), new Proto.ScalarValue { Int16Value = 1000 } },
        { new ScalarValue.Int32(42), new Proto.ScalarValue { Int32Value = 42 } },
        { new ScalarValue.Int64(1_000_000L), new Proto.ScalarValue { Int64Value = 1_000_000L } },
        { new ScalarValue.UInt8(255), new Proto.ScalarValue { Uint8Value = 255 } },
        { new ScalarValue.UInt16(65535), new Proto.ScalarValue { Uint16Value = 65535 } },
        { new ScalarValue.UInt32(uint.MaxValue), new Proto.ScalarValue { Uint32Value = uint.MaxValue } },
        { new ScalarValue.UInt64(ulong.MaxValue), new Proto.ScalarValue { Uint64Value = ulong.MaxValue } },
        { new ScalarValue.Utf8("hello"), new Proto.ScalarValue { Utf8Value = "hello" } },
        { new ScalarValue.Utf8View("world"), new Proto.ScalarValue { Utf8ViewValue = "world" } },
        { new ScalarValue.LargeUtf8("large"), new Proto.ScalarValue { LargeUtf8Value = "large" } },
        { new ScalarValue.Binary([1, 2]), new Proto.ScalarValue { BinaryValue = ByteString.CopyFrom(1, 2) } },
        { new ScalarValue.BinaryView([3]), new Proto.ScalarValue { BinaryViewValue = ByteString.CopyFrom(3) } },
        { new ScalarValue.LargeBinary([4]), new Proto.ScalarValue { LargeBinaryValue = ByteString.CopyFrom(4) } },
        { new ScalarValue.Date32(100), new Proto.ScalarValue { Date32Value = 100 } },
        { new ScalarValue.Date64(86400000L), new Proto.ScalarValue { Date64Value = 86400000L } },
        { new ScalarValue.IntervalYearMonth(12), new Proto.ScalarValue { IntervalYearmonthValue = 12 } },
        { new ScalarValue.DurationSecond(60L), new Proto.ScalarValue { DurationSecondValue = 60L } },
        { new ScalarValue.DurationMillisecond(1000L), new Proto.ScalarValue { DurationMillisecondValue = 1000L } },
        { new ScalarValue.DurationMicrosecond(1_000_000L), new Proto.ScalarValue { DurationMicrosecondValue = 1_000_000L } },
        { new ScalarValue.DurationNanosecond(1_000_000_000L), new Proto.ScalarValue { DurationNanosecondValue = 1_000_000_000L } },
        { new ScalarValue.Time32Second(3600), new Proto.ScalarValue { Time32Value = new Proto.ScalarTime32Value { Time32SecondValue = 3600 } } },
        { new ScalarValue.Time32Millisecond(3_600_000), new Proto.ScalarValue { Time32Value = new Proto.ScalarTime32Value { Time32MillisecondValue = 3_600_000 } } },
        { new ScalarValue.Time64Microsecond(3_600_000_000L), new Proto.ScalarValue { Time64Value = new Proto.ScalarTime64Value { Time64MicrosecondValue = 3_600_000_000L } } },
        { new ScalarValue.Time64Nanosecond(3_600_000_000_000L), new Proto.ScalarValue { Time64Value = new Proto.ScalarTime64Value { Time64NanosecondValue = 3_600_000_000_000L } } },
        { new ScalarValue.TimestampSecond(1_000_000L, "UTC"), new Proto.ScalarValue { TimestampValue = new Proto.ScalarTimestampValue { TimeSecondValue = 1_000_000L, Timezone = "UTC" } } },
        { new ScalarValue.TimestampMillisecond(1_000_000L, null), new Proto.ScalarValue { TimestampValue = new Proto.ScalarTimestampValue { TimeMillisecondValue = 1_000_000L, Timezone = "" } } },
        { new ScalarValue.TimestampMicrosecond(1_000_000L, "Europe/Kiev"), new Proto.ScalarValue { TimestampValue = new Proto.ScalarTimestampValue { TimeMicrosecondValue = 1_000_000L, Timezone = "Europe/Kiev" } } },
        { new ScalarValue.TimestampNanosecond(1_000_000L, null), new Proto.ScalarValue { TimestampValue = new Proto.ScalarTimestampValue { TimeNanosecondValue = 1_000_000L, Timezone = "" } } },
        {
            new ScalarValue.IntervalDayTime(new IntervalDayTimeValue(3, 500)),
            new Proto.ScalarValue { IntervalDaytimeValue = new Proto.IntervalDayTimeValue { Days = 3, Milliseconds = 500 } }
        },
        {
            new ScalarValue.IntervalMonthDayNano(new IntervalMonthDayNanoValue(1, 15, 1000)),
            new Proto.ScalarValue { IntervalMonthDayNano = new Proto.IntervalMonthDayNanoValue { Months = 1, Days = 15, Nanos = 1000 } }
        },
        {
            new ScalarValue.FixedSizeBinary(4, [1, 2, 3, 4]),
            new Proto.ScalarValue { FixedSizeBinaryValue = new Proto.ScalarFixedSizeBinary { Values = ByteString.CopyFrom(1, 2, 3, 4), Length = 4 } }
        },
    };

    [Theory]
    [MemberData(nameof(NonNullScalars))]
    public void NonNull_MapsToExpectedProto(ScalarValue input, Proto.ScalarValue expected)
    {
        var actual = input.ToProto();
        Assert.Equal(expected, actual);
    }

    #endregion

    #region Null scalar values

    public static TheoryData<ScalarValue, Proto.ArrowType.ArrowTypeEnumOneofCase> NullScalars => new()
    {
        { ScalarValue.Boolean.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.BOOL },
        { ScalarValue.Float32.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.FLOAT32 },
        { ScalarValue.Float64.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.FLOAT64 },
        { ScalarValue.Int8.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.INT8 },
        { ScalarValue.Int16.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.INT16 },
        { ScalarValue.Int32.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.INT32 },
        { ScalarValue.Int64.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.INT64 },
        { ScalarValue.UInt8.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.UINT8 },
        { ScalarValue.UInt16.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.UINT16 },
        { ScalarValue.UInt32.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.UINT32 },
        { ScalarValue.UInt64.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.UINT64 },
        { ScalarValue.Utf8.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.UTF8 },
        { ScalarValue.Utf8View.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.UTF8VIEW },
        { ScalarValue.LargeUtf8.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.LARGEUTF8 },
        { ScalarValue.Binary.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.BINARY },
        { ScalarValue.BinaryView.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.BINARYVIEW },
        { ScalarValue.LargeBinary.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.LARGEBINARY },
        { ScalarValue.Date32.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.DATE32 },
        { ScalarValue.Date64.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.DATE64 },
        { ScalarValue.IntervalYearMonth.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.INTERVAL },
        { ScalarValue.IntervalDayTime.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.INTERVAL },
        { ScalarValue.IntervalMonthDayNano.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.INTERVAL },
        { ScalarValue.DurationSecond.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.DURATION },
        { ScalarValue.DurationMillisecond.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.DURATION },
        { ScalarValue.DurationMicrosecond.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.DURATION },
        { ScalarValue.DurationNanosecond.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.DURATION },
        { ScalarValue.Time32Second.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.TIME32 },
        { ScalarValue.Time32Millisecond.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.TIME32 },
        { ScalarValue.Time64Microsecond.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.TIME64 },
        { ScalarValue.Time64Nanosecond.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.TIME64 },
        { ScalarValue.TimestampSecond.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.TIMESTAMP },
        { ScalarValue.TimestampMillisecond.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.TIMESTAMP },
        { ScalarValue.TimestampMicrosecond.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.TIMESTAMP },
        { ScalarValue.TimestampNanosecond.Null, Proto.ArrowType.ArrowTypeEnumOneofCase.TIMESTAMP },
        { new ScalarValue.FixedSizeBinary(8, null), Proto.ArrowType.ArrowTypeEnumOneofCase.FIXEDSIZEBINARY },
    };

    [Theory]
    [MemberData(nameof(NullScalars))]
    public void Null_MapsToNullValueWithCorrectArrowType(ScalarValue input, Proto.ArrowType.ArrowTypeEnumOneofCase expectedCase)
    {
        var proto = input.ToProto();

        Assert.Equal(Proto.ScalarValue.ValueOneofCase.NullValue, proto.ValueCase);
        Assert.Equal(expectedCase, proto.NullValue.ArrowTypeEnumCase);
    }

    #endregion

    #region Decimal128

    [Fact]
    public void Decimal128_PositiveFractional_EncodesCorrectly()
    {
        var proto = new ScalarValue.Decimal128(123.45M, 10).ToProto();

        Assert.Equal(Proto.ScalarValue.ValueOneofCase.Decimal128Value, proto.ValueCase);
        Assert.Equal(10, proto.Decimal128Value.P);
        Assert.Equal(2, proto.Decimal128Value.S);
        Assert.Equal(12345, MemoryMarshal.Read<Int128>(proto.Decimal128Value.Value.Span));
    }

    [Fact]
    public void Decimal128_NegativeFractional_EncodesCorrectly()
    {
        var proto = new ScalarValue.Decimal128(-99.999M, 15).ToProto();

        Assert.Equal(Proto.ScalarValue.ValueOneofCase.Decimal128Value, proto.ValueCase);
        Assert.Equal(15, proto.Decimal128Value.P);
        Assert.Equal(3, proto.Decimal128Value.S);
        Assert.Equal(-99999, MemoryMarshal.Read<Int128>(proto.Decimal128Value.Value.Span));
    }

    [Fact]
    public void Decimal128_Integer_EncodesCorrectly()
    {
        var proto = new ScalarValue.Decimal128(1000M, 10).ToProto();

        Assert.Equal(Proto.ScalarValue.ValueOneofCase.Decimal128Value, proto.ValueCase);
        Assert.Equal(10, proto.Decimal128Value.P);
        Assert.Equal(0, proto.Decimal128Value.S);
        Assert.Equal(1000, MemoryMarshal.Read<Int128>(proto.Decimal128Value.Value.Span));
    }

    [Fact]
    public void Decimal128_Null_MapsToNullValueWithDecimalType()
    {
        var proto = new ScalarValue.Decimal128(null, 20).ToProto();

        Assert.Equal(Proto.ScalarValue.ValueOneofCase.NullValue, proto.ValueCase);
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.DECIMAL, proto.NullValue.ArrowTypeEnumCase);
        Assert.Equal(20u, proto.NullValue.DECIMAL.Precision);
        Assert.Equal(0, proto.NullValue.DECIMAL.Scale);
    }

    [Fact]
    public void Decimal128_InvalidPrecision_Throws()
        => Assert.Throws<ArgumentOutOfRangeException>(() => new ScalarValue.Decimal128(null, ScalarValue.Decimal128.MaxPrecision + 1));

    #endregion
}
