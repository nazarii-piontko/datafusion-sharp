using Apache.Arrow;
using Apache.Arrow.Scalars;
using Apache.Arrow.Types;

namespace DataFusionSharp.Tests;

public sealed class ArrayExtensionsTests
{
    [Fact]
    public void AsBool_BooleanArray_ReturnsValues()
    {
        // Arrange
        using var array = new BooleanArray.Builder().Append(true).Append(false).Build();

        // Act
        var result = array.AsBool().ToList();

        // Assert
        Assert.Equal([true, false], result);
    }

    [Fact]
    public void AsBool_IArrowArray_ReturnsValues()
    {
        // Arrange
        using IArrowArray array = new BooleanArray.Builder().Append(true).AppendNull().Build();

        // Act
        var result = array.AsBool().ToList();

        // Assert
        Assert.Equal([true, null], result);
    }

    [Fact]
    public void AsInt64_Int64Array_ReturnsValues()
    {
        // Arrange
        var array = new Int64Array.Builder().Append(10).Append(20).Build();

        // Act
        var result = array.AsInt64().ToList();

        // Assert
        Assert.Equal([10L, 20L], result);
    }

    [Fact]
    public void AsInt64_IArrowArray_ReturnsValues()
    {
        // Arrange
        using IArrowArray array = new Int64Array.Builder().Append(42).AppendNull().Build();

        // Act
        var result = array.AsInt64().ToList();

        // Assert
        Assert.Equal([42L, null], result);
    }
    
    [Fact]
    public void AsInt64_IArrowArray_WithWrongType_Throws()
    {
        // Arrange
        using IArrowArray array = new BooleanArray.Builder().Append(false).Build();

        // Act and Assert
        Assert.Throws<ArgumentException>(() => array.AsInt64().ToList());
    }

    [Fact]
    public void AsDouble_DoubleArray_ReturnsValues()
    {
        // Arrange
        using var array = new DoubleArray.Builder().Append(1.1).Append(2.2).Build();

        // Act
        var result = array.AsDouble().ToList();

        // Assert
        Assert.Equal([1.1, 2.2], result);
    }

    [Fact]
    public void AsDouble_IArrowArray_ReturnsValues()
    {
        // Arrange
        using IArrowArray array = new DoubleArray.Builder().Append(3.14).AppendNull().Build();

        // Act
        var result = array.AsDouble().ToList();

        // Assert
        Assert.Equal([3.14, null], result);
    }
    
    [Fact]
    public void AsDouble_IArrowArray_WithWrongType_Throws()
    {
        // Arrange
        using IArrowArray array = new BooleanArray.Builder().Append(false).Build();

        // Act and Assert
        Assert.Throws<ArgumentException>(() => array.AsDouble().ToList());
    }

    [Fact]
    public void AsInt32_Int32Array_ReturnsValues()
    {
        // Arrange
        using var array = new Int32Array.Builder().Append(10).Append(20).Build();

        // Act
        var result = array.AsInt32().ToList();

        // Assert
        Assert.Equal([10, 20], result);
    }

    [Fact]
    public void AsInt32_IArrowArray_ReturnsValues()
    {
        // Arrange
        using IArrowArray array = new Int32Array.Builder().Append(42).AppendNull().Build();

        // Act
        var result = array.AsInt32().ToList();

        // Assert
        Assert.Equal([42, null], result);
    }

    [Fact]
    public void AsInt32_IArrowArray_WithWrongType_Throws()
    {
        // Arrange
        using IArrowArray array = new BooleanArray.Builder().Append(false).Build();

        // Act and Assert
        Assert.Throws<ArgumentException>(() => array.AsInt32().ToList());
    }

    [Fact]
    public void AsFloat_FloatArray_ReturnsValues()
    {
        // Arrange
        using var array = new FloatArray.Builder().Append(1.5f).Append(2.5f).Build();

        // Act
        var result = array.AsFloat().ToList();

        // Assert
        Assert.Equal([1.5f, 2.5f], result);
    }

    [Fact]
    public void AsFloat_IArrowArray_ReturnsValues()
    {
        // Arrange
        using IArrowArray array = new FloatArray.Builder().Append(3.14f).AppendNull().Build();

        // Act
        var result = array.AsFloat().ToList();

        // Assert
        Assert.Equal([3.14f, null], result);
    }

    [Fact]
    public void AsFloat_IArrowArray_WithWrongType_Throws()
    {
        // Arrange
        using IArrowArray array = new BooleanArray.Builder().Append(false).Build();

        // Act and Assert
        Assert.Throws<ArgumentException>(() => array.AsFloat().ToList());
    }

    [Fact]
    public void AsDecimal_Decimal128Array_ReturnsValues()
    {
        // Arrange
        using var array = new Decimal128Array.Builder(new Decimal128Type(10, 2))
            .Append(12.34m).Append(56.78m).Build();

        // Act
        var result = array.AsDecimal().ToList();

        // Assert
        Assert.Equal([12.34m, 56.78m], result);
    }

    [Fact]
    public void AsDecimal_IArrowArray_ReturnsValues()
    {
        // Arrange
        using IArrowArray array = new Decimal128Array.Builder(new Decimal128Type(10, 2))
            .Append(99.99m).AppendNull().Build();

        // Act
        var result = array.AsDecimal().ToList();

        // Assert
        Assert.Equal([99.99m, null], result);
    }

    [Fact]
    public void AsDecimal_IArrowArray_WithWrongType_Throws()
    {
        // Arrange
        using IArrowArray array = new BooleanArray.Builder().Append(false).Build();

        // Act and Assert
        Assert.Throws<ArgumentException>(() => array.AsDecimal().ToList());
    }
    
    [Fact]
    public void AsDecimal_Decimal256Array_ReturnsValues()
    {
        // Arrange
        using var array = new Decimal256Array.Builder(new Decimal256Type(10, 2))
            .Append(12.34m).Append(56.78m).Build();

        // Act
        var result = array.AsDecimal().ToList();

        // Assert
        Assert.Equal([12.34m, 56.78m], result);
    }

    [Fact]
    public void AsDecimal_Decimal256Array_WithNulls_ReturnsValues()
    {
        // Arrange
        using var array = new Decimal256Array.Builder(new Decimal256Type(10, 2))
            .Append(99.99m).AppendNull().Build();

        // Act
        var result = array.AsDecimal().ToList();

        // Assert
        Assert.Equal([99.99m, null], result);
    }

    [Fact]
    public void AsDecimal_IArrowArray_Decimal256_ReturnsValues()
    {
        // Arrange
        using IArrowArray array = new Decimal256Array.Builder(new Decimal256Type(10, 2))
            .Append(42.42m).AppendNull().Build();

        // Act
        var result = array.AsDecimal().ToList();

        // Assert
        Assert.Equal([42.42m, null], result);
    }

    [Fact]
    public void AsDateOnly_Date32Array_ReturnsValues()
    {
        // Arrange
        using var array = new Date32Array.Builder()
            .Append(new DateTime(2026, 1, 10))
            .Append(new DateTime(2026, 2, 15))
            .Build();

        // Act
        var result = array.AsDateOnly().ToList();

        // Assert
        Assert.Equal([new DateOnly(2026, 1, 10), new DateOnly(2026, 2, 15)], result);
    }

    [Fact]
    public void AsDateOnly_Date64Array_ReturnsValues()
    {
        // Arrange
        using var array = new Date64Array.Builder()
            .Append(new DateTime(2026, 1, 10))
            .Append(new DateTime(2026, 2, 15))
            .Build();

        // Act
        var result = array.AsDateOnly().ToList();

        // Assert
        Assert.Equal([new DateOnly(2026, 1, 10), new DateOnly(2026, 2, 15)], result);
    }

    [Fact]
    public void AsDateOnly_IArrowArray_ReturnsValues()
    {
        // Arrange
        using IArrowArray array = new Date32Array.Builder()
            .Append(new DateTime(2026, 1, 10))
            .AppendNull()
            .Build();

        // Act
        var result = array.AsDateOnly().ToList();

        // Assert
        Assert.Equal([new DateOnly(2026, 1, 10), null], result);
    }

    [Fact]
    public void AsDateOnly_IArrowArray_WithWrongType_Throws()
    {
        // Arrange
        using IArrowArray array = new BooleanArray.Builder().Append(false).Build();

        // Act and Assert
        Assert.Throws<ArgumentException>(() => array.AsDateOnly().ToList());
    }

    [Fact]
    public void AsTimestamp_TimestampArray_ReturnsValues()
    {
        // Arrange
        var ts1 = new DateTimeOffset(2026, 1, 10, 10, 30, 0, TimeSpan.Zero);
        var ts2 = new DateTimeOffset(2026, 2, 15, 11, 45, 0, TimeSpan.Zero);
        using var array = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, TimeZoneInfo.Utc))
            .Append(ts1).Append(ts2).Build();

        // Act
        var result = array.AsTimestamp().ToList();

        // Assert
        Assert.Equal([ts1, ts2], result);
    }

    [Fact]
    public void AsTimestamp_IArrowArray_ReturnsValues()
    {
        // Arrange
        var ts = new DateTimeOffset(2026, 1, 1, 8, 0, 0, TimeSpan.Zero);
        using IArrowArray array = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, TimeZoneInfo.Utc))
            .Append(ts).AppendNull().Build();

        // Act
        var result = array.AsTimestamp().ToList();

        // Assert
        Assert.Equal([ts, null], result);
    }

    [Fact]
    public void AsTimestamp_IArrowArray_WithWrongType_Throws()
    {
        // Arrange
        using IArrowArray array = new BooleanArray.Builder().Append(false).Build();

        // Act and Assert
        Assert.Throws<ArgumentException>(() => array.AsTimestamp().ToList());
    }

    [Fact]
    public void AsString_StringArray_ReturnsValues()
    {
        // Arrange
        using var array = new StringArray.Builder().Append("hello").Append("world").Build();

        // Act
        var result = array.AsString().ToList();

        // Assert
        Assert.Equal(["hello", "world"], result);
    }

    [Fact]
    public void AsString_StringViewArray_ReturnsValues()
    {
        // Arrange
        using var array = new StringViewArray.Builder().Append("alpha").Append("beta").Build();

        // Act
        var result = array.AsString().ToList();

        // Assert
        Assert.Equal(["alpha", "beta"], result);
    }

    [Fact]
    public void AsString_LargeStringArray_ReturnsValues()
    {
        // Arrange
        var stringData = "largestring"u8.ToArray();
        using var abNulls = new ArrowBuffer.BitmapBuilder().Build();
        using var abOffsets = new ArrowBuffer.Builder<long>().Append(0).Append("large".Length) .Append(stringData.Length).Build();
        using var abData = new ArrowBuffer.Builder<byte>().Append(stringData.AsSpan()).Build();
        using var array = new LargeStringArray(2, abOffsets, abData, abNulls);

        // Act
        var result = array.AsString().ToList();

        // Assert
        Assert.Equal(["large", "string"], result);
    }

    [Fact]
    public void AsString_IArrowArray_ReturnsValues()
    {
        // Arrange
        using IArrowArray array = new StringArray.Builder().Append("test").AppendNull().Build();

        // Act
        var result = array.AsString().ToList();

        // Assert
        Assert.Equal(["test", null], result);
    }
    
    [Fact]
    public void AsString_IArrowArray_WithWrongType_Throws()
    {
        // Arrange
        using IArrowArray array = new BooleanArray.Builder().Append(false).Build();

        // Act and Assert
        Assert.Throws<ArgumentException>(() => array.AsString().ToList());
    }

    [Theory]
    [MemberData(nameof(ToNetTypeTestData))]
    public void ToNetType_ReturnsExpectedType(IArrowType arrowType, Type? expectedType)
    {
        // Act
        var result = arrowType.ToNetType();

        // Assert
        Assert.Equal(expectedType, result);
    }

    public static TheoryData<IArrowType, Type?> ToNetTypeTestData => new()
    {
        { NullType.Default, typeof(DBNull) },
        { BooleanType.Default, typeof(bool) },
        { new Int8Type(), typeof(sbyte) },
        { new Int16Type(), typeof(short) },
        { new Int32Type(), typeof(int) },
        { new Int64Type(), typeof(long) },
        { new UInt8Type(), typeof(byte) },
        { new UInt16Type(), typeof(ushort) },
        { new UInt32Type(), typeof(uint) },
        { new UInt64Type(), typeof(ulong) },
        { HalfFloatType.Default, typeof(Half) },
        { FloatType.Default, typeof(float) },
        { DoubleType.Default, typeof(double) },
        { new Decimal128Type(10, 2), typeof(decimal) },
        { new Decimal256Type(10, 2), typeof(decimal) },
        { StringType.Default, typeof(string) },
        { StringViewType.Default, typeof(string) },
        { LargeStringType.Default, typeof(string) },
        { BinaryType.Default, typeof(byte[]) },
        { BinaryViewType.Default, typeof(byte[]) },
        { new FixedSizeBinaryType(4), typeof(byte[]) },
        { LargeBinaryType.Default, typeof(byte[]) },
        { Date32Type.Default, typeof(DateOnly) },
        { Date64Type.Default, typeof(DateOnly) },
        { new TimestampType(TimeUnit.Microsecond, TimeZoneInfo.Utc), typeof(DateTimeOffset) },
        { new Time32Type(TimeUnit.Second), typeof(TimeOnly) },
        { new Time64Type(TimeUnit.Microsecond), typeof(TimeOnly) },
        { DurationType.Microsecond, typeof(TimeSpan) },
        { new IntervalType(IntervalUnit.YearMonth), typeof(YearMonthInterval) },
        { new IntervalType(IntervalUnit.DayTime), typeof(DayTimeInterval) },
        { new IntervalType(IntervalUnit.MonthDayNanosecond), typeof(MonthDayNanosecondInterval) },
        { new MapType(new Field("key", new StringType(), false), new Field("value", new Int32Type(), true)), typeof(IEnumerable<IArrowArray>) },
        { new ListType(new Field("item", new Int32Type(), false)), typeof(IEnumerable<IArrowArray>) },
        { new LargeListType(new Field("item", new Int32Type(), false)), typeof(IEnumerable<IArrowArray>) },
        { new FixedSizeListType(new Field("item", new Int32Type(), false), 3), typeof(IEnumerable<IArrowArray>) },
        { new StructType([new Field("f", new Int32Type(), false)]), typeof(object?[]) },
    };

    [Fact]
    public void ToNetType_UnsupportedType_ReturnsNull()
    {
        // Arrange
        // DictionaryType is not mapped
        var arrowType = new DictionaryType(new Int32Type(), new StringType(), false);

        // Act
        var result = arrowType.ToNetType();

        // Assert
        Assert.Null(result);
    }
}
