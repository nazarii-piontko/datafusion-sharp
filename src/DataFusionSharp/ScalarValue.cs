// These suppressions apply to the entire file.
// The type names intentionally mirror the Rust datafusion::scalar::ScalarValue enum variants and Apache Arrow type names.
// Renaming them to satisfy generic .NET naming guidelines would break the 1:1 correspondence with DataFusion
// and make the API harder to use for anyone familiar with the upstream project.
#pragma warning disable CA1034 // Nested types should not be visible
#pragma warning disable CA1711 // Identifiers should not have incorrect suffix
#pragma warning disable CA1716 // Identifiers should not match keywords
#pragma warning disable CA1720 // Identifier contains type name
#pragma warning disable CA1724 // Type names should not match namespaces
#pragma warning disable CA1819 // Properties should not return arrays
#pragma warning disable CA2225 // Operator overloads have named alternates
// ReSharper disable UnusedType.Global
// ReSharper disable NotAccessedPositionalProperty.Global

using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Google.Protobuf;

namespace DataFusionSharp;

/// <summary>
/// Represents a single scalar value in DataFusion.
/// This is the C# equivalent of the Rust <c>datafusion::scalar::ScalarValue</c> enum.
/// </summary>
public abstract record ScalarValue
{
    /// <summary>
    /// A true or false value.
    /// </summary>
    public sealed record Boolean(bool? Value) : ScalarValue
    {
        /// <summary>
        /// A Boolean scalar with a null value.
        /// </summary>
        public static Boolean Null => new(null);
        
        /// <summary>
        /// Implicit conversion from bool to Boolean for convenience.
        /// </summary>
        /// <param name="value">The bool value to convert.</param>
        /// <returns>>A Boolean scalar with the given value.</returns>
        public static implicit operator Boolean(bool value) => new(value);
    }

    /// <summary>
    /// 32-bit floating-point value.
    /// </summary>
    public sealed record Float32(float? Value) : ScalarValue
    {
        /// <summary>
        /// A Float32 scalar with a null value.
        /// </summary>
        public static Float32 Null => new(null);

        /// <summary>
        /// Implicit conversion from float to Float32 for convenience.
        /// </summary>
        /// <param name="value">The float value to convert.</param>
        /// <returns>A Float32 scalar with the given value.</returns>
        public static implicit operator Float32(float value) => new(value);
    }

    /// <summary>
    /// 64-bit floating-point value.
    /// </summary>
    public sealed record Float64(double? Value) : ScalarValue
    {
        /// <summary>
        /// A Float64 scalar with a null value.
        /// </summary>
        public static Float64 Null => new(null);

        /// <summary>
        /// Implicit conversion from double to Float64 for convenience.
        /// </summary>
        /// <param name="value">The double value to convert.</param>
        /// <returns>A Float64 scalar with the given value.</returns>
        public static implicit operator Float64(double value) => new(value);
    }

    /// <summary>
    /// 128-bit decimal value. Scale is inferred from <see cref="Value"/> itself.
    /// </summary>
    public sealed record Decimal128 : ScalarValue
    {
        /// <summary>
        /// The decimal value, or <c>null</c> if the scalar is null.
        /// </summary>
        public decimal? Value { get; init; }

        /// <summary>
        /// The total number of significant digits (1–38).
        /// </summary>
        public byte Precision { get; init; }

        /// <summary>
        /// Initializes a new instance of <see cref="ScalarValue.Decimal128"/>.
        /// </summary>
        /// <param name="value">The decimal value, or <c>null</c> if the scalar is null.</param>
        /// <param name="precision">The total number of significant digits (1–38).</param>
        public Decimal128(decimal? value, byte precision)
        {
            if (precision is < 1 or > 38)
                throw new ArgumentOutOfRangeException(nameof(precision), "Precision must be between 1 and 38 for Decimal128.");
            Value = value;
            Precision = precision;
        }
    }

    /// <summary>
    /// Signed 8-bit integer.
    /// </summary>
    public sealed record Int8(sbyte? Value) : ScalarValue
    {
        /// <summary>
        /// An Int8 scalar with a null value.
        /// </summary>
        public static Int8 Null => new(null);

        /// <summary>
        /// Implicit conversion from sbyte to Int8 for convenience.
        /// </summary>
        /// <param name="value">The sbyte value to convert.</param>
        /// <returns>An Int8 scalar with the given value.</returns>
        public static implicit operator Int8(sbyte value) => new(value);
    }

    /// <summary>
    /// Signed 16-bit integer.
    /// </summary>
    public sealed record Int16(short? Value) : ScalarValue
    {
        /// <summary>
        /// An Int16 scalar with a null value.
        /// </summary>
        public static Int16 Null => new(null);

        /// <summary>
        /// Implicit conversion from short to Int16 for convenience.
        /// </summary>
        /// <param name="value">The short value to convert.</param>
        /// <returns>An Int16 scalar with the given value.</returns>
        public static implicit operator Int16(short value) => new(value);
    }

    /// <summary>
    /// Signed 32-bit integer.
    /// </summary>
    public sealed record Int32(int? Value) : ScalarValue
    {
        /// <summary>
        /// An Int32 scalar with a null value.
        /// </summary>
        public static Int32 Null => new(null);

        /// <summary>
        /// Implicit conversion from int to Int32 for convenience.
        /// </summary>
        /// <param name="value">The int value to convert.</param>
        /// <returns>An Int32 scalar with the given value.</returns>
        public static implicit operator Int32(int value) => new(value);
    }

    /// <summary>
    /// Signed 64-bit integer.
    /// </summary>
    public sealed record Int64(long? Value) : ScalarValue
    {
        /// <summary>
        /// An Int64 scalar with a null value.
        /// </summary>
        public static Int64 Null => new(null);

        /// <summary>
        /// Implicit conversion from long to Int64 for convenience.
        /// </summary>
        /// <param name="value">The long value to convert.</param>
        /// <returns>An Int64 scalar with the given value.</returns>
        public static implicit operator Int64(long value) => new(value);
    }

    /// <summary>
    /// Unsigned 8-bit integer.
    /// </summary>
    public sealed record UInt8(byte? Value) : ScalarValue
    {
        /// <summary>
        /// A UInt8 scalar with a null value.
        /// </summary>
        public static UInt8 Null => new(null);

        /// <summary>
        /// Implicit conversion from byte to UInt8 for convenience.
        /// </summary>
        /// <param name="value">The byte value to convert.</param>
        /// <returns>A UInt8 scalar with the given value.</returns>
        public static implicit operator UInt8(byte value) => new(value);
    }

    /// <summary>
    /// Unsigned 16-bit integer.
    /// </summary>
    public sealed record UInt16(ushort? Value) : ScalarValue
    {
        /// <summary>
        /// A UInt16 scalar with a null value.
        /// </summary>
        public static UInt16 Null => new(null);

        /// <summary>
        /// Implicit conversion from ushort to UInt16 for convenience.
        /// </summary>
        /// <param name="value">The ushort value to convert.</param>
        /// <returns>A UInt16 scalar with the given value.</returns>
        public static implicit operator UInt16(ushort value) => new(value);
    }

    /// <summary>
    /// Unsigned 32-bit integer.
    /// </summary>
    public sealed record UInt32(uint? Value) : ScalarValue
    {
        /// <summary>
        /// A UInt32 scalar with a null value.
        /// </summary>
        public static UInt32 Null => new(null);

        /// <summary>
        /// Implicit conversion from uint to UInt32 for convenience.
        /// </summary>
        /// <param name="value">The uint value to convert.</param>
        /// <returns>A UInt32 scalar with the given value.</returns>
        public static implicit operator UInt32(uint value) => new(value);
    }

    /// <summary>
    /// Unsigned 64-bit integer.
    /// </summary>
    public sealed record UInt64(ulong? Value) : ScalarValue
    {
        /// <summary>
        /// A UInt64 scalar with a null value.
        /// </summary>
        public static UInt64 Null => new(null);

        /// <summary>
        /// Implicit conversion from ulong to UInt64 for convenience.
        /// </summary>
        /// <param name="value">The ulong value to convert.</param>
        /// <returns>A UInt64 scalar with the given value.</returns>
        public static implicit operator UInt64(ulong value) => new(value);
    }

    /// <summary>
    /// UTF-8 encoded string.
    /// </summary>
    public sealed record Utf8(string? Value) : ScalarValue
    {
        /// <summary>
        /// A Utf8 scalar with a null value.
        /// </summary>
        public static Utf8 Null => new(null);

        /// <summary>
        /// Implicit conversion from string to Utf8 for convenience.
        /// </summary>
        /// <param name="value">The string value to convert.</param>
        /// <returns>An Utf8 scalar with the given value.</returns>
        public static implicit operator Utf8(string value) => new(value);
    }

    /// <summary>
    /// UTF-8 encoded string from view types.
    /// </summary>
    public sealed record Utf8View(string? Value) : ScalarValue
    {
        /// <summary>
        /// A Utf8View scalar with a null value.
        /// </summary>
        public static Utf8View Null => new((string?)null);
    }

    /// <summary>
    /// UTF-8 encoded string representing a LargeString's Arrow type.
    /// </summary>
    public sealed record LargeUtf8(string? Value) : ScalarValue
    {
        /// <summary>
        /// A LargeUtf8 scalar with a null value.
        /// </summary>
        public static LargeUtf8 Null => new((string?)null);
    }

    /// <summary>
    /// Binary data.
    /// </summary>
    public sealed record Binary(byte[]? Value) : ScalarValue
    {
        /// <summary>
        /// A Binary scalar with a null value.
        /// </summary>
        public static Binary Null => new(null);

        /// <summary>
        /// Implicit conversion from byte array to Binary for convenience.
        /// </summary>
        /// <param name="value">The byte array value to convert.</param>
        /// <returns>A Binary scalar with the given value.</returns>
        public static implicit operator Binary(byte[] value) => new(value);
    }

    /// <summary>
    /// Binary data from view types.
    /// </summary>
    public sealed record BinaryView(byte[]? Value) : ScalarValue
    {
        /// <summary>
        /// A BinaryView scalar with a null value.
        /// </summary>
        public static BinaryView Null => new((byte[]?)null);
    }

    /// <summary>
    /// Fixed-size binary data.
    /// </summary>
    /// <param name="Size">The fixed byte-width of each value.</param>
    /// <param name="Value">The binary data, or <c>null</c> if the scalar is null.</param>
    public sealed record FixedSizeBinary(int Size, byte[]? Value) : ScalarValue;

    /// <summary>
    /// Large binary data.
    /// </summary>
    public sealed record LargeBinary(byte[]? Value) : ScalarValue
    {
        /// <summary>
        /// A LargeBinary scalar with a null value.
        /// </summary>
        public static LargeBinary Null => new((byte[]?)null);
    }

    /// <summary>
    /// Fixed-size list scalar. The array must be a <see cref="FixedSizeListArray"/> with length 1.
    /// </summary>
    public sealed record FixedSizeList(FixedSizeListArray Array) : ScalarValue;

    /// <summary>
    /// Represents a single element of a <see cref="ListArray"/>.
    /// The array must be a <see cref="ListArray"/> with length 1.
    /// </summary>
    public sealed record List(ListArray Array) : ScalarValue;

    /// <summary>
    /// Represents a single element of a <see cref="Apache.Arrow.LargeListArray"/>.
    /// The array must be a <see cref="Apache.Arrow.LargeListArray"/> with length 1.
    /// </summary>
    public sealed record LargeList(LargeListArray Array) : ScalarValue;

    /// <summary>
    /// Represents a single element <see cref="StructArray"/>.
    /// </summary>
    public sealed record Struct(StructArray Array) : ScalarValue;

    /// <summary>
    /// Represents a single element <see cref="MapArray"/>.
    /// </summary>
    public sealed record Map(MapArray Array) : ScalarValue;

    /// <summary>
    /// Date stored as a signed 32-bit int — days since UNIX epoch 1970-01-01.
    /// </summary>
    public sealed record Date32(int? Value) : ScalarValue
    {
        /// <summary>
        /// A Date32 scalar with a null value.
        /// </summary>
        public static Date32 Null => new((int?)null);

        /// <inheritdoc />
        public Date32(DateOnly date)
            : this((int)(date.ToDateTime(TimeOnly.MinValue) - DateTime.UnixEpoch).TotalDays)
        {
        }
    }

    /// <summary>
    /// Date stored as a signed 64-bit int — milliseconds since UNIX epoch 1970-01-01.
    /// </summary>
    public sealed record Date64(long? Value) : ScalarValue
    {
        /// <summary>
        /// A Date64 scalar with a null value.
        /// </summary>
        public static Date64 Null => new((long?)null);

        /// <inheritdoc />
        public Date64(DateOnly date)
            : this((long)(date.ToDateTime(TimeOnly.MinValue) - DateTime.UnixEpoch).TotalMilliseconds)
        {
        }
    }

    /// <summary>
    /// Time stored as a signed 32-bit int — seconds since midnight.
    /// </summary>
    public sealed record Time32Second(int? Value) : ScalarValue
    {
        /// <summary>
        /// A Time32Second scalar with a null value.
        /// </summary>
        public static Time32Second Null => new((int?)null);

        /// <inheritdoc />
        public Time32Second(TimeOnly time)
            : this (time.ToTimeSpan().Seconds)
        {
        }
    }

    /// <summary>
    /// Time stored as a signed 32-bit int — milliseconds since midnight.
    /// </summary>
    public sealed record Time32Millisecond(int? Value) : ScalarValue
    {
        /// <summary>
        /// A Time32Millisecond scalar with a null value.
        /// </summary>
        public static Time32Millisecond Null => new((int?)null);

        /// <inheritdoc />
        public Time32Millisecond(TimeOnly time)
            : this (time.ToTimeSpan().Milliseconds)
        {
        }
    }

    /// <summary>
    /// Time stored as a signed 64-bit int — microseconds since midnight.
    /// </summary>
    public sealed record Time64Microsecond(long? Value) : ScalarValue
    {
        /// <summary>
        /// A Time64Microsecond scalar with a null value.
        /// </summary>
        public static Time64Microsecond Null => new((long?)null);

        /// <inheritdoc />
        public Time64Microsecond(TimeOnly time)
            : this (time.ToTimeSpan().Ticks / 10) // 1 tick = 100 nanoseconds = 0.1 microseconds
        {
        }
    }

    /// <summary>
    /// Time stored as a signed 64-bit int — nanoseconds since midnight.
    /// </summary>
    public sealed record Time64Nanosecond(long? Value) : ScalarValue
    {
        /// <summary>
        /// A Time64Nanosecond scalar with a null value.
        /// </summary>
        public static Time64Nanosecond Null => new((long?)null);

        /// <inheritdoc />
        public Time64Nanosecond(TimeOnly time)
            : this (time.ToTimeSpan().Ticks * 100) // 1 tick = 100 nanoseconds
        {
        }
    }

    /// <summary>
    /// Timestamp with second precision.
    /// </summary>
    /// <param name="Value">Seconds since UNIX epoch, or <c>null</c>.</param>
    /// <param name="Timezone">Optional IANA timezone string.</param>
    public sealed record TimestampSecond(long? Value, string? Timezone) : ScalarValue
    {
        /// <summary>
        /// A TimestampSecond scalar with a null value.
        /// </summary>
        public static TimestampSecond Null => new(null, null);

        /// <inheritdoc />
        public TimestampSecond(DateTimeOffset timestamp)
            : this((long)(timestamp - DateTimeOffset.UnixEpoch).TotalSeconds, timestamp.Offset == TimeSpan.Zero ? null : timestamp.Offset.ToString())
        {
        }
    }

    /// <summary>
    /// Timestamp with millisecond precision.
    /// </summary>
    /// <param name="Value">Milliseconds since UNIX epoch, or <c>null</c>.</param>
    /// <param name="Timezone">Optional IANA timezone string.</param>
    public sealed record TimestampMillisecond(long? Value, string? Timezone) : ScalarValue
    {
        /// <summary>
        /// A TimestampMillisecond scalar with a null value.
        /// </summary>
        public static TimestampMillisecond Null => new(null, null);

        /// <inheritdoc />
        public TimestampMillisecond(DateTimeOffset timestamp)
            : this((long)(timestamp - DateTimeOffset.UnixEpoch).TotalMilliseconds, timestamp.Offset == TimeSpan.Zero ? null : timestamp.Offset.ToString())
        {
        }
    }

    /// <summary>
    /// Timestamp with microsecond precision.
    /// </summary>
    /// <param name="Value">Microseconds since UNIX epoch, or <c>null</c>.</param>
    /// <param name="Timezone">Optional IANA timezone string.</param>
    public sealed record TimestampMicrosecond(long? Value, string? Timezone) : ScalarValue
    {
        /// <summary>
        /// A TimestampMicrosecond scalar with a null value.
        /// </summary>
        public static TimestampMicrosecond Null => new(null, null);

        /// <inheritdoc />
        public TimestampMicrosecond(DateTimeOffset timestamp)
            : this((long)(timestamp - DateTimeOffset.UnixEpoch).TotalMicroseconds, timestamp.Offset == TimeSpan.Zero ? null : timestamp.Offset.ToString())
        {
        }
    }

    /// <summary>
    /// Timestamp with nanosecond precision.
    /// </summary>
    /// <param name="Value">Nanoseconds since UNIX epoch, or <c>null</c>.</param>
    /// <param name="Timezone">Optional IANA timezone string.</param>
    public sealed record TimestampNanosecond(long? Value, string? Timezone) : ScalarValue
    {
        /// <summary>
        /// A TimestampNanosecond scalar with a null value.
        /// </summary>
        public static TimestampNanosecond Null => new(null, null);

        /// <inheritdoc />
        public TimestampNanosecond(DateTimeOffset timestamp)
            : this((long)(timestamp - DateTimeOffset.UnixEpoch).TotalNanoseconds, timestamp.Offset == TimeSpan.Zero ? null : timestamp.Offset.ToString())
        {
        }
    }

    /// <summary>
    /// Number of elapsed whole months.
    /// </summary>
    public sealed record IntervalYearMonth(int? Value) : ScalarValue
    {
        /// <summary>
        /// An IntervalYearMonth scalar with a null value.
        /// </summary>
        public static IntervalYearMonth Null => new((int?)null);
    }

    /// <summary>
    /// Number of elapsed days and milliseconds (no leap seconds),
    /// stored as two contiguous 32-bit signed integers.
    /// </summary>
    public sealed record IntervalDayTime(IntervalDayTimeValue? Value) : ScalarValue
    {
        /// <summary>
        /// An IntervalDayTime scalar with a null value.
        /// </summary>
        public static IntervalDayTime Null => new((IntervalDayTimeValue?)null);
    }

    /// <summary>
    /// A triple of elapsed months, days, and nanoseconds.
    /// Months and days are 32-bit signed integers; nanoseconds is a 64-bit signed integer (no leap seconds).
    /// </summary>
    public sealed record IntervalMonthDayNano(IntervalMonthDayNanoValue? Value) : ScalarValue
    {
        /// <summary>
        /// An IntervalMonthDayNano scalar with a null value.
        /// </summary>
        public static IntervalMonthDayNano Null => new((IntervalMonthDayNanoValue?)null);
    }

    /// <summary>
    /// Duration in seconds.
    /// </summary>
    public sealed record DurationSecond(long? Value) : ScalarValue
    {
        /// <summary>
        /// A DurationSecond scalar with a null value.
        /// </summary>
        public static DurationSecond Null => new((long?)null);
    }

    /// <summary>
    /// Duration in milliseconds.
    /// </summary>
    public sealed record DurationMillisecond(long? Value) : ScalarValue
    {
        /// <summary>
        /// A DurationMillisecond scalar with a null value.
        /// </summary>
        public static DurationMillisecond Null => new((long?)null);
    }

    /// <summary>
    /// Duration in microseconds.
    /// </summary>
    public sealed record DurationMicrosecond(long? Value) : ScalarValue
    {
        /// <summary>
        /// A DurationMicrosecond scalar with a null value.
        /// </summary>
        public static DurationMicrosecond Null => new((long?)null);
    }

    /// <summary>
    /// Duration in nanoseconds.
    /// </summary>
    public sealed record DurationNanosecond(long? Value) : ScalarValue
    {
        /// <summary>
        /// A DurationNanosecond scalar with a null value.
        /// </summary>
        public static DurationNanosecond Null => new((long?)null);
    }

    /// <summary>
    /// A nested datatype that can represent slots of differing types.
    /// </summary>
    /// <param name="TypeIdAndValue">A tuple of the union <c>type_id</c> and the single value held by this scalar, or <c>null</c>.</param>
    /// <param name="Fields">The union fields descriptor (zero-to-one of which will be set).</param>
    /// <param name="Mode">The physical storage mode of the source/destination <see cref="UnionArray"/>.</param>
    public sealed record Union(
        (ArrowTypeId TypeId, ScalarValue Value)? TypeIdAndValue,
        UnionType Fields,
        UnionMode Mode) : ScalarValue;

    /// <summary>
    /// Dictionary-encoded scalar: an index type and a value.
    /// </summary>
    /// <param name="IndexType">The Arrow data type of the dictionary index.</param>
    /// <param name="Value">The scalar value that the dictionary entry represents.</param>
    public sealed record Dictionary(IArrowType IndexType, ScalarValue Value) : ScalarValue;
}

/// <summary>
/// Represents a day-time interval as elapsed days and milliseconds (no leap seconds).
/// Stored as two contiguous 32-bit signed integers in Arrow.
/// </summary>
/// <param name="Days">Number of elapsed days.</param>
/// <param name="Milliseconds">Number of elapsed milliseconds within the day.</param>
public readonly record struct IntervalDayTimeValue(int Days, int Milliseconds);

/// <summary>
/// Represents a month-day-nanosecond interval.
/// Months and days are 32-bit signed integers; nanoseconds is a 64-bit signed integer (no leap seconds).
/// </summary>
/// <param name="Months">Number of elapsed months.</param>
/// <param name="Days">Number of elapsed days.</param>
/// <param name="Nanoseconds">Number of elapsed nanoseconds.</param>
public readonly record struct IntervalMonthDayNanoValue(int Months, int Days, long Nanoseconds);

internal static class ProtoScalarValueExtensions
{
    private static Proto.ScalarValue Null(Proto.ArrowType t) => new() { NullValue = t };

    internal static Proto.ScalarValue ToProto(this ScalarValue scalar) => scalar switch
    {
        ScalarValue.Boolean b => b.Value is { } v
            ? new Proto.ScalarValue { BoolValue = v }
            : Null(new Proto.ArrowType { BOOL = new Proto.EmptyMessage() }),

        ScalarValue.Float32 b => b.Value is { } v
            ? new Proto.ScalarValue { Float32Value = v }
            : Null(new Proto.ArrowType { FLOAT32 = new Proto.EmptyMessage() }),

        ScalarValue.Float64 b => b.Value is { } v
            ? new Proto.ScalarValue { Float64Value = v }
            : Null(new Proto.ArrowType { FLOAT64 = new Proto.EmptyMessage() }),

        ScalarValue.Int8 b => b.Value is { } v
            ? new Proto.ScalarValue { Int8Value = v }
            : Null(new Proto.ArrowType { INT8 = new Proto.EmptyMessage() }),

        ScalarValue.Int16 b => b.Value is { } v
            ? new Proto.ScalarValue { Int16Value = v }
            : Null(new Proto.ArrowType { INT16 = new Proto.EmptyMessage() }),

        ScalarValue.Int32 b => b.Value is { } v
            ? new Proto.ScalarValue { Int32Value = v }
            : Null(new Proto.ArrowType { INT32 = new Proto.EmptyMessage() }),

        ScalarValue.Int64 b => b.Value is { } v
            ? new Proto.ScalarValue { Int64Value = v }
            : Null(new Proto.ArrowType { INT64 = new Proto.EmptyMessage() }),

        ScalarValue.UInt8 b => b.Value is { } v
            ? new Proto.ScalarValue { Uint8Value = v }
            : Null(new Proto.ArrowType { UINT8 = new Proto.EmptyMessage() }),

        ScalarValue.UInt16 b => b.Value is { } v
            ? new Proto.ScalarValue { Uint16Value = v }
            : Null(new Proto.ArrowType { UINT16 = new Proto.EmptyMessage() }),

        ScalarValue.UInt32 b => b.Value is { } v
            ? new Proto.ScalarValue { Uint32Value = v }
            : Null(new Proto.ArrowType { UINT32 = new Proto.EmptyMessage() }),

        ScalarValue.UInt64 b => b.Value is { } v
            ? new Proto.ScalarValue { Uint64Value = v }
            : Null(new Proto.ArrowType { UINT64 = new Proto.EmptyMessage() }),

        ScalarValue.Utf8 b => b.Value is { } v
            ? new Proto.ScalarValue { Utf8Value = v }
            : Null(new Proto.ArrowType { UTF8 = new Proto.EmptyMessage() }),

        ScalarValue.Utf8View b => b.Value is { } v
            ? new Proto.ScalarValue { Utf8ViewValue = v }
            : Null(new Proto.ArrowType { UTF8VIEW = new Proto.EmptyMessage() }),

        ScalarValue.LargeUtf8 b => b.Value is { } v
            ? new Proto.ScalarValue { LargeUtf8Value = v }
            : Null(new Proto.ArrowType { LARGEUTF8 = new Proto.EmptyMessage() }),

        ScalarValue.Binary b => b.Value is { } v
            ? new Proto.ScalarValue { BinaryValue = ByteString.CopyFrom(v) }
            : Null(new Proto.ArrowType { BINARY = new Proto.EmptyMessage() }),

        ScalarValue.BinaryView b => b.Value is { } v
            ? new Proto.ScalarValue { BinaryViewValue = ByteString.CopyFrom(v) }
            : Null(new Proto.ArrowType { BINARYVIEW = new Proto.EmptyMessage() }),

        ScalarValue.LargeBinary b => b.Value is { } v
            ? new Proto.ScalarValue { LargeBinaryValue = ByteString.CopyFrom(v) }
            : Null(new Proto.ArrowType { LARGEBINARY = new Proto.EmptyMessage() }),

        ScalarValue.Date32 b => b.Value is { } v
            ? new Proto.ScalarValue { Date32Value = v }
            : Null(new Proto.ArrowType { DATE32 = new Proto.EmptyMessage() }),

        ScalarValue.Date64 b => b.Value is { } v
            ? new Proto.ScalarValue { Date64Value = v }
            : Null(new Proto.ArrowType { DATE64 = new Proto.EmptyMessage() }),

        ScalarValue.IntervalYearMonth b => b.Value is { } v
            ? new Proto.ScalarValue { IntervalYearmonthValue = v }
            : Null(new Proto.ArrowType { INTERVAL = Proto.IntervalUnit.YearMonth }),

        ScalarValue.DurationSecond b => b.Value is { } v
            ? new Proto.ScalarValue { DurationSecondValue = v }
            : Null(new Proto.ArrowType { DURATION = Proto.TimeUnit.Second }),

        ScalarValue.DurationMillisecond b => b.Value is { } v
            ? new Proto.ScalarValue { DurationMillisecondValue = v }
            : Null(new Proto.ArrowType { DURATION = Proto.TimeUnit.Millisecond }),

        ScalarValue.DurationMicrosecond b => b.Value is { } v
            ? new Proto.ScalarValue { DurationMicrosecondValue = v }
            : Null(new Proto.ArrowType { DURATION = Proto.TimeUnit.Microsecond }),

        ScalarValue.DurationNanosecond b => b.Value is { } v
            ? new Proto.ScalarValue { DurationNanosecondValue = v }
            : Null(new Proto.ArrowType { DURATION = Proto.TimeUnit.Nanosecond }),

        ScalarValue.Time32Second b => b.Value is { } v
            ? new Proto.ScalarValue { Time32Value = new Proto.ScalarTime32Value { Time32SecondValue = v } }
            : Null(new Proto.ArrowType { TIME32 = Proto.TimeUnit.Second }),

        ScalarValue.Time32Millisecond b => b.Value is { } v
            ? new Proto.ScalarValue { Time32Value = new Proto.ScalarTime32Value { Time32MillisecondValue = v } }
            : Null(new Proto.ArrowType { TIME32 = Proto.TimeUnit.Millisecond }),

        ScalarValue.Time64Microsecond b => b.Value is { } v
            ? new Proto.ScalarValue { Time64Value = new Proto.ScalarTime64Value { Time64MicrosecondValue = v } }
            : Null(new Proto.ArrowType { TIME64 = Proto.TimeUnit.Microsecond }),

        ScalarValue.Time64Nanosecond b => b.Value is { } v
            ? new Proto.ScalarValue { Time64Value = new Proto.ScalarTime64Value { Time64NanosecondValue = v } }
            : Null(new Proto.ArrowType { TIME64 = Proto.TimeUnit.Nanosecond }),

        ScalarValue.TimestampSecond b => b.Value is { } v
            ? new Proto.ScalarValue { TimestampValue = new Proto.ScalarTimestampValue { TimeSecondValue = v, Timezone = b.Timezone ?? "" } }
            : Null(new Proto.ArrowType { TIMESTAMP = new Proto.Timestamp { TimeUnit = Proto.TimeUnit.Second, Timezone = b.Timezone ?? "" } }),

        ScalarValue.TimestampMillisecond b => b.Value is { } v
            ? new Proto.ScalarValue { TimestampValue = new Proto.ScalarTimestampValue { TimeMillisecondValue = v, Timezone = b.Timezone ?? "" } }
            : Null(new Proto.ArrowType { TIMESTAMP = new Proto.Timestamp { TimeUnit = Proto.TimeUnit.Millisecond, Timezone = b.Timezone ?? "" } }),

        ScalarValue.TimestampMicrosecond b => b.Value is { } v
            ? new Proto.ScalarValue { TimestampValue = new Proto.ScalarTimestampValue { TimeMicrosecondValue = v, Timezone = b.Timezone ?? "" } }
            : Null(new Proto.ArrowType { TIMESTAMP = new Proto.Timestamp { TimeUnit = Proto.TimeUnit.Microsecond, Timezone = b.Timezone ?? "" } }),

        ScalarValue.TimestampNanosecond b => b.Value is { } v
            ? new Proto.ScalarValue { TimestampValue = new Proto.ScalarTimestampValue { TimeNanosecondValue = v, Timezone = b.Timezone ?? "" } }
            : Null(new Proto.ArrowType { TIMESTAMP = new Proto.Timestamp { TimeUnit = Proto.TimeUnit.Nanosecond, Timezone = b.Timezone ?? "" } }),

        ScalarValue.IntervalDayTime b => b.Value is { } v
            ? new Proto.ScalarValue { IntervalDaytimeValue = new Proto.IntervalDayTimeValue { Days = v.Days, Milliseconds = v.Milliseconds } }
            : Null(new Proto.ArrowType { INTERVAL = Proto.IntervalUnit.DayTime }),

        ScalarValue.IntervalMonthDayNano b => b.Value is { } v
            ? new Proto.ScalarValue { IntervalMonthDayNano = new Proto.IntervalMonthDayNanoValue { Months = v.Months, Days = v.Days, Nanos = v.Nanoseconds } }
            : Null(new Proto.ArrowType { INTERVAL = Proto.IntervalUnit.MonthDayNano }),

        ScalarValue.FixedSizeBinary b => b.Value is { } v
            ? new Proto.ScalarValue { FixedSizeBinaryValue = new Proto.ScalarFixedSizeBinary { Values = ByteString.CopyFrom(v), Length = b.Size } }
            : Null(new Proto.ArrowType { FIXEDSIZEBINARY = b.Size }),

        ScalarValue.Decimal128 b => b.Value is { } v
            ? new Proto.ScalarValue { Decimal128Value = ToProtoDecimal128(v, b.Precision) }
            : Null(new Proto.ArrowType { DECIMAL = new Proto.Decimal { Precision = b.Precision, Scale = 0 } }),

        ScalarValue.List b => new Proto.ScalarValue { ListValue = ToScalarNestedValue(b.Array) },
        ScalarValue.LargeList b => new Proto.ScalarValue { LargeListValue = ToScalarNestedValue(b.Array) },
        ScalarValue.FixedSizeList b => new Proto.ScalarValue { FixedSizeListValue = ToScalarNestedValue(b.Array) },
        ScalarValue.Struct b => new Proto.ScalarValue { StructValue = ToScalarNestedValue(b.Array) },
        ScalarValue.Map b => new Proto.ScalarValue { MapValue = ToScalarNestedValue(b.Array) },

        ScalarValue.Union b => new Proto.ScalarValue { UnionValue = ToUnionValue(b) },

        ScalarValue.Dictionary b => new Proto.ScalarValue
        {
            DictionaryValue = new Proto.ScalarDictionaryValue
            {
                IndexType = b.IndexType.ToProto(),
                Value = b.Value.ToProto()
            }
        },

        _ => throw new ArgumentOutOfRangeException(nameof(scalar), scalar.GetType().Name, "Unsupported ScalarValue type")
    };

    private static Proto.UnionValue ToUnionValue(ScalarValue.Union v)
    {
        var unionValue = new Proto.UnionValue
        {
            ValueId = v.TypeIdAndValue is { } tv ? (byte)tv.TypeId : 128,
            Mode = v.Mode switch
            {
                UnionMode.Dense => Proto.UnionMode.Dense,
                UnionMode.Sparse => Proto.UnionMode.Sparse,
                _ => throw new ArgumentOutOfRangeException(nameof(v), v.Mode, "Unknown UnionMode")
            }
        };

        if (v.TypeIdAndValue is { Value: var innerValue })
            unionValue.Value = innerValue.ToProto();

        unionValue.Fields.AddRange(
            v.Fields.Fields.Zip(v.Fields.TypeIds, (f, id) =>
                new Proto.UnionField { FieldId = id, Field = ProtoArrowExtensions.FieldToProto(f) }));

        return unionValue;
    }

    private static Proto.ScalarNestedValue ToScalarNestedValue(IArrowArray array)
    {
        var schema = new Schema([new Field("item", array.Data.DataType, true)], null);
        
        using var batch = new RecordBatch(schema, [array], array.Length);

        using var ms = new MemoryStream();
        using var writer = new ArrowStreamWriter(ms, schema);
        writer.WriteRecordBatch(batch);
        writer.WriteEnd();

        var bytes = ms.GetBuffer();
        var bytesLength = (int) ms.Length;
        var offset = 0;

        // Skip schema IPC message: [4-byte continuation][4-byte len][flatbuffer][padding]
        offset += 4; // continuation marker
        var schemaLen = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset));
        offset += 4;
        offset += schemaLen + (8 - schemaLen % 8) % 8;

        // Extract record batch IPC message: [4-byte continuation][4-byte len][ipc_message][padding]
        offset += 4; // continuation marker
        var recBatchLen = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset));
        offset += 4;
        var ipcMessage = bytes[offset..(offset + recBatchLen)];
        offset += recBatchLen + (8 - recBatchLen % 8) % 8;

        // Body data: remaining bytes minus 8-byte EOS marker
        var bodyLength = bytesLength - offset - 8;
        var arrowData = bodyLength > 0 ? bytes[offset..(offset + bodyLength)] : [];

        return new Proto.ScalarNestedValue
        {
            IpcMessage = ByteString.CopyFrom(ipcMessage),
            ArrowData = ByteString.CopyFrom(arrowData),
            Schema = schema.ToProto()
        };
    }

    private static Proto.Decimal128 ToProtoDecimal128(decimal value, byte precision)
    {
        Span<byte> raw = stackalloc byte[16];
        MemoryMarshal.Write(raw, in value);
        // .NET decimal memory layout: [flags(4), hi32(4), lo64(8)]
        // flags: sign = bit 31 (byte 3), scale = bits 16-23 (byte 2)
        var negative = (raw[3] & 0x80) != 0;
        var scale = (sbyte)raw[2];

        UInt128 mantissa = new(
            BinaryPrimitives.ReadUInt32LittleEndian(raw[4..8]),   // hi32
            BinaryPrimitives.ReadUInt64LittleEndian(raw[8..16])); // lo64 = lo | (mid << 32)
        var unscaled = negative ? -(Int128)mantissa : (Int128)mantissa;

        Span<byte> bytes = stackalloc byte[16];
        MemoryMarshal.Write(bytes, in unscaled);

        return new Proto.Decimal128 { Value = ByteString.CopyFrom(bytes), P = precision, S = scale };
    }
}
