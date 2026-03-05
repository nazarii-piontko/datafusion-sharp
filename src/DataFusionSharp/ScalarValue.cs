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
using System.Numerics;
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
        public static Boolean Null => new((bool?)null);
    }
    
    /// <summary>
    /// Implicit conversion from bool to Boolean for convenience.
    /// </summary>
    /// <param name="value">The bool value to convert.</param>
    /// <returns>>A Boolean scalar with the given value.</returns>
    public static implicit operator ScalarValue(bool? value) => new Boolean(value);

    /// <summary>
    /// 32-bit floating-point value.
    /// </summary>
    public sealed record Float32(float? Value) : ScalarValue
    {
        /// <summary>
        /// A Float32 scalar with a null value.
        /// </summary>
        public static Float32 Null => new((float?)null);
    }
    
    /// <summary>
    /// Implicit conversion from float to Float32 for convenience.
    /// </summary>
    /// <param name="value">The float value to convert.</param>
    /// <returns>A Float32 scalar with the given value.</returns>
    public static implicit operator ScalarValue(float? value) => new Float32(value);

    /// <summary>
    /// 64-bit floating-point value.
    /// </summary>
    public sealed record Float64(double? Value) : ScalarValue
    {
        /// <summary>
        /// A Float64 scalar with a null value.
        /// </summary>
        public static Float64 Null => new((double?)null);
    }
    
    /// <summary>
    /// Implicit conversion from double to Float64 for convenience.
    /// </summary>
    /// <param name="value">The double value to convert.</param>
    /// <returns>A Float64 scalar with the given value.</returns>
    public static implicit operator ScalarValue(double? value) => new Float64(value);

    /// <summary>
    /// 128-bit decimal value. Scale is inferred from <see cref="Value"/> itself.
    /// </summary>
    public sealed record Decimal128(decimal? Value, byte Precision, byte Scale) : ScalarValue
    {
        /// <summary>
        /// A Decimal128 scalar with a null value.
        /// </summary>
        public static Decimal128 Null => new(null, 1, 0);
    }
    
    /// <summary>
    /// Implicit conversion from decimal to Decimal128 for convenience.
    /// </summary>
    /// <param name="value">The decimal value to convert.</param>
    /// <returns>A Decimal128 scalar with the given value.</returns>
    public static implicit operator ScalarValue(decimal? value) => new Decimal128(value, Math.Max((byte)1, value?.Scale ?? 1), value?.Scale ?? 0);
    
    /// <summary>
    /// 256-bit decimal value. Scale is inferred from <see cref="Value"/> itself.
    /// </summary>
    public sealed record Decimal256(decimal? Value, byte Precision, byte Scale) : ScalarValue
    {
        /// <summary>
        /// A Decimal256 scalar with a null value.
        /// </summary>
        public static Decimal256 Null => new(null, 1, 0);
    }

    /// <summary>
    /// Signed 8-bit integer.
    /// </summary>
    public sealed record Int8(sbyte? Value) : ScalarValue
    {
        /// <summary>
        /// An Int8 scalar with a null value.
        /// </summary>
        public static Int8 Null => new((sbyte?)null);
    }
    
    /// <summary>
    /// Implicit conversion from sbyte to Int8 for convenience.
    /// </summary>
    /// <param name="value">The sbyte value to convert.</param>
    /// <returns>An Int8 scalar with the given value.</returns>
    public static implicit operator ScalarValue(sbyte? value) => new Int8(value);

    /// <summary>
    /// Signed 16-bit integer.
    /// </summary>
    public sealed record Int16(short? Value) : ScalarValue
    {
        /// <summary>
        /// An Int16 scalar with a null value.
        /// </summary>
        public static Int16 Null => new((short?)null);
    }
    
    /// <summary>
    /// Implicit conversion from short to Int16 for convenience.
    /// </summary>
    /// <param name="value">The short value to convert.</param>
    /// <returns>An Int16 scalar with the given value.</returns>
    public static implicit operator ScalarValue(short? value) => new Int16(value);

    /// <summary>
    /// Signed 32-bit integer.
    /// </summary>
    public sealed record Int32(int? Value) : ScalarValue
    {
        /// <summary>
        /// An Int32 scalar with a null value.
        /// </summary>
        public static Int32 Null => new((int?)null);
    }
    
    /// <summary>
    /// Implicit conversion from int to Int32 for convenience.
    /// </summary>
    /// <param name="value">The int value to convert.</param>
    /// <returns>An Int32 scalar with the given value.</returns>
    public static implicit operator ScalarValue(int? value) => new Int32(value);

    /// <summary>
    /// Signed 64-bit integer.
    /// </summary>
    public sealed record Int64(long? Value) : ScalarValue
    {
        /// <summary>
        /// An Int64 scalar with a null value.
        /// </summary>
        public static Int64 Null => new((long?)null);
    }
    
    /// <summary>
    /// Implicit conversion from long to Int64 for convenience.
    /// </summary>
    /// <param name="value">The long value to convert.</param>
    /// <returns>An Int64 scalar with the given value.</returns>
    public static implicit operator ScalarValue(long? value) => new Int64(value);
    
    /// <summary>
    /// Implicit conversion from long to Int64 for convenience.
    /// </summary>
    /// <param name="value">The long value to convert.</param>
    /// <returns>An Int64 scalar with the given value.</returns>
    public static implicit operator ScalarValue(long value) => new Int64(value);

    /// <summary>
    /// Unsigned 8-bit integer.
    /// </summary>
    public sealed record UInt8(byte? Value) : ScalarValue
    {
        /// <summary>
        /// A UInt8 scalar with a null value.
        /// </summary>
        public static UInt8 Null => new((byte?)null);
    }
    
    /// <summary>
    /// Implicit conversion from byte to UInt8 for convenience.
    /// </summary>
    /// <param name="value">The byte value to convert.</param>
    /// <returns>A UInt8 scalar with the given value.</returns>
    public static implicit operator ScalarValue(byte? value) => new UInt8(value);

    /// <summary>
    /// Unsigned 16-bit integer.
    /// </summary>
    public sealed record UInt16(ushort? Value) : ScalarValue
    {
        /// <summary>
        /// A UInt16 scalar with a null value.
        /// </summary>
        public static UInt16 Null => new((ushort?)null);
    }
    
    /// <summary>
    /// Implicit conversion from ushort to UInt16 for convenience.
    /// </summary>
    /// <param name="value">The ushort value to convert.</param>
    /// <returns>A UInt16 scalar with the given value.</returns>
    public static implicit operator ScalarValue(ushort? value) => new UInt16(value);

    /// <summary>
    /// Unsigned 32-bit integer.
    /// </summary>
    public sealed record UInt32(uint? Value) : ScalarValue
    {
        /// <summary>
        /// A UInt32 scalar with a null value.
        /// </summary>
        public static UInt32 Null => new((uint?)null);
    }
    
    /// <summary>
    /// Implicit conversion from uint to UInt32 for convenience.
    /// </summary>
    /// <param name="value">The uint value to convert.</param>
    /// <returns>A UInt32 scalar with the given value.</returns>
    public static implicit operator ScalarValue(uint? value) => new UInt32(value);

    /// <summary>
    /// Unsigned 64-bit integer.
    /// </summary>
    public sealed record UInt64(ulong? Value) : ScalarValue
    {
        /// <summary>
        /// A UInt64 scalar with a null value.
        /// </summary>
        public static UInt64 Null => new((ulong?)null);
    }
    
    /// <summary>
    /// Implicit conversion from ulong to UInt64 for convenience.
    /// </summary>
    /// <param name="value">The ulong value to convert.</param>
    /// <returns>A UInt64 scalar with the given value.</returns>
    public static implicit operator ScalarValue(ulong? value) => new UInt64(value);

    /// <summary>
    /// UTF-8 encoded string.
    /// </summary>
    public sealed record Utf8(string? Value) : ScalarValue
    {
        /// <summary>
        /// A Utf8 scalar with a null value.
        /// </summary>
        public static Utf8 Null => new((string?)null);
    }
    
    /// <summary>
    /// Implicit conversion from string to Utf8 for convenience.
    /// </summary>
    /// <param name="value">The string value to convert.</param>
    /// <returns>An Utf8 scalar with the given value.</returns>
    public static implicit operator ScalarValue(string value) => new Utf8(value);

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
        public static Binary Null => new((byte[]?)null);
    }
    
    /// <summary>
    /// Implicit conversion from byte array to Binary for convenience.
    /// </summary>
    /// <param name="value">The byte array value to convert.</param>
    /// <returns>A Binary scalar with the given value.</returns>
    public static implicit operator ScalarValue(byte[] value) => new Binary(value);

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
    public sealed record FixedSizeBinary(int Size, byte[]? Value) : ScalarValue
    {
        /// <summary>
        /// A FixedSizeBinary scalar with a null value.
        /// </summary>
        public static FixedSizeBinary Null => new(0, null);
    }

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
    /// Date stored as a signed 32-bit int — days since UNIX epoch 1970-01-01.
    /// </summary>
    public sealed record Date32(int? Value) : ScalarValue
    {
        /// <summary>
        /// A Date32 scalar with a null value.
        /// </summary>
        public static Date32 Null => new((int?)null);

        /// <inheritdoc />
        public Date32(DateOnly? date)
            : this(date != null
                ? (int?)(date.Value.ToDateTime(TimeOnly.MinValue) - DateTime.UnixEpoch).TotalDays
                : null)
        {
        }
    }
    
    /// <summary>
    /// Implicit conversion from DateOnly to Date32 for convenience.
    /// </summary>
    /// <param name="value">The DateOnly value to convert.</param>
    /// <returns>A Date32 scalar with the given value.</returns>
    public static implicit operator ScalarValue(DateOnly? value) => new Date32(value);

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
        public Date64(DateOnly? date)
            : this(date != null
                ? (long)(date.Value.ToDateTime(TimeOnly.MinValue) - DateTime.UnixEpoch).TotalMilliseconds
                : null)
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
        public Time32Second(TimeOnly? time)
            : this(time?.ToTimeSpan().Seconds)
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
        public Time32Millisecond(TimeOnly? time)
            : this(time?.ToTimeSpan().Milliseconds)
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
        public Time64Microsecond(TimeOnly? time)
            : this(time?.ToTimeSpan().Ticks / 10) // 1 tick = 100 nanoseconds = 0.1 microseconds
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
        public Time64Nanosecond(TimeOnly? time)
            : this(time?.ToTimeSpan().Ticks * 100) // 1 tick = 100 nanoseconds
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
        public TimestampSecond(DateTimeOffset? timestamp)
            : this(timestamp != null
                    ? (long)(timestamp.Value - DateTimeOffset.UnixEpoch).TotalSeconds
                    : null,
                timestamp != null
                    ? timestamp.Value.Offset == TimeSpan.Zero ? null : timestamp.Value.Offset.ToString()
                    : null)
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
        public TimestampMillisecond(DateTimeOffset? timestamp)
            : this(timestamp != null
                    ? (long)(timestamp.Value - DateTimeOffset.UnixEpoch).TotalMilliseconds
                    : null,
                timestamp != null
                    ? timestamp.Value.Offset == TimeSpan.Zero ? null : timestamp.Value.Offset.ToString()
                    : null)
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
        public TimestampMicrosecond(DateTimeOffset? timestamp)
            : this(timestamp != null
                    ? (long)(timestamp.Value - DateTimeOffset.UnixEpoch).TotalMicroseconds
                    : null,
                timestamp != null
                    ? timestamp.Value.Offset == TimeSpan.Zero ? null : timestamp.Value.Offset.ToString()
                    : null)
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
        public TimestampNanosecond(DateTimeOffset? timestamp)
            : this(timestamp != null
                    ? (long)(timestamp.Value - DateTimeOffset.UnixEpoch).TotalNanoseconds
                    : null,
                timestamp != null
                    ? timestamp.Value.Offset == TimeSpan.Zero ? null : timestamp.Value.Offset.ToString()
                    : null)
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
}

/// <summary>
/// A wrapper for a <see cref="ScalarValue"/> with optional metadata.
/// </summary>
/// <param name="Value">The scalar value.</param>
/// <param name="Metadata">Optional metadata as key-value pairs.</param>
public record ScalarValueAndMetadata(ScalarValue Value, Dictionary<string, string>? Metadata = null)
{
    /// <summary>
    /// Implicit conversion from ScalarValue to ScalarValueAndMetadata for convenience.
    /// </summary>
    /// <param name="value">The scalar value to convert.</param>
    /// <returns>A <see cref="ScalarValueAndMetadata" /> instance with the given value and no metadata.</returns>
    public static implicit operator ScalarValueAndMetadata(ScalarValue value) => new(value);
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
            ? new Proto.ScalarValue { Decimal128Value = ToProtoDecimal128(v, b.Precision, b.Scale) }
            : Null(new Proto.ArrowType { DECIMAL = new Proto.Decimal { Precision = b.Precision, Scale = b.Scale } }),
        
        ScalarValue.Decimal256 b => b.Value is { } v
            ? new Proto.ScalarValue { Decimal256Value = ToProtoDecimal256(v, b.Precision, b.Scale) }
            : Null(new Proto.ArrowType { DECIMAL256 = new Proto.Decimal256Type { Precision = b.Precision, Scale = b.Scale } }),

        _ => throw new ArgumentOutOfRangeException(nameof(scalar), scalar.GetType().Name, "Unsupported ScalarValue type")
    };

    private static Proto.Decimal128 ToProtoDecimal128(decimal value, byte precision, byte scale)
    {
        Span<byte> bytes = stackalloc byte[16];
        GetDecimalBytes(value, precision, scale, bytes.Length, bytes);
        return new Proto.Decimal128 { Value = ByteString.CopyFrom(bytes), S = scale, P = precision };
    }
    
    private static Proto.Decimal256 ToProtoDecimal256(decimal value, byte precision, byte scale)
    {
        Span<byte> bytes = stackalloc byte[32];
        GetDecimalBytes(value, precision, scale, bytes.Length, bytes);
        return new Proto.Decimal256 { Value = ByteString.CopyFrom(bytes), S = scale, P = precision };
    }
    
    /// <summary>
    /// Converts a decimal value to its byte representation. Inspired by Apache.Arrow DecimalUtility.GetBytes.
    /// </summary>
    private static void GetDecimalBytes(decimal value, int precision, int scale, int byteWidth, Span<byte> bytes)
    {
        Span<int> decimalBits = stackalloc int[4];
        decimal.GetBits(value, decimalBits);

        var decScale = (decimalBits[3] >> 16) & 0x7F;
        Span<byte> bigIntBytes = stackalloc byte[13];

        Span<byte> intBytes = stackalloc byte[4];
        for (var i = 0; i < 3; i++)
        {
            var bit = decimalBits[i];
            if (!BitConverter.TryWriteBytes(intBytes, bit))
                throw new OverflowException($"Could not extract bytes from int {bit}");

            BinaryPrimitives.WriteInt32LittleEndian(bigIntBytes[(4 * i)..], decimalBits[i]);
        }
        var bigInt = new BigInteger(bigIntBytes);

        if (value < 0)
            bigInt = -bigInt;
        
        if (decScale > scale)
            throw new OverflowException($"Decimal scale cannot be greater than that in the Arrow vector: {decScale} != {scale}");
        
        if (bigInt >= BigInteger.Pow(10, precision))
            throw new OverflowException($"Decimal precision cannot be greater than that in the Arrow vector: {value} has precision > {precision}");

        if (decScale < scale)
            bigInt *= BigInteger.Pow(10, scale - decScale);
        
        if (bytes.Length != byteWidth)
            throw new OverflowException($"ValueBuffer size not equal to {byteWidth} byte width: {bytes.Length}");
        
        if (!bigInt.TryWriteBytes(bytes, out var bytesWritten))
            throw new OverflowException("Could not extract bytes from integer value " + bigInt);

        if (bytes.Length > byteWidth)
            throw new OverflowException($"Decimal size greater than {byteWidth} bytes: {bytes.Length}");

        if (bigInt.Sign == -1)
            for (var i = bytesWritten; i < byteWidth; i++)
                bytes[i] = 0xFF;
        
        // DataFusion expects big-endian byte order
        for (var i = 0; i < byteWidth / 2; ++i)
            (bytes[i], bytes[byteWidth - 1 - i]) = (bytes[byteWidth - 1 - i], bytes[i]);
    }
    
    internal static Proto.ScalarValueAndMetadata ToProto(this ScalarValueAndMetadata scalarAndMeta)
    {
        var proto = new Proto.ScalarValueAndMetadata
        {
            Value = scalarAndMeta.Value.ToProto()
        };
        
        if (scalarAndMeta.Metadata is { Count: > 0 })
            proto.Metadata.Add(scalarAndMeta.Metadata);

        return proto;
    }

    internal static Proto.DataFrameParamValues ToProto(this IEnumerable<NamedScalarValueAndMetadata> parameters)
    {
        var namedParams = new Proto.DataFrameNamedParamValues();
        foreach (var p in parameters)
            namedParams.Values.Add(p.Name, p.Value.ToProto());
        return new Proto.DataFrameParamValues { Named = namedParams };
    }
}
