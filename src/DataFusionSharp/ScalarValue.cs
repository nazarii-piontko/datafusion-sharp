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

using Apache.Arrow;
using Apache.Arrow.Types;

namespace DataFusionSharp;

/// <summary>
/// Represents a single scalar value in DataFusion.
/// This is the C# equivalent of the Rust <c>datafusion::scalar::ScalarValue</c> enum.
/// Each variant is modelled as a sealed derived record.
/// </summary>
public abstract record ScalarValue
{
    /// <summary>
    /// Represents <c>DataType::Null</c> (castable to/from any other type).
    /// </summary>
    public sealed record Null : ScalarValue;

    /// <summary>
    /// A true or false value.
    /// </summary>
    public sealed record Boolean(bool? Value) : ScalarValue
    {
        /// <summary>
        /// Implicit conversion from bool to Boolean for convenience.
        /// </summary>
        /// <param name="value">The bool value to convert.</param>
        /// <returns>>A Boolean scalar with the given value.</returns>
        public static implicit operator Boolean(bool value) => new(value);
    }

    /// <summary>
    /// 16-bit floating-point value.
    /// </summary>
    public sealed record Float16(Half? Value) : ScalarValue
    {
        /// <summary>
        /// Implicit conversion from Half to Float16 for convenience.
        /// </summary>
        /// <param name="value">The Half value to convert.</param>
        /// <returns>A Float16 scalar with the given value.</returns>
        public static implicit operator Float16(Half value) => new(value);
    }

    /// <summary>
    /// 32-bit floating-point value.
    /// </summary>
    public sealed record Float32(float? Value) : ScalarValue
    {
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
        /// Implicit conversion from double to Float64 for convenience.
        /// </summary>
        /// <param name="value">The double value to convert.</param>
        /// <returns>A Float64 scalar with the given value.</returns>
        public static implicit operator Float64(double value) => new(value);
    }

    /// <summary>
    /// General decimal value.
    /// </summary>
    public abstract record Decimal : ScalarValue
    {
        /// <summary>
        /// The decimal value, or <c>null</c> if the scalar is null.
        /// </summary>
        public decimal? Value { get; init; }
        
        /// <summary>
        /// The total number of digits (1–9).
        /// </summary>
        public byte Precision { get; init; }
        
        /// <summary>
        /// The number of digits to the right of the decimal point.
        /// </summary>
        public sbyte Scale { get; init; }
        
        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="value">The decimal value, or <c>null</c> if the scalar is null.</param>
        /// <param name="precision">The total number of digits.</param>
        /// <param name="scale">The number of digits to the right of the decimal point.</param>
        protected internal Decimal(decimal? value, byte precision, sbyte scale)
        {
            Value = value;
            Precision = precision;
            Scale = scale;
            
#pragma warning disable CA2214
            // ReSharper disable once VirtualMemberCallInConstructor
            Validate();
#pragma warning restore CA2214
        }
        
        /// <summary>
        /// Validates that the precision and scale are within the allowed ranges for the specific decimal type.
        /// </summary>
        protected abstract void Validate();
    }
    
    /// <summary>
    /// 32-bit decimal value.
    /// </summary>
    /// <param name="Value">The decimal value, or <c>null</c> if the scalar is null.</param>
    /// <param name="Precision">The total number of digits (1–9).</param>
    /// <param name="Scale">The number of digits to the right of the decimal point.</param>
    public sealed record Decimal32(decimal? Value, byte Precision, sbyte Scale) : Decimal(Value, Precision, Scale)
    {
        /// <inheritdoc />
        protected override void Validate()
        {
            if (Precision is < 1 or > 9)
                throw new ArgumentOutOfRangeException(nameof(Precision), "Precision must be between 1 and 9 for Decimal32.");
            if (Scale < 0 || Scale > Precision)
                throw new ArgumentOutOfRangeException(nameof(Scale), "Scale must be between 0 and Precision for Decimal32.");
        }
    }

    /// <summary>
    /// 64-bit decimal value.
    /// </summary>
    /// <param name="Value">The decimal value, or <c>null</c> if the scalar is null.</param>
    /// <param name="Precision">The total number of digits (1–18).</param>
    /// <param name="Scale">The number of digits to the right of the decimal point.</param>
    public sealed record Decimal64(decimal? Value, byte Precision, sbyte Scale) : Decimal(Value, Precision, Scale)
    {
        /// <inheritdoc />
        protected override void Validate()
        {
            if (Precision is < 1 or > 18)
                throw new ArgumentOutOfRangeException(nameof(Precision), "Precision must be between 1 and 18 for Decimal64.");
            if (Scale < 0 || Scale > Precision)
                throw new ArgumentOutOfRangeException(nameof(Scale), "Scale must be between 0 and Precision for Decimal64.");
        }
    }

    /// <summary>
    /// 128-bit decimal value.
    /// </summary>
    /// <param name="Value">The decimal value, or <c>null</c> if the scalar is null.</param>
    /// <param name="Precision">The total number of digits (1–38).</param>
    /// <param name="Scale">The number of digits to the right of the decimal point.</param>
    public sealed record Decimal128(decimal? Value, byte Precision, sbyte Scale) : Decimal(Value, Precision, Scale)
    {
        /// <inheritdoc />
        protected override void Validate()
        {
            if (Precision is < 1 or > 38)
                throw new ArgumentOutOfRangeException(nameof(Precision), "Precision must be between 1 and 38 for Decimal128.");
            if (Scale < 0 || Scale > Precision)
                throw new ArgumentOutOfRangeException(nameof(Scale), "Scale must be between 0 and Precision for Decimal128.");
        }
        
        ///<summary>
        /// Implicit conversion from decimal to Decimal128 for convenience.
        /// </summary>
        /// <param name="value">The decimal value to convert.</param>
        /// <returns>A Decimal128 scalar with the given value and maximum precision/scale.</returns>
        public static implicit operator Decimal128(decimal value) => new(value, 38, 38);
    }
    
    /// <summary>
    /// 256-bit decimal value.
    /// </summary>
    /// <param name="Value">The decimal value, or <c>null</c> if the scalar is null.</param>
    /// <param name="Precision">The total number of digits (1–76).</param>
    /// <param name="Scale">The number of digits to the right of the decimal point.</param>
    public sealed record Decimal256(decimal? Value, byte Precision, sbyte Scale) : Decimal(Value, Precision, Scale)
    {
        /// <inheritdoc />
        protected override void Validate()
        {
            if (Precision is < 1 or > 76)
                throw new ArgumentOutOfRangeException(nameof(Precision), "Precision must be between 1 and 76 for Decimal256.");
            if (Scale < 0 || Scale > Precision)
                throw new ArgumentOutOfRangeException(nameof(Scale), "Scale must be between 0 and Precision for Decimal256.");
        }
    }

    /// <summary>
    /// Signed 8-bit integer.
    /// </summary>
    public sealed record Int8(sbyte? Value) : ScalarValue
    {
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
        /// Implicit conversion from string to Utf8 for convenience.
        /// </summary>
        /// <param name="value">The string value to convert.</param>
        /// <returns>An Utf8 scalar with the given value.</returns>
        public static implicit operator Utf8(string value) => new(value);
    }

    /// <summary>
    /// UTF-8 encoded string from view types.
    /// </summary>
    public sealed record Utf8View(string? Value) : ScalarValue;

    /// <summary>
    /// UTF-8 encoded string representing a LargeString's Arrow type.
    /// </summary>
    public sealed record LargeUtf8(string? Value) : ScalarValue;

    /// <summary>
    /// Binary data.
    /// </summary>
    public sealed record Binary(byte[]? Value) : ScalarValue
    {
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
    public sealed record BinaryView(byte[]? Value) : ScalarValue;

    /// <summary>
    /// Fixed-size binary data.
    /// </summary>
    /// <param name="Size">The fixed byte-width of each value.</param>
    /// <param name="Value">The binary data, or <c>null</c> if the scalar is null.</param>
    public sealed record FixedSizeBinary(int Size, byte[]? Value) : ScalarValue;

    /// <summary>
    /// Large binary data.
    /// </summary>
    public sealed record LargeBinary(byte[]? Value) : ScalarValue;

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
        /// <inheritdoc />
        public TimestampNanosecond(DateTimeOffset timestamp)
            : this((long)(timestamp - DateTimeOffset.UnixEpoch).TotalNanoseconds, timestamp.Offset == TimeSpan.Zero ? null : timestamp.Offset.ToString())
        {
        }
    }

    /// <summary>
    /// Number of elapsed whole months.
    /// </summary>
    public sealed record IntervalYearMonth(int? Value) : ScalarValue;

    /// <summary>
    /// Number of elapsed days and milliseconds (no leap seconds),
    /// stored as two contiguous 32-bit signed integers.
    /// </summary>
    public sealed record IntervalDayTime(IntervalDayTimeValue? Value) : ScalarValue;

    /// <summary>
    /// A triple of elapsed months, days, and nanoseconds.
    /// Months and days are 32-bit signed integers; nanoseconds is a 64-bit signed integer (no leap seconds).
    /// </summary>
    public sealed record IntervalMonthDayNano(IntervalMonthDayNanoValue? Value) : ScalarValue;
    
    /// <summary>
    /// Duration in seconds.
    /// </summary>
    public sealed record DurationSecond(long? Value) : ScalarValue;

    /// <summary>
    /// Duration in milliseconds.
    /// </summary>
    public sealed record DurationMillisecond(long? Value) : ScalarValue;

    /// <summary>
    /// Duration in microseconds.
    /// </summary>
    public sealed record DurationMicrosecond(long? Value) : ScalarValue;

    /// <summary>
    /// Duration in nanoseconds.
    /// </summary>
    public sealed record DurationNanosecond(long? Value) : ScalarValue;

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
