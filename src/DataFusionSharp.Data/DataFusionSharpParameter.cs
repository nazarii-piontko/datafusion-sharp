using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace DataFusionSharp.Data;

/// <summary>
/// Represents a named parameter for a <see cref="DataFusionSharpCommand"/> SQL query.
/// </summary>
/// <remarks>
/// Parameter names may use any common ADO.NET prefix (<c>@</c>, <c>$</c>, or <c>:</c>);
/// all prefixes are stripped internally before the parameter is forwarded to DataFusion.
/// The SQL command text should use either the original prefix or the DataFusion-native <c>$</c> prefix — both are accepted.
/// </remarks>
public sealed class DataFusionSharpParameter : DbParameter
{
    private string _parameterName = string.Empty;

    /// <summary>
    /// Initializes a new, empty parameter.
    /// </summary>
    public DataFusionSharpParameter()
    {
    }

    /// <summary>
    /// Initializes a parameter with the given name and value.
    /// </summary>
    public DataFusionSharpParameter(string parameterName, object? value)
    {
        ParameterName = parameterName;
        Value = value;
    }

    /// <summary>
    /// Initializes a parameter with the given name and explicit DB type.
    /// </summary>
    public DataFusionSharpParameter(string parameterName, DbType dbType)
    {
        ParameterName = parameterName;
        DbType = dbType;
    }

    /// <inheritdoc />
    public override DbType DbType { get; set; } = DbType.AnsiString;

    /// <inheritdoc />
    public override ParameterDirection Direction
    {
        get => ParameterDirection.Input;
        set
        {
            if (value != ParameterDirection.Input)
                throw new NotSupportedException("DataFusion parameters only support ParameterDirection.Input.");
        }
    }

    /// <inheritdoc />
    public override bool IsNullable { get; set; } = true;

    /// <inheritdoc />
    [AllowNull]
    public override string ParameterName
    {
        get => _parameterName;
        set => _parameterName = value ?? string.Empty;
    }

    /// <inheritdoc />
    [AllowNull]
    public override string SourceColumn { get; set; }

    /// <inheritdoc />
    public override object? Value { get; set; }

    /// <inheritdoc />
    public override bool SourceColumnNullMapping { get; set; }

    /// <inheritdoc />
    public override int Size { get; set; }

    /// <inheritdoc />
    public override void ResetDbType() => DbType = DbType.AnsiString;

    /// <summary>
    /// Returns the parameter name with any leading prefix characters (<c>@</c>, <c>$</c>, <c>:</c>) stripped.
    /// </summary>
    internal string NormalizedName => _parameterName.TrimStart('@', '$', ':');

    /// <summary>
    /// Converts the current <see cref="Value"/> to the corresponding DataFusion <see cref="ScalarValue"/>.
    /// Common .NET primitive types are mapped to their DataFusion equivalents;
    /// unknown types are serialized to a UTF-8 string via <see cref="object.ToString"/>.
    /// </summary>
    internal ScalarValue ToScalarValue()
    {
        if (Value is null or DBNull)
            return NullToScalarValue();

        return Value switch
        {
            bool b => new ScalarValue.Boolean(b),
            sbyte sb => new ScalarValue.Int8(sb),
            short s => new ScalarValue.Int16(s),
            int i => new ScalarValue.Int32(i),
            long l => new ScalarValue.Int64(l),
            byte b => new ScalarValue.UInt8(b),
            ushort us => new ScalarValue.UInt16(us),
            uint ui => new ScalarValue.UInt32(ui),
            ulong ul => new ScalarValue.UInt64(ul),
            float f => new ScalarValue.Float32(f),
            double d => new ScalarValue.Float64(d),
            decimal dec => new ScalarValue.Decimal128(dec),
            string s => new ScalarValue.Utf8(s),
            char c => new ScalarValue.Utf8(c.ToString()),
            byte[] ba => new ScalarValue.Binary(ba),
            DateOnly date => new ScalarValue.Date32(date),
            TimeOnly time => new ScalarValue.Time64Microsecond(time),
            DateTime dt => new ScalarValue.TimestampMicrosecond(new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))),
            DateTimeOffset dto => new ScalarValue.TimestampMicrosecond(dto),
            TimeSpan ts => new ScalarValue.DurationMicrosecond(ts),
            ScalarValue s => s,
            _ => new ScalarValue.Utf8(Value.ToString())
        };
    }

    private ScalarValue NullToScalarValue()
    {
        return DbType switch
        {
            DbType.Binary => ScalarValue.Binary.Null,
            DbType.Boolean => ScalarValue.Boolean.Null,
            DbType.Byte => ScalarValue.UInt8.Null,
            DbType.Currency => throw new NotSupportedException("Cannot infer DataFusion type for DbType.Currency parameters with null value."),
            DbType.Date => ScalarValue.Date64.Null,
            DbType.DateTime => ScalarValue.Time64Microsecond.Null,
            DbType.DateTime2 => throw new NotSupportedException("Cannot infer DataFusion type for DbType.DateTime2 parameters with null value."),
            DbType.DateTimeOffset => ScalarValue.Time64Microsecond.Null,
            DbType.Decimal => ScalarValue.Decimal128.Null,
            DbType.Double => ScalarValue.Float64.Null,
            DbType.Guid => throw new NotSupportedException("Cannot infer DataFusion type for DbType.Guid parameters with null value."),
            DbType.Int16 => ScalarValue.Int16.Null,
            DbType.Int32 => ScalarValue.Int32.Null,
            DbType.Int64 => ScalarValue.Int64.Null,
            DbType.Object => throw new NotSupportedException("Cannot infer DataFusion type for DbType.Object parameters with null value."),
            DbType.SByte => ScalarValue.Int8.Null,
            DbType.Single => ScalarValue.Float32.Null,
            DbType.Time => ScalarValue.Time64Microsecond.Null,
            DbType.UInt16 => ScalarValue.UInt16.Null,
            DbType.UInt32 => ScalarValue.UInt32.Null,
            DbType.UInt64 => ScalarValue.UInt64.Null,
            DbType.VarNumeric => throw new NotSupportedException("Cannot infer DataFusion type for DbType.VarNumeric parameters with null value."),
            DbType.Xml => throw new NotSupportedException("Cannot infer DataFusion type for DbType.Xml parameters with null value."),
            _ => ScalarValue.Utf8.Null
        };
    }
}

