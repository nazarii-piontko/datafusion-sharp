#pragma warning disable CA1010 // Collections should implement generic interface
#pragma warning disable CA1305 // Specify IFormatProvider

using System.Collections;
using System.Data.Common;
using System.Data.SqlTypes;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace DataFusionSharp.Data;

/// <summary>
/// Provides a forward-only, read-only, streaming row reader over a DataFusion query result.
/// </summary>
/// <remarks>
/// <para>
/// Internally this reader consumes a <see cref="DataFrameStream"/> one Arrow <see cref="RecordBatch"/> at a time,
/// exposing a familiar row-by-row ADO.NET interface on top of the columnar Arrow data.
/// </para>
/// <para>
/// Because Arrow data lives in native (Rust-allocated) memory, disposing the reader is important:
/// it releases both the stream and the underlying <see cref="DataFrame"/>.
/// Never access values through an already-disposed reader.
/// </para>
/// </remarks>
public sealed class DataFusionSharpDataReader : DbDataReader
{
    private readonly DataFrame _dataFrame;
    private readonly DataFrameStream _stream;
    private readonly IAsyncEnumerator<RecordBatch> _enumerator;
    private readonly Schema _schema;
    private readonly Dictionary<string, int> _fieldIndex;

    private RecordBatch? _currentBatch;
    private int _rowIndex = -1;
    private bool _isClosed;

    internal DataFusionSharpDataReader(DataFrame dataFrame, DataFrameStream stream)
    {
        _dataFrame = dataFrame;
        _stream = stream;
        _enumerator = stream.GetAsyncEnumerator();
        _schema = stream.Schema;

        _fieldIndex = new Dictionary<string, int>(_schema.FieldsList.Count, StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < _schema.FieldsList.Count; i++)
            _fieldIndex[_schema.FieldsList[i].Name] = i;
    }

    /// <inheritdoc />
    public override int FieldCount => _schema.FieldsList.Count;

    /// <inheritdoc />
    public override int Depth => 0;

    /// <inheritdoc />
    public override bool IsClosed => _isClosed;

    /// <inheritdoc />
    public override int RecordsAffected => -1;

    /// <inheritdoc />
    public override bool HasRows => _currentBatch is { Length: > 0 };

    /// <inheritdoc />
    public override object this[int ordinal] => GetValue(ordinal);

    /// <inheritdoc />
    public override object this[string name] => GetValue(GetOrdinal(name));

    /// <inheritdoc />
    public override string GetName(int ordinal) => _schema.FieldsList[ordinal].Name;

    /// <inheritdoc />
    public override int GetOrdinal(string name)
    {
        if (_fieldIndex.TryGetValue(name, out var ordinal))
            return ordinal;
        
#pragma warning disable CA2201
        throw new IndexOutOfRangeException($"No column named '{name}' exists in the result set.");
#pragma warning restore CA2201
    }

    /// <inheritdoc />
    public override string GetDataTypeName(int ordinal) => _schema.FieldsList[ordinal].DataType.Name;

    /// <inheritdoc />
    public override Type GetFieldType(int ordinal) => ArrowTypeToNetType(_schema.FieldsList[ordinal].DataType);

    /// <inheritdoc />
    public override bool Read() => ReadAsync(CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc />
    public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        ThrowIfClosed();

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            if (_currentBatch is not null && _rowIndex + 1 < _currentBatch.Length)
            {
                _rowIndex++;
                return true;
            }
            
            if (!await _enumerator.MoveNextAsync().ConfigureAwait(false))
                return false;

            _currentBatch = _enumerator.Current;
            _rowIndex = -1;
        }
    }

    /// <inheritdoc />
    public override bool NextResult() => false;

    /// <inheritdoc />
    public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => Task.FromResult(false);

    /// <inheritdoc />
    public override bool IsDBNull(int ordinal)
    {
        EnsureRow();
        return _currentBatch!.Column(ordinal).IsNull(_rowIndex);
    }

    /// <inheritdoc />
    public override object GetValue(int ordinal)
    {
        EnsureRow();
        return GetArrowValue(_currentBatch!.Column(ordinal), _rowIndex) ?? DBNull.Value;
    }

    /// <inheritdoc />
    public override int GetValues(object[] values)
    {
        ArgumentNullException.ThrowIfNull(values);
        
        EnsureRow();
        
        var count = Math.Min(values.Length, FieldCount);
        for (var i = 0; i < count; i++)
            values[i] = GetValue(i);
        
        return count;
    }

    /// <inheritdoc />
    public override bool GetBoolean(int ordinal) => (bool)GetNonNullValue(ordinal);

    /// <inheritdoc />
    public override byte GetByte(int ordinal) => Convert.ToByte(GetNonNullValue(ordinal));

    /// <inheritdoc />
    public override char GetChar(int ordinal)
    {
        var v = GetNonNullValue(ordinal);
        return v is string { Length: 1 } s ? s[0] : Convert.ToChar(v);
    }

    /// <inheritdoc />
    public override short GetInt16(int ordinal) => Convert.ToInt16(GetNonNullValue(ordinal));

    /// <inheritdoc />
    public override int GetInt32(int ordinal) => Convert.ToInt32(GetNonNullValue(ordinal));

    /// <inheritdoc />
    public override long GetInt64(int ordinal) => Convert.ToInt64(GetNonNullValue(ordinal));

    /// <inheritdoc />
    public override float GetFloat(int ordinal) => Convert.ToSingle(GetNonNullValue(ordinal));

    /// <inheritdoc />
    public override double GetDouble(int ordinal) => Convert.ToDouble(GetNonNullValue(ordinal));

    /// <inheritdoc />
    public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(GetNonNullValue(ordinal));
    
    /// <inheritdoc />
    public override string GetString(int ordinal) => (string)GetNonNullValue(ordinal);

    /// <inheritdoc />
    public override DateTime GetDateTime(int ordinal)
    {
        return GetNonNullValue(ordinal) switch
        {
            DateTime dt        => dt,
            DateTimeOffset dto => dto.UtcDateTime,
            DateOnly d         => d.ToDateTime(TimeOnly.MinValue),
            _                  => Convert.ToDateTime(GetNonNullValue(ordinal))
        };
    }

    /// <inheritdoc />
    public override Guid GetGuid(int ordinal) => Guid.Parse(GetString(ordinal));

    /// <inheritdoc />
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        throw new NotSupportedException("GetBytes is not supported. Use GetValue to retrieve binary data as a byte array.");
    }

    /// <inheritdoc />
    public override long GetChars( int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        var str = GetString(ordinal);
        if (buffer is null) return str.Length;
        var toCopy = Math.Min(length, str.Length - (int)dataOffset);
        str.CopyTo((int)dataOffset, buffer, bufferOffset, toCopy);
        return toCopy;
    }

    /// <inheritdoc />
    public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    /// <inheritdoc />
    public override void Close()
    {
        if (_isClosed) return;
        _isClosed = true;
        _currentBatch = null;

        // Dispose the async enumerator. Arrow async iterators normally complete synchronously
        // here because no async work remains once iteration has stopped.
        var disposeTask = _enumerator.DisposeAsync();
        if (!disposeTask.IsCompleted)
            disposeTask.AsTask().GetAwaiter().GetResult();

        _stream.Dispose();    // releases all tracked Arrow batches (native memory)
        _dataFrame.Dispose(); // releases the DataFrame handle
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing) Close();
        base.Dispose(disposing);
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private void ThrowIfClosed()
    {
        if (_isClosed)
            throw new InvalidOperationException("The data reader is closed.");
    }

    private void EnsureRow()
    {
        ThrowIfClosed();
        if (_currentBatch is null || _rowIndex < 0)
            throw new InvalidOperationException(
                "No current row. Call Read() or ReadAsync() before accessing data.");
    }

    private object GetNonNullValue(int ordinal)
    {
        if (IsDBNull(ordinal))
            throw new InvalidCastException(
                $"Cannot get a non-null value from column {ordinal}: the value is NULL.");
        return GetArrowValue(_currentBatch!.Column(ordinal), _rowIndex)!;
    }

    private static object? GetArrowValue(IArrowArray array, int rowIndex)
    {
        if (array.IsNull(rowIndex))
            return null;

        return array switch
        {
            BooleanArray         a => a.GetValue(rowIndex),
            Int8Array            a => a.GetValue(rowIndex),
            Int16Array           a => a.GetValue(rowIndex),
            Int32Array           a => a.GetValue(rowIndex),
            Int64Array           a => a.GetValue(rowIndex),
            UInt8Array           a => a.GetValue(rowIndex),
            UInt16Array          a => a.GetValue(rowIndex),
            UInt32Array          a => a.GetValue(rowIndex),
            UInt64Array          a => a.GetValue(rowIndex),
            FloatArray           a => a.GetValue(rowIndex),
            DoubleArray          a => a.GetValue(rowIndex),
            Decimal128Array      a => ConvertSqlDecimal(a.GetSqlDecimal(rowIndex)),
            StringArray          a => a.GetString(rowIndex),
            StringViewArray      a => a.GetString(rowIndex),
            LargeStringArray     a => a.GetString(rowIndex),
            BinaryArray          a => a.GetBytes(rowIndex).ToArray(),
            LargeBinaryArray     a => a.GetBytes(rowIndex).ToArray(),
            FixedSizeBinaryArray a => a.GetBytes(rowIndex).ToArray(),
            Date32Array          a => a.GetDateOnly(rowIndex),
            Date64Array          a => a.GetDateOnly(rowIndex),
            TimestampArray       a => a.GetTimestamp(rowIndex),
            Time32Array          a => a.GetTime(rowIndex),
            Time64Array          a => a.GetTime(rowIndex),
            _                      => array.ToString()
        };
    }

    private static decimal? ConvertSqlDecimal(SqlDecimal? value) =>
        value.HasValue ? (decimal)value.Value : null;

    // ── Arrow type → .NET CLR type mapping ───────────────────────────────────

    private static Type ArrowTypeToNetType(IArrowType arrowType) => arrowType switch
    {
        BooleanType          => typeof(bool),
        Int8Type             => typeof(sbyte),
        Int16Type            => typeof(short),
        Int32Type            => typeof(int),
        Int64Type            => typeof(long),
        UInt8Type            => typeof(byte),
        UInt16Type           => typeof(ushort),
        UInt32Type           => typeof(uint),
        UInt64Type           => typeof(ulong),
        FloatType            => typeof(float),
        DoubleType           => typeof(double),
        Decimal128Type       => typeof(decimal),
        Decimal256Type       => typeof(decimal),
        StringType           => typeof(string),
        StringViewType       => typeof(string),
        LargeStringType      => typeof(string),
        BinaryType           => typeof(byte[]),
        LargeBinaryType      => typeof(byte[]),
        FixedSizeBinaryType  => typeof(byte[]),
        BinaryViewType       => typeof(byte[]),
        Date32Type           => typeof(DateOnly),
        Date64Type           => typeof(DateOnly),
        TimestampType        => typeof(DateTimeOffset),
        Time32Type           => typeof(TimeOnly),
        Time64Type           => typeof(TimeOnly),
        DurationType         => typeof(TimeSpan),
        _                    => typeof(object)
    };
}

