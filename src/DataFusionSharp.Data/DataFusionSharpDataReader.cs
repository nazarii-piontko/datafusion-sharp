#pragma warning disable CA1010 // Collections should implement generic interface

using System.Collections;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Arrays;

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
    private int _rowIndexComm = -1;
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
        if (!_fieldIndex.TryGetValue(name, out var ordinal))
            throw new ArgumentException($"Column name '{name}' does not exist in the result set.", nameof(name));
        return ordinal;
    }

    /// <inheritdoc />
    public override string GetDataTypeName(int ordinal) => _schema.FieldsList[ordinal].DataType.Name;

    /// <inheritdoc />
    public override Type GetFieldType(int ordinal)
    {
        return _schema.FieldsList[ordinal].DataType.ToNetType() ??
               throw new InvalidOperationException($"Unsupported data type {_schema.FieldsList[ordinal].DataType} in column {ordinal}.");
    }

    /// <inheritdoc />
    public override bool Read() => ReadAsync(CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc />
    public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        EnsureOpen();

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            if (_currentBatch is not null && _rowIndex + 1 < _currentBatch.Length)
            {
                ++_rowIndex;
                ++_rowIndexComm;
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
        return _currentBatch!.Column(ordinal).GetValue(_rowIndex) ?? DBNull.Value;
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
    public override bool GetBoolean(int ordinal)
    {
        EnsureRow();
        if (_currentBatch!.Column(ordinal) is BooleanArray boolArray)
            return boolArray.GetValue(_rowIndex) ?? ThrowNullValue<bool>(ordinal);
        throw new InvalidCastException($"Column {ordinal} is not a boolean array.");
    }

    /// <inheritdoc />
    public override byte GetByte(int ordinal)
    {
        EnsureRow();
        if (_currentBatch!.Column(ordinal) is UInt8Array uint8Array)
            return uint8Array.GetValue(_rowIndex) ?? ThrowNullValue<byte>(ordinal);
        throw new InvalidCastException($"Column {ordinal} is not a byte array.");
    }

    /// <inheritdoc />
    public override char GetChar(int ordinal)
    {
        var str = GetString(ordinal);
        if (str.Length != 1)
            throw new InvalidCastException($"Column {ordinal} contains a string value '{str}' that cannot be cast to char because it has length {str.Length}.");
        return str[0];
    }

    /// <inheritdoc />
    public override short GetInt16(int ordinal)
    {
        EnsureRow();
        if (_currentBatch!.Column(ordinal) is Int16Array a)
            return a.GetValue(_rowIndex) ?? ThrowNullValue<short>(ordinal);
        throw new InvalidCastException($"Column {ordinal} is not a 16-bit integer array.");
    }

    /// <inheritdoc />
    public override int GetInt32(int ordinal)
    {
        EnsureRow();
        if (_currentBatch!.Column(ordinal) is Int32Array a)
            return a.GetValue(_rowIndex) ?? ThrowNullValue<int>(ordinal);
        throw new InvalidCastException($"Column {ordinal} is not a 32-bit integer array.");
    }

    /// <inheritdoc />
    public override long GetInt64(int ordinal)
    {
        EnsureRow();
        if (_currentBatch!.Column(ordinal) is Int64Array a)
            return a.GetValue(_rowIndex) ?? ThrowNullValue<long>(ordinal);
        throw new InvalidCastException($"Column {ordinal} is not a 64-bit integer array.");
    }

    /// <inheritdoc />
    public override float GetFloat(int ordinal)
    {
        EnsureRow();
        if (_currentBatch!.Column(ordinal) is FloatArray a)
            return a.GetValue(_rowIndex) ?? ThrowNullValue<float>(ordinal);
        throw new InvalidCastException($"Column {ordinal} is not a float array.");
    }

    /// <inheritdoc />
    public override double GetDouble(int ordinal)
    {
        EnsureRow();
        if (_currentBatch!.Column(ordinal) is DoubleArray a)
            return a.GetValue(_rowIndex) ?? ThrowNullValue<double>(ordinal);
        throw new InvalidCastException($"Column {ordinal} is not a double array.");
    }

    /// <inheritdoc />
    public override decimal GetDecimal(int ordinal)
    {
        EnsureRow();
        return _currentBatch!.Column(ordinal) switch
        {
            Decimal128Array a => a.GetValue(_rowIndex) ?? ThrowNullValue<decimal>(ordinal),
            Decimal256Array a => a.GetValue(_rowIndex) ?? ThrowNullValue<decimal>(ordinal),
            _ => throw new InvalidCastException($"Column {ordinal} is not a double array.")
        };
    }

    /// <inheritdoc />
    public override string GetString(int ordinal)
    {
        EnsureRow();
        return _currentBatch!.Column(ordinal) switch
        {
            StringArray a => a.GetString(_rowIndex) ?? ThrowNullValue<string>(ordinal),
            StringViewArray a => a.GetString(_rowIndex) ?? ThrowNullValue<string>(ordinal),
            LargeStringArray a => a.GetString(_rowIndex) ?? ThrowNullValue<string>(ordinal),
            _ => throw new InvalidCastException($"Column {ordinal} is not a double array.")
        };
    }

    /// <inheritdoc />
    public override DateTime GetDateTime(int ordinal)
    {
        EnsureRow();
        return _currentBatch!.Column(ordinal) switch
        {
            Date32Array a => a.GetDateTime(_rowIndex) ?? ThrowNullValue<DateTime>(ordinal),
            Date64Array a => a.GetDateTime(_rowIndex) ?? ThrowNullValue<DateTime>(ordinal),
            TimestampArray a => a.GetTimestamp(_rowIndex)?.UtcDateTime ?? ThrowNullValue<DateTime>(ordinal),
            _ => throw new InvalidCastException($"Column {ordinal} does not represent a DateTime")
        };
    }

    /// <inheritdoc />
    public override Guid GetGuid(int ordinal)
    {
        if (!Guid.TryParse(GetString(ordinal), CultureInfo.InvariantCulture, out var g))
            throw new InvalidCastException($"Column {ordinal} does not represent a GUID");
        return g;
    }

    /// <inheritdoc />
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        EnsureRow();
        
        var bytes = _currentBatch!.Column(ordinal) switch
        {
            BinaryArray a => a.GetBytes(_rowIndex),
            LargeBinaryArray a => a.GetBytes(_rowIndex),
            FixedSizeBinaryArray a => a.GetBytes(_rowIndex),
            BinaryViewArray a => a.GetBytes(_rowIndex),
            _ => throw new InvalidCastException($"Column {ordinal} is not a binary array.")
        };
        if (buffer is null)
            return bytes.Length;
        
        var bytesToCopy = (int)Math.Min(length, bytes.Length - dataOffset);
        var outputSpan = buffer.AsSpan(bufferOffset, bytesToCopy);
        bytes.CopyTo(outputSpan);
        
        return bytesToCopy;
    }

    /// <inheritdoc />
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        var str = GetString(ordinal);
        if (buffer is null)
            return str.Length;
        
        var charsToCopy = Math.Min(length, str.Length - (int) dataOffset);
        str.CopyTo((int)dataOffset, buffer, bufferOffset, charsToCopy);
        
        return charsToCopy;
    }

    /// <inheritdoc />
    public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

    /// <inheritdoc />
    public override void Close()
    {
        if (_isClosed)
            return;
        
        _isClosed = true;
        _currentBatch = null;
        
        var disposeTask = _enumerator.DisposeAsync();
        if (!disposeTask.IsCompleted)
            disposeTask.AsTask().GetAwaiter().GetResult();

        _stream.Dispose();
        _dataFrame.Dispose();
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
            Close();
        base.Dispose(disposing);
    }

    private void EnsureOpen()
    {
        if (_isClosed)
            throw new InvalidOperationException("The data reader is closed.");
    }

    private void EnsureRow()
    {
        EnsureOpen();

        if (_currentBatch is null || _rowIndex < 0)
            throw new InvalidOperationException("No current row. Call Read() or ReadAsync() before accessing data.");
    }
    
    [DoesNotReturn]
    private T ThrowNullValue<T>(int ordinal)
    {
        throw new InvalidCastException($"Column {ordinal} at row {_rowIndexComm} contains a NULL value");
    }
}
