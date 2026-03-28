using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace DataFusionSharp;

/// <summary>
/// Extension methods for working with Arrow column arrays returned from DataFusion queries.
/// </summary>
public static class ArrayExtensions
{
    /// <summary>
    /// Enumerates the values of an Arrow array as nullable booleans.
    /// </summary>
    /// <param name="array">The Arrow array to enumerate.</param>
    /// <returns>An enumerator of nullable booleans representing the values in the array.</returns>
    /// <exception cref="ArgumentException">Thrown if the provided array is not a Boolean array.</exception>
    public static IEnumerable<bool?> AsBool(this IArrowArray array)
    {
        ArgumentNullException.ThrowIfNull(array);
        
        if (array is BooleanArray typedArray)
            return typedArray.AsBool();
        
        throw new ArgumentException($"Column is not Boolean Array, actual type: {array.GetType().Name}");
    }
    
    /// <summary>
    /// Enumerates the values of an BooleanArray as nullable booleans.
    /// </summary>
    /// <param name="array">The BooleanArray to enumerate.</param>
    /// <returns>An enumerator of nullable long integers representing the values in the array.</returns>
    public static IEnumerable<bool?> AsBool(this BooleanArray array)
    {
        ArgumentNullException.ThrowIfNull(array);
        
        for (int i = 0; i < array.Length; i++)
            yield return array.GetValue(i);
    }
    
    /// <summary>
    /// Enumerates the values of an Arrow array as nullable long integers.
    /// </summary>
    /// <param name="array">The Arrow array to enumerate.</param>
    /// <returns>An enumerator of nullable long integers representing the values in the array.</returns>
    /// <exception cref="ArgumentException">Thrown if the provided array is not an Int64 array.</exception>
    public static IEnumerable<long?> AsInt64(this IArrowArray array)
    {
        ArgumentNullException.ThrowIfNull(array);
        
        if (array is Int64Array typedArray)
            return typedArray.AsInt64();
        
        throw new ArgumentException($"Column is not Int64 Array, actual type: {array.GetType().Name}");
    }
    
    /// <summary>
    /// Enumerates the values of an Int64Array as nullable long integers.
    /// </summary>
    /// <param name="array">The Int64Array to enumerate.</param>
    /// <returns>An enumerator of nullable long integers representing the values in the array.</returns>
    public static IEnumerable<long?> AsInt64(this Int64Array array)
    {
        ArgumentNullException.ThrowIfNull(array);
        
        for (int i = 0; i < array.Length; i++)
            yield return array.GetValue(i);
    }
    
    /// <summary>
    /// Enumerates the values of an Arrow array as nullable double-precision floating-point numbers.
    /// </summary>
    /// <param name="array">The Arrow array to enumerate.</param>
    /// <returns>An enumerator of nullable doubles representing the values in the array.</returns>
    /// <exception cref="ArgumentException">Thrown if the provided array is not a Double array.</exception>
    public static IEnumerable<double?> AsDouble(this IArrowArray array)
    {
        ArgumentNullException.ThrowIfNull(array);
        
        if (array is DoubleArray typedArray)
            return typedArray.AsDouble();
        
        throw new ArgumentException($"Column is not Double Array, actual type: {array.GetType().Name}");
    }
    
    /// <summary>
    /// Enumerates the values of a DoubleArray as nullable double-precision floating-point numbers.
    /// </summary>
    /// <param name="array">The DoubleArray to enumerate.</param>
    /// <returns>An enumerator of nullable doubles representing the values in the array.</returns>
    public static IEnumerable<double?> AsDouble(this DoubleArray array)
    {
        ArgumentNullException.ThrowIfNull(array);
        
        for (int i = 0; i < array.Length; i++)
            yield return array.GetValue(i);
    }
    
    /// <summary>
    /// Enumerates the values of an Arrow array as nullable strings.
    /// </summary>
    /// <param name="column">The Arrow array to enumerate.</param>
    /// <returns>>An enumerator of nullable strings representing the values in the column.</returns>
    /// <exception cref="ArgumentException">Thrown if the provided column is not a String array.</exception>
    public static IEnumerable<string?> AsString(this IArrowArray column)
    {
        ArgumentNullException.ThrowIfNull(column);
        
        if (column is StringArray stringColumn)
            return stringColumn.AsString();
        
        if (column is StringViewArray typedViewColumn)
            return typedViewColumn.AsString();
        
        if (column is LargeStringArray largeStringArray)
            return largeStringArray.AsString();
        
        throw new ArgumentException($"Column is not String Array, actual type: {column.GetType().Name}");
    }
    
    /// <summary>
    /// Enumerates the values of a StringArray as nullable strings.
    /// </summary>
    /// <param name="column">The StringArray to enumerate.</param>
    /// <returns>>An enumerator of nullable strings representing the values in the column.</returns>
    public static IEnumerable<string?> AsString(this StringArray column)
    {
        ArgumentNullException.ThrowIfNull(column);
        
        for (int i = 0; i < column.Length; i++)
            yield return column.GetString(i);
    }
    
    /// <summary>
    /// Enumerates the values of a StringViewArray as nullable strings.
    /// </summary>
    /// <param name="column">The StringViewArray to enumerate.</param>
    /// <returns>>An enumerator of nullable strings representing the values in the column.</returns>
    public static IEnumerable<string?> AsString(this StringViewArray column)
    {
        ArgumentNullException.ThrowIfNull(column);
        
        for (int i = 0; i < column.Length; i++)
            yield return column.GetString(i);
    }
    
    /// <summary>
    /// Enumerates the values of a LargeStringArray as nullable strings.
    /// </summary>
    /// <param name="column">The LargeStringArray to enumerate.</param>
    /// <returns>>An enumerator of nullable strings representing the values in the column.</returns>
    public static IEnumerable<string?> AsString(this LargeStringArray column)
    {
        ArgumentNullException.ThrowIfNull(column);
        
        for (int i = 0; i < column.Length; i++)
            yield return column.GetString(i);
    }
    
    /// <summary>
    /// Returns the value at the specified rowIndex from any Arrow array as an <see cref="object"/>,
    /// mapping each Arrow type to its idiomatic .NET equivalent.
    /// Returns <c>null</c> if the element at <paramref name="rowIndex"/> is null.
    /// </summary>
    /// <param name="array">The Arrow array to read from.</param>
    /// <param name="rowIndex">The zero-based rowIndex of the element to retrieve.</param>
    /// <returns>The .NET value at the given rowIndex, or <c>null</c> if the element is null.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="array"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">Thrown when the array type is not supported.</exception>
    public static object? GetValue(this IArrowArray array, int rowIndex)
    {
        ArgumentNullException.ThrowIfNull(array);

        if (array.IsNull(rowIndex))
            return null;

        return array switch
        {
            NullArray => null,
            BooleanArray a => a.GetValue(rowIndex)!.Value,
            Int8Array a => a.GetValue(rowIndex)!.Value,
            Int16Array a => a.GetValue(rowIndex)!.Value,
            Int32Array a => a.GetValue(rowIndex)!.Value,
            Int64Array a => a.GetValue(rowIndex)!.Value,
            UInt8Array a => a.GetValue(rowIndex)!.Value,
            UInt16Array a => a.GetValue(rowIndex)!.Value,
            UInt32Array a => a.GetValue(rowIndex)!.Value,
            UInt64Array a => a.GetValue(rowIndex)!.Value,
            HalfFloatArray a => a.GetValue(rowIndex)!.Value,
            FloatArray a => a.GetValue(rowIndex)!.Value,
            DoubleArray a => a.GetValue(rowIndex)!.Value,
            Decimal128Array a => a.GetValue(rowIndex)!.Value,
            Decimal256Array a => a.GetValue(rowIndex)!.Value,
            StringArray a => a.GetString(rowIndex),
            StringViewArray a => a.GetString(rowIndex),
            LargeStringArray a => a.GetString(rowIndex),
            BinaryArray a => a.GetBytes(rowIndex).ToArray(),
            BinaryViewArray a => a.GetBytes(rowIndex).ToArray(),
            FixedSizeBinaryArray a => a.GetBytes(rowIndex).ToArray(),
            LargeBinaryArray a => a.GetBytes(rowIndex).ToArray(),
            Date32Array a => a.GetDateOnly(rowIndex)!.Value,
            Date64Array a => a.GetDateOnly(rowIndex)!.Value,
            TimestampArray a => a.GetTimestamp(rowIndex)!.Value,
            Time32Array a => a.GetTime(rowIndex)!.Value,
            Time64Array a => a.GetTime(rowIndex)!.Value,
            DurationArray a => a.GetTimeSpan(rowIndex)!.Value,
            YearMonthIntervalArray a => a.GetValue(rowIndex)!.Value,
            DayTimeIntervalArray a => a.GetValue(rowIndex)!.Value,
            MonthDayNanosecondIntervalArray a => a.GetValue(rowIndex)!.Value,
            MapArray a => a.GetSlicedValues(rowIndex),
            ListArray a => a.GetSlicedValues(rowIndex),
            LargeListArray a => a.GetSlicedValues(rowIndex),
            FixedSizeListArray a => a.GetSlicedValues(rowIndex),
            StructArray a => a.Fields.Select(f => f.GetValue(rowIndex)).ToArray(),
            // DictionaryArray a => ...
            // SparseUnionArray a => ...
            // DenseUnionArray a => ...
            _ => throw new ArgumentException($"Unsupported Arrow array type: {array.GetType().Name}", nameof(array))
        };
    }

    /// <summary>
    /// Gets the corresponding .NET type for a given Arrow type, if it exists.
    /// The instance of that type will be returned by <see cref="GetValue"/>
    /// </summary>
    /// <param name="arrowType">The Arrow type to map to a .NET type.</param>
    /// <returns>The .NET type corresponding to the given Arrow type, or <c>null</c> if no suitable mapping exists.</returns>
    public static Type? ToNetType(this IArrowType arrowType)
    {
        return arrowType switch
        {
            NullType => typeof(DBNull),
            BooleanType => typeof(bool),
            Int8Type => typeof(sbyte),
            Int16Type => typeof(short),
            Int32Type => typeof(int),
            Int64Type => typeof(long),
            UInt8Type => typeof(byte),
            UInt16Type => typeof(ushort),
            UInt32Type => typeof(uint),
            UInt64Type => typeof(ulong),
            HalfFloatType => typeof(Half),
            FloatType => typeof(float),
            DoubleType => typeof(double),
            Decimal128Type or Decimal256Type => typeof(decimal),
            StringType or StringViewType or LargeStringType => typeof(string),
            BinaryType or BinaryViewType or FixedSizeBinaryType or LargeBinaryType => typeof(byte[]),
            Date32Type or Date64Type => typeof(DateOnly),
            TimestampType => typeof(DateTimeOffset),
            Time32Type or Time64Type => typeof(TimeOnly),
            DurationType => typeof(TimeSpan),
            IntervalType t => t.Unit switch
            {
                IntervalUnit.YearMonth => typeof(Apache.Arrow.Scalars.YearMonthInterval),
                IntervalUnit.DayTime => typeof(Apache.Arrow.Scalars.DayTimeInterval),
                IntervalUnit.MonthDayNanosecond => typeof(Apache.Arrow.Scalars.MonthDayNanosecondInterval),
                _ => null
            },
            MapType or ListType or LargeListType or FixedSizeListType => typeof(IEnumerable<IArrowArray>),
            StructType => typeof(object?[]),
            // DictionaryType => ...
            // UnionType => ...
            _ => null
        };
    }
}


