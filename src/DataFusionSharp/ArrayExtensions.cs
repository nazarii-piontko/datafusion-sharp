using Apache.Arrow;

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
}