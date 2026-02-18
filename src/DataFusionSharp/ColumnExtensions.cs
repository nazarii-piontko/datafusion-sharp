using Apache.Arrow;

namespace DataFusionSharp;

/// <summary>
/// Extension methods for working with Arrow column arrays returned from DataFusion queries.
/// </summary>
public static class ColumnExtensions
{
    /// <summary>
    /// Enumerates the values of an Arrow array as nullable long integers.
    /// </summary>
    /// <param name="column">The Arrow array to enumerate.</param>
    /// <returns>An enumerator of nullable long integers representing the values in the column.</returns>
    /// <exception cref="ArgumentException">Thrown if the provided column is not an Int64 array.</exception>
    public static IEnumerable<long?> AsInt64(this IArrowArray column)
    {
        ArgumentNullException.ThrowIfNull(column);
        
        for (int i = 0; i < column.Length; i++)
        {
            yield return column switch
            {
                Int64Array a => a.GetValue(i),
                _ => throw new ArgumentException($"Column is not a Int64 Array, actual type: {column.GetType().Name}")
            };
        }
    }
    
    /// <summary>
    /// Enumerates the values of an Arrow array as nullable double-precision floating-point numbers.
    /// </summary>
    /// <param name="column">The Arrow array to enumerate.</param>
    /// <returns>An enumerator of nullable doubles representing the values in the column.</returns>
    /// <exception cref="ArgumentException">Thrown if the provided column is not a Double array.</exception>
    public static IEnumerable<double?> AsDouble(this IArrowArray column)
    {
        ArgumentNullException.ThrowIfNull(column);
        
        for (int i = 0; i < column.Length; i++)
        {
            yield return column switch
            {
                DoubleArray a => a.GetValue(i),
                _ => throw new ArgumentException($"Column is not a Double Array, actual type: {column.GetType().Name}")
            };
        }
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
        
        for (int i = 0; i < column.Length; i++)
        {
            yield return column switch
            {
                StringArray a => a.GetString(i),
                StringViewArray a => a.GetString(i),
                _ => throw new ArgumentException($"Column is not a String or StringView Array, actual type: {column.GetType().Name}")
            };
        }
    }
}