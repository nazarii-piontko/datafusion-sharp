using Apache.Arrow;

namespace DataFusionSharp.Tests;

internal static class ColumnExtensions
{
    public static IEnumerable<string?> GetStringValues(this IArrowArray column)
    {
        for (int i = 0; i < column.Length; i++)
        {
            yield return column switch
            {
                StringArray a => a.GetString(i),
                StringViewArray a => a.GetString(i),
                _ => throw new ArgumentException("Column is not a string array")
            };
        }
    }
}