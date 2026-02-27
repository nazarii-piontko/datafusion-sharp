using Apache.Arrow.Types;

namespace DataFusionSharp.Formats;

/// <summary>
/// Represents a partition column with a name and Arrow data type.
/// Used to specify table partition columns for Hive-style partitioned data.
/// </summary>
/// <param name="Name">The name of the partition column.</param>
/// <param name="ArrowType">The Arrow data type of the partition column.</param>
public record PartitionColumn(string Name, IArrowType ArrowType);

internal static class ProtoPartitionColumnExtensions
{
    internal static IEnumerable<Proto.PartitionColumn> ToProto(this IReadOnlyList<PartitionColumn> cols)
    {
        return cols.Select(c => new Proto.PartitionColumn
        {
            Name = c.Name,
            ArrowType = c.ArrowType.ToProto()
        });
    }
}