using Apache.Arrow;

namespace DataFusionSharp.Formats.Parquet;

/// <summary>
/// Options for reading Parquet files.
/// </summary>
public sealed class ParquetReadOptions
{
    /// <summary>
    /// Explicit schema for the Parquet data. If null, DataFusion infers the schema from the file metadata.
    /// </summary>
    public Schema? Schema { get; set; }

    /// <summary>
    /// File extension filter. If null, DataFusion uses its default (".parquet").
    /// </summary>
    public string? FileExtension { get; set; }

    /// <summary>
    /// Partition columns for hive-style partitioned reads.
    /// Each entry specifies a column name and its Arrow data type.
    /// Empty if non-partitioned read.
    /// </summary>
    public IReadOnlyList<PartitionColumn>? TablePartitionCols { get; set; }

    /// <summary>
    /// Whether the parquet reader should use the predicate to prune row groups.
    /// If null, DataFusion uses its session config default.
    /// </summary>
    public bool? ParquetPruning { get; set; }

    /// <summary>
    /// Whether the parquet reader should skip any metadata in the file schema.
    /// This can help avoid schema conflicts due to metadata.
    /// If null, DataFusion uses its session config default.
    /// </summary>
    public bool? SkipMetadata { get; set; }
}
