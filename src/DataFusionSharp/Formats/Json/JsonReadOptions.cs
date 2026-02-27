using Apache.Arrow;

namespace DataFusionSharp.Formats.Json;

/// <summary>
/// Options for reading JSON (NDJSON) files.
/// </summary>
public sealed class JsonReadOptions
{
    /// <summary>
    /// Explicit schema for the JSON data. If null, DataFusion infers the schema from the data.
    /// </summary>
    public Schema? Schema { get; set; }

    /// <summary>
    /// Maximum number of rows to use for schema inference. If null, DataFusion uses its default.
    /// </summary>
    public ulong? SchemaInferMaxRecords { get; set; }

    /// <summary>
    /// File extension filter. If null, DataFusion uses its default (".json").
    /// </summary>
    public string? FileExtension { get; set; }

    /// <summary>
    /// Compression type for the JSON file. If null, DataFusion uses its default (uncompressed).
    /// </summary>
    public CompressionType? FileCompressionType { get; set; }

    /// <summary>
    /// Partition columns for Hive-style partitioned data.
    /// Each entry specifies a column name and its Arrow data type.
    /// </summary>
    public IReadOnlyList<PartitionColumn>? TablePartitionCols { get; set; }
}
