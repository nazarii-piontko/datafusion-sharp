using Apache.Arrow;

namespace DataFusionSharp.Formats.Csv;

/// <summary>
/// Options for reading CSV files.
/// </summary>
public sealed class CsvReadOptions
{
    /// <summary>
    /// Whether the CSV file has a header row. If null, DataFusion uses its default (true).
    /// </summary>
    public bool? HasHeader { get; set; }

    /// <summary>
    /// Column delimiter character. If null, DataFusion uses its default (',').
    /// Must be a single-byte ASCII character.
    /// </summary>
    public char? Delimiter { get; set; }

    /// <summary>
    /// Quote character. If null, DataFusion uses its default ('"').
    /// Must be a single-byte ASCII character.
    /// </summary>
    public char? Quote { get; set; }

    /// <summary>
    /// Line terminator character. If null, DataFusion uses its default (CRLF).
    /// Must be a single-byte ASCII character.
    /// </summary>
    public char? Terminator { get; set; }

    /// <summary>
    /// Escape character. If null, DataFusion uses its default (no escape character).
    /// Must be a single-byte ASCII character.
    /// </summary>
    public char? Escape { get; set; }

    /// <summary>
    /// Comment character. Lines beginning with this character are ignored.
    /// If null, comment lines are not supported.
    /// Must be a single-byte ASCII character.
    /// </summary>
    public char? Comment { get; set; }

    /// <summary>
    /// Whether newlines in quoted values are supported. If null, DataFusion uses its default (false).
    /// </summary>
    public bool? NewlinesInValues { get; set; }

    /// <summary>
    /// Explicit schema for the CSV data. If null, DataFusion infers the schema from the data.
    /// </summary>
    public Schema? Schema { get; set; }

    /// <summary>
    /// Maximum number of rows to use for schema inference. If null, DataFusion uses its default.
    /// </summary>
    public ulong? SchemaInferMaxRecords { get; set; }

    /// <summary>
    /// File extension filter. If null, DataFusion uses its default (".csv").
    /// </summary>
    public string? FileExtension { get; set; }

    /// <summary>
    /// Compression type for the CSV file. If null, DataFusion uses its default (uncompressed).
    /// </summary>
    public CompressionType? FileCompressionType { get; set; }

    /// <summary>
    /// Regular expression pattern to match null values. If null, no null pattern matching is applied.
    /// </summary>
    public string? NullRegex { get; set; }

    /// <summary>
    /// Whether to allow truncated (incomplete) rows. If null, DataFusion uses its default (false).
    /// </summary>
    public bool? TruncatedRows { get; set; }

    /// <summary>
    /// Partition columns for Hive-style partitioned data.
    /// Each entry specifies a column name and its Arrow data type.
    /// </summary>
    public IReadOnlyList<PartitionColumn>? TablePartitionCols { get; set; }
}
