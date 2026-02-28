namespace DataFusionSharp.Formats.Parquet;

/// <summary>
/// Options for writing Parquet files.
/// </summary>
public sealed class ParquetWriteOptions
{
    /// <summary>
    /// Compression codec for the output Parquet file. If null, DataFusion uses its default.
    /// </summary>
    public ParquetCompression? Compression { get; set; }

    /// <summary>
    /// Maximum number of rows per row group. If null, DataFusion uses its default.
    /// </summary>
    public ulong? MaxRowGroupSize { get; set; }
}
