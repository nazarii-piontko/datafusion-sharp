namespace DataFusionSharp.Formats.Parquet;

/// <summary>
/// Specifies the compression codec for Parquet files.
/// </summary>
public enum ParquetCompression
{
    /// <summary>No compression.</summary>
    Uncompressed,
    /// <summary>Snappy compression.</summary>
    Snappy,
    /// <summary>Gzip compression.</summary>
    Gzip,
    /// <summary>Brotli compression.</summary>
    Brotli,
    /// <summary>LZ4 compression.</summary>
    Lz4,
    /// <summary>LZ4 raw compression.</summary>
    Lz4Raw,
    /// <summary>Zstd compression.</summary>
    Zstd,
}
