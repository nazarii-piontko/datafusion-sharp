using DataFusionSharp.Proto;

namespace DataFusionSharp.Formats;

/// <summary>
/// Specifies the compression type for CSV files.
/// </summary>
public enum CompressionType
{
    /// <summary>GZIP compression.</summary>
    Gzip,
    /// <summary>BZIP2 compression.</summary>
    Bzip2,
    /// <summary>XZ compression.</summary>
    Xz,
    /// <summary>ZSTD compression.</summary>
    Zstd,
    /// <summary>No compression.</summary>
    Uncompressed,
}

internal static class ProtoCompressionTypeExtensions
{
    internal static CompressionTypeVariant ToProto(this CompressionType compression) => (CompressionTypeVariant) compression;
}
