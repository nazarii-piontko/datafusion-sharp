using DataFusionSharp.Proto;

namespace DataFusionSharp.Formats.Parquet;

internal static class ProtoParquetOptionsExtensions
{
    internal static Proto.ParquetReadOptions ToProto(this ParquetReadOptions options)
    {
        var proto = new Proto.ParquetReadOptions();

        if (options.Schema is not null)
            proto.Schema = options.Schema.ToProto();

        if (!string.IsNullOrEmpty(options.FileExtension))
            proto.FileExtension = options.FileExtension.ToProto();

        if (options.TablePartitionCols is { Count: > 0 })
            proto.TablePartitionCols.AddRange(options.TablePartitionCols.ToProto());

        if (options.ParquetPruning.HasValue)
            proto.ParquetPruning = options.ParquetPruning.Value;

        if (options.SkipMetadata.HasValue)
            proto.SkipMetadata = options.SkipMetadata.Value;

        return proto;
    }

    internal static TableParquetOptions ToProto(this ParquetWriteOptions options)
    {
        // The vendor proto's From<&ParquetOptionsProto> takes all fields literally
        // (no "if zero use default" logic), so we must populate DataFusion's actual
        // defaults for every regular field — proto3 would otherwise zero them out.
        var parquetOptions = new ParquetOptions
        {
            EnablePageIndex = true,
            Pruning = true,
            SkipMetadata = true,
            DataPagesizeLimit = 1024 * 1024,
            WriteBatchSize = 1024,
            WriterVersion = "1.0",
            AllowSingleFileParallelism = true,
            MaximumParallelRowGroupWriters = 1,
            MaximumBufferedRecordBatchesPerStream = 2,
            BloomFilterOnRead = true,
            MaxRowGroupSize = 1024 * 1024,
        };

        if (options.Compression.HasValue)
            parquetOptions.Compression = options.Compression.Value.ToProtoString();

        if (options.MaxRowGroupSize.HasValue)
            parquetOptions.MaxRowGroupSize = options.MaxRowGroupSize.Value;

        return new TableParquetOptions
        {
            Global = parquetOptions
        };
    }

    private static string ToProtoString(this ParquetCompression compression) => compression switch
    {
        ParquetCompression.Uncompressed => "uncompressed",
        ParquetCompression.Snappy => "snappy",
        ParquetCompression.Gzip => "gzip(4)",
        ParquetCompression.Brotli => "brotli(4)",
        ParquetCompression.Lz4 => "lz4",
        ParquetCompression.Lz4Raw => "lz4_raw",
        ParquetCompression.Zstd => "zstd(4)",
        _ => throw new ArgumentOutOfRangeException(nameof(compression), compression, "Unsupported Parquet compression type")
    };
}
