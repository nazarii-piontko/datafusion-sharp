using DataFusionSharp.Proto;

namespace DataFusionSharp.Formats.Json;

internal static class JsonOptionsExtensions
{
    internal static Proto.JsonReadOptions ToProto(this JsonReadOptions options)
    {
        var proto = new Proto.JsonReadOptions();

        if (options.Schema is not null)
            proto.Schema = options.Schema.ToProto();

        if (options.SchemaInferMaxRecords.HasValue)
            proto.SchemaInferMaxRecords = options.SchemaInferMaxRecords.Value;

        if (!string.IsNullOrEmpty(options.FileExtension))
            proto.FileExtension = options.FileExtension.ToProto();

        if (options.FileCompressionType.HasValue)
            proto.FileCompressionType = options.FileCompressionType.Value.ToProto();

        if (options.TablePartitionCols is { Count: > 0 })
            proto.TablePartitionCols.AddRange(options.TablePartitionCols.ToProto());

        return proto;
    }

    internal static JsonOptions ToProto(this JsonWriteOptions options)
    {
        var proto = new JsonOptions();

        // Always set compression; when null, default to Uncompressed.
        // Omitting the field (Gzip=0, the proto3 zero value) causes to apply GZIP compression.
        proto.Compression = options.Compression?.ToProto() ?? CompressionTypeVariant.Uncompressed;

        if (options.SchemaInferMaxRec.HasValue)
            proto.SchemaInferMaxRec = options.SchemaInferMaxRec.Value;

        return proto;
    }
}
