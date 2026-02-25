namespace DataFusionSharp.Formats.Json;

/// <summary>
/// Options for writing JSON files.
/// </summary>
public sealed class JsonWriteOptions
{
    /// <summary>
    /// Compression type for the output JSON file. If null, output is uncompressed.
    /// <para>
    /// <b>Known limitation:</b> Due to the underlying protobuf encoding, <see cref="CompressionType.Gzip"/>
    /// maps to the proto3 zero-value enum entry and is omitted during serialization. Because the Rust
    /// DataFusion layer interprets an omitted compression field as GZIP, requesting
    /// <see cref="CompressionType.Gzip"/> will unexpectedly produce uncompressed output
    /// (the same as <c>null</c>). All other compression types work as expected.
    /// </para>
    /// </summary>
    public CompressionType? Compression { get; set; }

    /// <summary>
    /// Maximum number of records to use for schema inference. If null, DataFusion uses its default.
    /// </summary>
    public ulong? SchemaInferMaxRec { get; set; }
}
