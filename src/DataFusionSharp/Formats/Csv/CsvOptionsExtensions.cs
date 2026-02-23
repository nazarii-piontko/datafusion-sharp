using DataFusionSharp.Proto;
using Google.Protobuf;

namespace DataFusionSharp.Formats.Csv;

internal static class CsvOptionsExtensions
{
    private static readonly ByteString TrueByteString = '\x7f'.ToProto();
    private static readonly ByteString FalseByteString = '\x00'.ToProto();

    internal static Proto.CsvReadOptions ToProto(this CsvReadOptions options)
    {
        var proto = new Proto.CsvReadOptions();

        if (options.HasHeader.HasValue)
            proto.HasHeader = options.HasHeader.Value;

        if (options.Delimiter.HasValue)
            proto.Delimiter = options.Delimiter.Value.ToProto();

        if (options.Quote.HasValue)
            proto.Quote = ByteString.CopyFrom((byte)options.Quote.Value);

        if (options.Terminator.HasValue)
            proto.Terminator = options.Terminator.Value.ToProto();

        if (options.Escape.HasValue)
            proto.Escape = options.Escape.Value.ToProto();

        if (options.Comment.HasValue)
            proto.Comment = options.Comment.Value.ToProto();

        if (options.NewlinesInValues.HasValue)
            proto.NewlinesInValues = options.NewlinesInValues.Value;

        if (options.Schema is not null)
            proto.Schema = options.Schema.ToProto();

        if (options.SchemaInferMaxRecords.HasValue)
            proto.SchemaInferMaxRecords = options.SchemaInferMaxRecords.Value;

        if (!string.IsNullOrEmpty(options.FileExtension))
            proto.FileExtension = options.FileExtension.ToProto();

        if (options.FileCompressionType.HasValue)
            proto.FileCompressionType = options.FileCompressionType.Value.ToProto();

        if (!string.IsNullOrEmpty(options.NullRegex))
            proto.NullRegex = options.NullRegex.ToProto();

        if (options.TruncatedRows.HasValue)
            proto.TruncatedRows = options.TruncatedRows.Value;

        return proto;
    }

    internal static CsvOptions ToProto(this CsvWriteOptions options)
    {
        var proto = new CsvOptions();

        if (options.HasHeader.HasValue)
            proto.HasHeader = options.HasHeader.Value ? TrueByteString : FalseByteString;

        if (options.Delimiter.HasValue)
            proto.Delimiter = options.Delimiter.Value.ToProto();

        if (options.Quote.HasValue)
            proto.Quote = options.Quote.Value.ToProto();

        if (options.Escape.HasValue)
            proto.Escape = options.Escape.Value.ToProto();

        // Always set compression; when null, default to Uncompressed.
        // Omitting the field (Gzip=0, the proto3 zero value) causes to apply GZIP compression.
        proto.Compression = options.Compression?.ToProto() ?? CompressionTypeVariant.Uncompressed;

        if (options.SchemaInferMaxRec.HasValue)
            proto.SchemaInferMaxRec = options.SchemaInferMaxRec.Value;

        if (!string.IsNullOrEmpty(options.DateFormat))
            proto.DateFormat = options.DateFormat;

        if (!string.IsNullOrEmpty(options.DatetimeFormat))
            proto.DatetimeFormat = options.DatetimeFormat;

        if (!string.IsNullOrEmpty(options.TimestampFormat))
            proto.TimestampFormat = options.TimestampFormat;

        if (!string.IsNullOrEmpty(options.TimestampTzFormat))
            proto.TimestampTzFormat = options.TimestampTzFormat;

        if (!string.IsNullOrEmpty(options.TimeFormat))
            proto.TimeFormat = options.TimeFormat;

        if (options.NullValue != null)
            proto.NullValue = options.NullValue;

        if (options.NullRegex != null)
            proto.NullRegex = options.NullRegex;

        if (options.Comment.HasValue)
            proto.Comment = options.Comment.Value.ToProto();

        if (options.DoubleQuote.HasValue)
            proto.DoubleQuote = options.DoubleQuote.Value ? TrueByteString : FalseByteString;

        if (options.NewlinesInValues.HasValue)
            proto.NewlinesInValues = options.NewlinesInValues.Value ? TrueByteString : FalseByteString;

        if (options.Terminator.HasValue)
            proto.Terminator = options.Terminator.Value.ToProto();

        return proto;
    }
}
