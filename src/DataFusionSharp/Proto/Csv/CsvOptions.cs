using Google.Protobuf;

// ReSharper disable once CheckNamespace
namespace DataFusionSharp.Proto;

public partial class CsvOptions
{
    partial void OnConstruction()
    {
        Delimiter = ByteString.CopyFromUtf8(",");
        Quote = ByteString.CopyFromUtf8("\"");
        Compression = CompressionTypeVariant.Uncompressed;
    }
}