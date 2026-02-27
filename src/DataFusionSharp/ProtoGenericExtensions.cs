using System.Runtime.CompilerServices;
using Google.Protobuf;

namespace DataFusionSharp;

internal static class ProtoGenericExtensions
{
    internal static ByteString ToProto(this string str)
    {
        return ByteString.CopyFromUtf8(str);
    }
    
    internal static ByteString ToProto(this char symbol, [CallerMemberName] string? propertyName = null) => char.IsAscii(symbol)
        ? ByteString.CopyFrom((byte) symbol)
        : throw new ArgumentOutOfRangeException(propertyName, symbol, "Value must be a single-byte ASCII character");
}
