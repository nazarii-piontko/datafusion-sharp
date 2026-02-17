using Google.Protobuf;

namespace DataFusionSharp.Proto;

/// <summary>
/// Extension methods for protobuf types.
/// </summary>
public static class ProtoExtensions
{
    /// <summary>
    /// Converts a string to a ByteString using UTF-8 encoding.
    /// </summary>
    /// <param name="str">String to convert.</param>
    public static ByteString AsByteString(this string str)
    {
        return ByteString.CopyFromUtf8(str);
    }
}