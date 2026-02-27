using System.Runtime.CompilerServices;
using DataFusionSharp.Formats;
using Google.Protobuf;

namespace DataFusionSharp.Proto;

/// <summary>
/// Extension methods for protobuf types.
/// </summary>
internal static class ProtoExtensions
{
    /// <summary>
    /// Converts a string to a ByteString using UTF-8 encoding.
    /// </summary>
    /// <param name="str">String to convert.</param>
    public static ByteString ToProto(this string str)
    {
        return ByteString.CopyFromUtf8(str);
    }
    
    /// <summary>
    /// Converts a single ASCII character to a ByteString.
    /// </summary>
    /// <param name="symbol">Character to convert. Must be a single-byte ASCII character.</param>
    /// <param name="propertyName">Name of the property being converted, used for error messages. Automatically provided by the caller.</param>
    /// <returns>A ByteString containing the ASCII character.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if the character is not a single-byte ASCII character.</exception>
    public static ByteString ToProto(this char symbol, [CallerMemberName] string? propertyName = null) => char.IsAscii(symbol)
        ? ByteString.CopyFrom((byte) symbol)
        : throw new ArgumentOutOfRangeException(propertyName, symbol, "Value must be a single-byte ASCII character");

    /// <summary>
    /// Converts a CompressionType enum value to the corresponding CompressionTypeVariant protobuf enum value.
    /// </summary>
    /// <param name="compression">CompressionType value to convert.</param>
    /// <returns>>The corresponding CompressionTypeVariant protobuf enum value.</returns>
    public static CompressionTypeVariant ToProto(this CompressionType compression) => (CompressionTypeVariant) compression;

    /// <summary>
    /// Converts a list of partition columns to their protobuf representations.
    /// </summary>
    /// <param name="cols">The partition columns to convert.</param>
    /// <returns>An enumerable of protobuf PartitionColumn messages.</returns>
    public static IEnumerable<PartitionColumn> ToProto(this IReadOnlyList<Formats.PartitionColumn> cols)
    {
        return cols.Select(c => new PartitionColumn
        {
            Name = c.Name,
            ArrowType = c.ArrowType.ToProto()
        });
    }
}