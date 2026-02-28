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
    
    internal static Proto.ScalarValue ToProtoScalarValue(this object? value) => value switch
    {
        null => new Proto.ScalarValue { NullValue = new Proto.ArrowType { NONE = new Proto.EmptyMessage() } },
        bool v => new Proto.ScalarValue { BoolValue = v },
        sbyte v => new Proto.ScalarValue { Int8Value = v },
        byte v => new Proto.ScalarValue { Uint8Value = v },
        short v => new Proto.ScalarValue { Int16Value = v },
        ushort v => new Proto.ScalarValue { Uint16Value = v },
        int v => new Proto.ScalarValue { Int32Value = v },
        uint v => new Proto.ScalarValue { Uint32Value = v },
        long v => new Proto.ScalarValue { Int64Value = v },
        ulong v => new Proto.ScalarValue { Uint64Value = v },
        float v => new Proto.ScalarValue { Float32Value = v },
        double v => new Proto.ScalarValue { Float64Value = v },
        string v => new Proto.ScalarValue { Utf8Value = v },
        byte[] v => new Proto.ScalarValue { BinaryValue = ByteString.CopyFrom(v) },
        _ => throw new ArgumentException($"Unsupported parameter type '{value.GetType().FullName}', only primitive types and byte arrays are supported", nameof(value))
    };
}
