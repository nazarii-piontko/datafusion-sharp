namespace DataFusionSharp.Tests;

public class SqlNamedParameterTests
{
    public static TheoryData<object?, Proto.ScalarValue> ValueMappingTestData => new()
    {
        { null, new Proto.ScalarValue { NullValue = new Proto.ArrowType { NONE = new Proto.EmptyMessage() } } },
        { true, new Proto.ScalarValue { BoolValue = true } },
        { (sbyte)-1, new Proto.ScalarValue { Int8Value = -1 } },
        { (byte)255, new Proto.ScalarValue { Uint8Value = 255 } },
        { (short)123, new Proto.ScalarValue { Int16Value = 123 } },
        { (ushort)65535, new Proto.ScalarValue { Uint16Value = 65535 } },
        { 456, new Proto.ScalarValue { Int32Value = 456 } },
        { 4294967295U, new Proto.ScalarValue { Uint32Value = 4294967295 } },
        { 789L, new Proto.ScalarValue { Int64Value = 789L } },
        { 18446744073709551615UL, new Proto.ScalarValue { Uint64Value = 18446744073709551615 } },
        { 3.14f, new Proto.ScalarValue { Float32Value = 3.14f } },
        { 2.71828, new Proto.ScalarValue { Float64Value = 2.71828 } },
        { "hello", new Proto.ScalarValue { Utf8Value = "hello" } },
        { new byte[] { 0x01, 0x02 }, new Proto.ScalarValue { BinaryValue = Google.Protobuf.ByteString.CopyFrom(0x01, 0x02) } }
    };
    
    [Theory]
    [MemberData(nameof(ValueMappingTestData))]
    public void Constructor_WithValidValueType_ProvidesValidProto(object? value, Proto.ScalarValue expected)
    {
        // Arrange
        var parameter = new SqlNamedParameter("param", value);
        
        // Act & Assert
        Assert.Equal(expected, parameter.ProtoValue);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("$a")]
    public void Constructor_WithInvalidName_Throws(string name)
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => new SqlNamedParameter(name, 42));
    }
}