using DataFusionSharp.Expressions;

namespace DataFusionSharp.Tests;

public class ProtoGenericExtensionsTests
{
    [Theory]
    [InlineData("Hello, World!")]
    [InlineData("")]
    public void ToProto_ShouldConvertStringToByteString(string str)
    {
        // Act
        var result = str.ToProto();

        // Assert
        Assert.Equal(str.Length, result.Length);
    }

    [Theory]
    [InlineData('A')]
    [InlineData('z')]
    public void ToProto_ShouldConvertAsciiCharToByteString(char symbol)
    {
        // Act
        var result = symbol.ToProto();

        // Assert
        Assert.Single(result);
        Assert.Equal((byte)symbol, result[0]);
    }
}

public class ProtoInsertOpExtensionsTests
{
    [Theory]
    [InlineData(Proto.InsertOp.Append, InsertOp.Append)]
    [InlineData(Proto.InsertOp.Overwrite, InsertOp.Overwrite)]
    [InlineData(Proto.InsertOp.Replace, InsertOp.Replace)]
    public void ToInsertOp_ShouldConvertProtoInsertOpVariant(Proto.InsertOp expected, InsertOp original)
    {
        // Act
        var result = original.ToProto();

        // Assert
        Assert.Equal(expected, result);
    }
}