using Apache.Arrow;

namespace DataFusionSharp.Tests;

public sealed class ArrayExtensionsTests
{
    [Fact]
    public void AsBool_BooleanArray_ReturnsValues()
    {
        // Arrange
        using var array = new BooleanArray.Builder().Append(true).Append(false).Build();

        // Act
        var result = array.AsBool().ToList();

        // Assert
        Assert.Equal([true, false], result);
    }

    [Fact]
    public void AsBool_IArrowArray_ReturnsValues()
    {
        // Arrange
        using IArrowArray array = new BooleanArray.Builder().Append(true).AppendNull().Build();

        // Act
        var result = array.AsBool().ToList();

        // Assert
        Assert.Equal([true, null], result);
    }

    [Fact]
    public void AsInt64_Int64Array_ReturnsValues()
    {
        // Arrange
        var array = new Int64Array.Builder().Append(10).Append(20).Build();

        // Act
        var result = array.AsInt64().ToList();

        // Assert
        Assert.Equal([10L, 20L], result);
    }

    [Fact]
    public void AsInt64_IArrowArray_ReturnsValues()
    {
        // Arrange
        using IArrowArray array = new Int64Array.Builder().Append(42).AppendNull().Build();

        // Act
        var result = array.AsInt64().ToList();

        // Assert
        Assert.Equal([42L, null], result);
    }
    
    [Fact]
    public void AsInt64_IArrowArray_WithWrongType_Throws()
    {
        // Arrange
        using IArrowArray array = new BooleanArray.Builder().Append(false).Build();

        // Act and Assert
        Assert.Throws<ArgumentException>(() => array.AsInt64().ToList());
    }

    [Fact]
    public void AsDouble_DoubleArray_ReturnsValues()
    {
        // Arrange
        using var array = new DoubleArray.Builder().Append(1.1).Append(2.2).Build();

        // Act
        var result = array.AsDouble().ToList();

        // Assert
        Assert.Equal([1.1, 2.2], result);
    }

    [Fact]
    public void AsDouble_IArrowArray_ReturnsValues()
    {
        // Arrange
        using IArrowArray array = new DoubleArray.Builder().Append(3.14).AppendNull().Build();

        // Act
        var result = array.AsDouble().ToList();

        // Assert
        Assert.Equal([3.14, null], result);
    }
    
    [Fact]
    public void AsDouble_IArrowArray_WithWrongType_Throws()
    {
        // Arrange
        using IArrowArray array = new BooleanArray.Builder().Append(false).Build();

        // Act and Assert
        Assert.Throws<ArgumentException>(() => array.AsDouble().ToList());
    }

    [Fact]
    public void AsString_StringArray_ReturnsValues()
    {
        // Arrange
        using var array = new StringArray.Builder().Append("hello").Append("world").Build();

        // Act
        var result = array.AsString().ToList();

        // Assert
        Assert.Equal(["hello", "world"], result);
    }

    [Fact]
    public void AsString_StringViewArray_ReturnsValues()
    {
        // Arrange
        using var array = new StringViewArray.Builder().Append("alpha").Append("beta").Build();

        // Act
        var result = array.AsString().ToList();

        // Assert
        Assert.Equal(["alpha", "beta"], result);
    }

    [Fact]
    public void AsString_LargeStringArray_ReturnsValues()
    {
        // Arrange
        var stringData = "largestring"u8.ToArray();
        using var abNulls = new ArrowBuffer.BitmapBuilder().Build();
        using var abOffsets = new ArrowBuffer.Builder<long>().Append(0).Append("large".Length) .Append(stringData.Length).Build();
        using var abData = new ArrowBuffer.Builder<byte>().Append(stringData.AsSpan()).Build();
        using var array = new LargeStringArray(2, abOffsets, abData, abNulls);

        // Act
        var result = array.AsString().ToList();

        // Assert
        Assert.Equal(["large", "string"], result);
    }

    [Fact]
    public void AsString_IArrowArray_ReturnsValues()
    {
        // Arrange
        using IArrowArray array = new StringArray.Builder().Append("test").AppendNull().Build();

        // Act
        var result = array.AsString().ToList();

        // Assert
        Assert.Equal(["test", null], result);
    }
    
    [Fact]
    public void AsString_IArrowArray_WithWrongType_Throws()
    {
        // Arrange
        using IArrowArray array = new BooleanArray.Builder().Append(false).Build();

        // Act and Assert
        Assert.Throws<ArgumentException>(() => array.AsString().ToList());
    }
}
