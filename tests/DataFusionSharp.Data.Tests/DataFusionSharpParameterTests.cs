using System.Data;

namespace DataFusionSharp.Data.Tests;

public sealed class DataFusionSharpParameterTests
{
    [Fact]
    public void ParameterName_SetAndGet_RoundTrips()
    {
        // Arrange & Act
        var p = new DataFusionSharpParameter
        {
            ParameterName = "@myParam"
        };

        // Verify
        Assert.Equal("@myParam", p.ParameterName);
    }

    [Fact]
    public void Value_SetAndGet_RoundTrips()
    {
        // Arrange & Act
        var p = new DataFusionSharpParameter("@x", 42);

        // Verify
        Assert.Equal(42, p.Value);
    }

    [Fact]
    public void DbType_Default_IsAnsiString()
    {
        // Arrange & Act
        var p = new DataFusionSharpParameter();

        // Verify
        Assert.Equal(DbType.AnsiString, p.DbType);
    }

    [Fact]
    public void IsNullable_Default_IsTrue()
    {
        // Arrange & Act
        var p = new DataFusionSharpParameter();

        // Verify
        Assert.True(p.IsNullable);
    }

    [Fact]
    public void Direction_Default_IsInput()
    {
        // Arrange & Act
        var p = new DataFusionSharpParameter();

        // Verify
        Assert.Equal(ParameterDirection.Input, p.Direction);
    }

    [Fact]
    public void Direction_SetNonInput_ThrowsNotSupportedException()
    {
        // Arrange & Act
        var p = new DataFusionSharpParameter();

        // Verify
        Assert.Throws<NotSupportedException>(() => p.Direction = ParameterDirection.Output);
    }

    [Fact]
    public void ResetDbType_ResetsToAnsiString()
    {
        // Arrange
        var p = new DataFusionSharpParameter
        {
            DbType = DbType.Int32
        };

        // Act
        p.ResetDbType();

        // Verify
        Assert.Equal(DbType.AnsiString, p.DbType);
    }

    [Fact]
    public void Collection_Add_IncreasesCount()
    {
        // Arrange
        var col = new DataFusionSharpParameterCollection();

        // Act
        col.Add(new DataFusionSharpParameter("@a", 1));
        col.Add(new DataFusionSharpParameter("@b", 2));

        // Verify
        Assert.Equal(2, col.Count);
    }

    [Fact]
    public void Collection_AddWithValue_AddsAndReturnsParameter()
    {
        // Arrange
        var col = new DataFusionSharpParameterCollection();

        // Act
        var p = col.AddWithValue("@status", "Completed");

        // Verify
        Assert.Equal(1, col.Count);
        Assert.Equal("@status", p.ParameterName);
        Assert.Equal("Completed", p.Value);
    }

    [Fact]
    public void Collection_Clear_EmptiesCollection()
    {
        // Arrange
        var col = new DataFusionSharpParameterCollection();
        col.AddWithValue("@a", 1);
        col.AddWithValue("@b", 2);

        // Act
        col.Clear();

        // Verify
        Assert.Equal(0, col.Count);
    }

    [Fact]
    public void Collection_Contains_ByName_CaseInsensitive()
    {
        // Arrange & Act
        var col = new DataFusionSharpParameterCollection();
        col.AddWithValue("@order_status", "Completed");

        // Verify
        Assert.True(col.Contains("@order_status"));
        Assert.True(col.Contains("@ORDER_STATUS"));
        Assert.True(col.Contains("@Order_Status"));
    }

    [Fact]
    public void Collection_IndexOf_ByName_CaseInsensitive()
    {
        // Arrange & Act
        var col = new DataFusionSharpParameterCollection();
        col.AddWithValue("@first",  1);
        col.AddWithValue("@second", 2);

        // Verify
        Assert.Equal(0, col.IndexOf("@first"));
        Assert.Equal(0, col.IndexOf("@FIRST"));
        Assert.Equal(1, col.IndexOf("@Second"));
    }

    [Fact]
    public void Collection_IndexOf_UnknownName_ReturnsMinus1()
    {
        // Arrange & Act
        var col = new DataFusionSharpParameterCollection();

        // Verify
        Assert.Equal(-1, col.IndexOf("@ghost"));
    }

    [Fact]
    public void Collection_RemoveAt_ByIndex_RemovesParameter()
    {
        // Arrange
        var col = new DataFusionSharpParameterCollection();
        col.AddWithValue("@a", 1);
        col.AddWithValue("@b", 2);

        // Act
        col.RemoveAt(0);

        // Verify
        Assert.Equal(1, col.Count);
        Assert.Equal(0, col.IndexOf("@b"));
    }

    [Fact]
    public void Collection_RemoveAt_ByName_RemovesParameter()
    {
        // Arrange
        var col = new DataFusionSharpParameterCollection();
        col.AddWithValue("@keep",   1);
        col.AddWithValue("@remove", 2);

        // Act
        col.RemoveAt("@remove");

        // Verify
        Assert.Equal(1, col.Count);
        Assert.Equal(-1, col.IndexOf("@remove"));
    }

    [Fact]
    public void Collection_Insert_AtIndex_ShiftsOtherParams()
    {
        // Arrange
        var col = new DataFusionSharpParameterCollection();
        col.AddWithValue("@a", 1);
        col.AddWithValue("@c", 3);

        // Act
        col.Insert(1, new DataFusionSharpParameter("@b", 2));

        // Verify
        Assert.Equal(3, col.Count);
        Assert.Equal(1, col.IndexOf("@b"));
        Assert.Equal(2, col.IndexOf("@c"));
    }

    [Fact]
    public void Collection_Add_NonParameterObject_ThrowsArgumentException()
    {
        // Arrange
        var col = new DataFusionSharpParameterCollection();

        // Act & Verify
        Assert.Throws<ArgumentException>(() => col.Add("not a parameter"));
    }
}
