namespace DataFusionSharp.Tests;

public sealed class SessionContextTests : IDisposable
{
    private readonly DataFusionRuntime _runtime = DataFusionRuntime.Create();

    [Fact]
    public void CreateSessionContext_ReturnsContext()
    {
        // Act
        using var context = _runtime.CreateSessionContext();

        // Assert
        Assert.NotNull(context);
    }

    [Fact]
    public async Task SqlAsync_ReturnsDataFrame()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();

        // Act
        using var dataFrame = await context.SqlAsync("SELECT 1");

        // Assert
        Assert.NotNull(dataFrame);
    }

    [Theory]
    [InlineData("customers")]
    [InlineData("клієнти")]
    public async Task SqlAsync_WithMissingTable_Throws(string tableName)
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();

        // Act & Assert
        var exception = await Assert.ThrowsAsync<DataFusionException>(async () =>
        {
            using var df = await context.SqlAsync($"SELECT * FROM {tableName}");
        });
        
        Assert.Contains(tableName, exception.Message, StringComparison.Ordinal);
    }

    public void Dispose()
    {
        _runtime.Dispose();
    }
}