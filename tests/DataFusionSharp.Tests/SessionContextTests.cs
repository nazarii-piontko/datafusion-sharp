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

    [Fact]
    public async Task SqlAsync_OnNonExistentRegisteredCsv_ThrowsOnCollect()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        var fakePath = Path.Combine(Path.GetTempPath(), $"non_existent_{Guid.NewGuid():N}.csv");
        await context.RegisterCsvAsync("ghost_table", fakePath); // It will not fail immediately

        // Act & Assert
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            using var df = await context.SqlAsync("SELECT * FROM ghost_table");
            await df.CollectAsync();
        });
    }

    [Fact]
    public async Task TwoSessions_HaveIsolatedCatalogs()
    {
        // Arrange
        using var sessionA = _runtime.CreateSessionContext();
        using var sessionB = _runtime.CreateSessionContext();
        await sessionA.RegisterCsvAsync("customers", DataSet.CustomersCsvPath);

        // Act & Assert session A
        using var dfA = await sessionA.SqlAsync("SELECT * FROM customers");
        var countA = await dfA.CountAsync();
        Assert.True(countA > 0);

        // Act & Assert session B
        var ex = await Assert.ThrowsAsync<DataFusionException>(async () =>
        {
            using var dfB = await sessionB.SqlAsync("SELECT * FROM customers");
            await dfB.CollectAsync();
        });
        Assert.Contains("customers", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DeregisterTableAsync_RegisteredTable_MakesTableUnavailable()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        await context.RegisterCsvAsync("customers", DataSet.CustomersCsvPath);
        
        // Sanity check - the table should be queryable before deregistration
        using (var dfBefore = await context.SqlAsync("SELECT * FROM customers"))
        {
            var countBefore = await dfBefore.CountAsync();
            Assert.True(countBefore > 0, "countBefore > 0");
        }
        
        // Act
        await context.DeregisterTableAsync("customers");

        // Assert
        var ex = await Assert.ThrowsAsync<DataFusionException>(async () =>
        {
            using var df = await context.SqlAsync("SELECT * FROM customers");
        });
        Assert.Contains("customers", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DeregisterTableAsync_NonExistentTable_DoesNotThrow()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();

        // Act
        await context.DeregisterTableAsync("non_existent_table");
        
        // Assert
        Assert.True(true, "Successfully deregistered a non-existent table without throwing an exception");
    }

    public void Dispose()
    {
        _runtime.Dispose();
    }
}