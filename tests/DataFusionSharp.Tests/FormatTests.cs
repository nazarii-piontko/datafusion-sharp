namespace DataFusionSharp.Tests;

public abstract class FormatTests : IDisposable
{
    protected DataFusionRuntime Runtime { get; }
    
    protected SessionContext Context { get; }

    protected FormatTests()
    {
        Runtime = DataFusionRuntime.Create();
        Context = Runtime.CreateSessionContext();
    }

    protected abstract Task RegisterCustomersTableAsync();
    
    protected abstract Task RegisterOrdersTableAsync();

    [Fact]
    public async Task RegisterTableAsync_CompletesSuccessfully()
    {
        // Arrange

        // Act & Assert
        await RegisterCustomersTableAsync();
    }

    [Fact]
    public async Task QueryRegisteredTable_ReturnsData()
    {
        // Arrange
        await RegisterCustomersTableAsync();

        // Act
        using var df = await Context.SqlAsync("SELECT * FROM customers");
        var count = await df.CountAsync();

        // Assert
        Assert.True(count > 0);
    }

    [Fact]
    public async Task QueryMultipleTables_ReturnsData()
    {
        // Arrange
        await RegisterCustomersTableAsync();
        await RegisterOrdersTableAsync();

        // Act
        using var df = await Context.SqlAsync(
            """
            SELECT 
                c.customer_id,
                COUNT(o.order_id) as order_count
            FROM customers c
            LEFT JOIN orders o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id
            ORDER BY c.customer_id
            """);
        var count = await df.CountAsync();

        // Assert
        Assert.True(count > 0);
    }

    public void Dispose()
    {
        Context.Dispose();
        Runtime.Dispose();
    }
}