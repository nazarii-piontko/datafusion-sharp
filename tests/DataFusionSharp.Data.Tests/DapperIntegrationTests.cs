using Dapper;

namespace DataFusionSharp.Data.Tests;

public sealed class DapperIntegrationTests : IAsyncLifetime
{
    private readonly DataFusionRuntime _runtime = DataFusionRuntime.Create();
    private SessionContext _session = null!;
    private DataFusionSharpConnection _connection = null!;

    public async Task InitializeAsync()
    {
        _session = _runtime.CreateSessionContext();
        
        await _session.RegisterCsvAsync("orders",    DataSet.OrdersCsvPath);
        await _session.RegisterCsvAsync("customers", DataSet.CustomersCsvPath);
        
        _connection = _session.AsConnection(leaveOpen: true);
    }

    public Task DisposeAsync()
    {
        _connection.Dispose();
        _session.Dispose();
        _runtime.Dispose();
        return Task.CompletedTask;
    }

    [Fact]
    public async Task QueryAsync_ReturnsMappedRows()
    {
        // Act
        var rows = (await _connection.QueryAsync<Order>(
                "SELECT order_id AS OrderId, order_amount AS OrderAmount FROM orders"))
            .ToList();

        // Assert
        Assert.NotEmpty(rows);
        Assert.All(rows, r =>
        {
            Assert.True(r.OrderId > 0);
            Assert.True(r.OrderAmount > 0);
        });
    }

    [Fact]
    public async Task QueryAsync_WithParameter_ReturnsMappedRows()
    {
        // Act
        var rows = (await _connection.QueryAsync<Order>(
                "SELECT order_id AS OrderId, order_amount AS OrderAmount FROM orders WHERE order_status = @status",
                new { status = "Completed" }))
            .ToList();

        // Assert
        Assert.NotEmpty(rows);
        Assert.All(rows, r =>
        {
            Assert.True(r.OrderId > 0);
            Assert.True(r.OrderAmount > 0);
        });
    }

    [Fact]
    public async Task ExecuteScalarAsync_CountAllOrders_ReturnsPositiveValue()
    {
        // Act
        var count = await _connection.ExecuteScalarAsync<long>("SELECT COUNT(*) FROM orders");

        // Assert
        Assert.True(count > 0);
    }

    [Fact]
    public async Task ExecuteScalarAsync_WithParameter_ReturnsFilteredAggregate()
    {
        // Act
        var total = await _connection.ExecuteScalarAsync<long>(
            "SELECT COALESCE(SUM(order_amount), 0) FROM orders WHERE order_status = @status",
            new { status = "Completed" });

        // Assert
        Assert.True(total > 0);
    }
    
    private sealed record Order(long OrderId, long OrderAmount);
}
