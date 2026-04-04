namespace DataFusionSharp.Tests;

public sealed class InMemoryObjectStoreTests : IDisposable
{
    private readonly DataFusionRuntime _runtime = DataFusionRuntime.Create();

    [Fact]
    public void CreateInMemoryStore_ReturnsNonNullInstance()
    {
        // Act
        using var store = _runtime.CreateInMemoryStore();

        // Assert
        Assert.NotNull(store);
        Assert.Same(_runtime, store.Runtime);
    }

    [Fact]
    public async Task PutAsync_AndRegister_ThenQueryCsv_ReturnsData()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var store = _runtime.CreateInMemoryStore();
        
        var csvBytes = await File.ReadAllBytesAsync(DataSet.CustomersCsvPath);

        // Act
        await store.PutAsync("customers.csv", csvBytes);
        context.RegisterInMemoryObjectStore("memory://", store);
        await context.RegisterCsvAsync("customers", "memory:///customers.csv");

        // Assert
        using var df = await context.SqlAsync("SELECT * FROM customers");
        var count = await df.CountAsync();
        Assert.Equal(10UL, count);
    }

    [Fact]
    public async Task PutAsync_AndRegister_ThenQueryJson_ReturnsData()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var store = _runtime.CreateInMemoryStore();
        
        var jsonBytes = await File.ReadAllBytesAsync(DataSet.CustomersJsonPath);

        // Act
        await store.PutAsync("customers.json", jsonBytes);
        context.RegisterInMemoryObjectStore("memory://", store);
        await context.RegisterJsonAsync("customers", "memory:///customers.json");

        // Assert
        using var df = await context.SqlAsync("SELECT * FROM customers");
        var count = await df.CountAsync();
        Assert.Equal(10UL, count);
    }

    [Fact]
    public async Task PutAsync_AndRegister_ThenQueryParquet_ReturnsData()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var store = _runtime.CreateInMemoryStore();
        
        var parquetBytes = await File.ReadAllBytesAsync(DataSet.CustomersParquetPath);

        // Act
        await store.PutAsync("customers.parquet", parquetBytes);
        context.RegisterInMemoryObjectStore("memory://", store);
        await context.RegisterParquetAsync("customers", "memory:///customers.parquet");

        // Assert
        using var df = await context.SqlAsync("SELECT * FROM customers");
        var count = await df.CountAsync();
        Assert.Equal(10UL, count);
    }

    [Fact]
    public async Task DeleteAsync_RemovesData_QueryFails()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var store = _runtime.CreateInMemoryStore();
        
        var csvBytes = await File.ReadAllBytesAsync(DataSet.CustomersCsvPath);
        
        await store.PutAsync("customers.csv", csvBytes);

        context.RegisterInMemoryObjectStore("memory://", store);
        await context.RegisterCsvAsync("customers", "memory:///customers.csv");

        // Sanity check — data is queryable before delete
        using (var dfBefore = await context.SqlAsync("SELECT * FROM customers"))
        {
            var countBefore = await dfBefore.CountAsync();
            Assert.Equal(10UL, countBefore);
        }

        // Act
        await store.DeleteAsync("customers.csv");

        // Assert
        // Query should return no data after delete
        using (var dfAfter = await context.SqlAsync("SELECT * FROM customers"))
        {
            var countAfter = await dfAfter.CountAsync();
            Assert.Equal(0UL, countAfter);
        }
    }

    [Fact]
    public async Task PutAsync_NullArguments_Throw()
    {
        // Arrange
        using var store = _runtime.CreateInMemoryStore();

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => store.PutAsync(null!, new byte[] {1, 2, 3}));
        await Assert.ThrowsAsync<ArgumentException>(() => store.PutAsync("path", null!));
    }

    [Fact]
    public async Task DeleteAsync_NullOrEmptyPath_Throws()
    {
        // Arrange
        using var store = _runtime.CreateInMemoryStore();

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => store.DeleteAsync(""));
        await Assert.ThrowsAsync<ArgumentNullException>(() => store.DeleteAsync(null!));
    }

    [Fact]
    public async Task SameStore_RegisteredInTwoSessions_BothCanQuery()
    {
        // Arrange
        using var contextA = _runtime.CreateSessionContext();
        using var contextB = _runtime.CreateSessionContext();
        using var store = _runtime.CreateInMemoryStore();

        var csvBytes = await File.ReadAllBytesAsync(DataSet.CustomersCsvPath);
        await store.PutAsync("customers.csv", csvBytes);

        // Act
        contextA.RegisterInMemoryObjectStore("memory://", store);
        contextB.RegisterInMemoryObjectStore("memory://", store);

        await contextA.RegisterCsvAsync("customers", "memory:///customers.csv");
        await contextB.RegisterCsvAsync("customers", "memory:///customers.csv");

        // Assert
        using (var dfA = await contextA.SqlAsync("SELECT * FROM customers"))
        {
            var countA = await dfA.CountAsync();
            Assert.Equal(10UL, countA);
        }

        using (var dfB = await contextB.SqlAsync("SELECT * FROM customers"))
        {
            var countB = await dfB.CountAsync();
            Assert.Equal(10UL, countB);
        }
    }

    [Fact]
    public async Task PutAsStaticAsync_AndRegister_ThenQueryCsv_ReturnsData()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var store = _runtime.CreateInMemoryStore();

        var csvBytes = await File.ReadAllBytesAsync(DataSet.CustomersCsvPath);
        using var memoryHandle = csvBytes.AsMemory().Pin();
        
        // Act
        await store.PutAsStaticAsync("customers.csv", memoryHandle, csvBytes.Length);
        context.RegisterInMemoryObjectStore("memory://", store);
        await context.RegisterCsvAsync("customers", "memory:///customers.csv");

        // Assert
        using var df = await context.SqlAsync("SELECT * FROM customers");
        var count = await df.CountAsync();
        Assert.Equal(10UL, count);
    }

    [Fact]
    public async Task TwoStores_RegisterBoth_ThenJoinQuery_ReturnsData()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var storeA = _runtime.CreateInMemoryStore();
        using var storeB = _runtime.CreateInMemoryStore();

        var customersCsv = await File.ReadAllBytesAsync(DataSet.CustomersCsvPath);
        var ordersCsv = await File.ReadAllBytesAsync(DataSet.OrdersCsvPath);

        // Act
        await storeA.PutAsync("customers.csv", customersCsv);
        await storeB.PutAsync("orders.csv", ordersCsv);

        context.RegisterInMemoryObjectStore("mem-a://", storeA);
        context.RegisterInMemoryObjectStore("mem-b://", storeB);

        await context.RegisterCsvAsync("customers", "mem-a:///customers.csv");
        await context.RegisterCsvAsync("orders", "mem-b:///orders.csv");

        // Assert
        using (var dfCustomers = await context.SqlAsync("SELECT * FROM customers"))
        {
            var count = await dfCustomers.CountAsync();
            Assert.Equal(10UL, count);
        }

        using (var dfOrders = await context.SqlAsync("SELECT * FROM orders"))
        {
            var count = await dfOrders.CountAsync();
            Assert.Equal(50UL, count);
        }
        
        using (var dfJoin = await context.SqlAsync(
            "SELECT c.customer_name, o.order_id " +
            "FROM customers c " +
            "JOIN orders o ON c.customer_id = o.customer_id"))
        {
            var count = await dfJoin.CountAsync();
            Assert.Equal(50UL, count);
        }
    }

    public void Dispose()
    {
        _runtime.Dispose();
    }
}

