using Dapper;
using DataFusionSharp;
using DataFusionSharp.Data;
using Microsoft.Extensions.Logging;

// Setup logging
var loggerFactory = LoggerFactory.Create(builder =>
{
    builder
        .SetMinimumLevel(LogLevel.Information)
        .AddSimpleConsole(o => o.IncludeScopes = true);
});
DataFusionNativeLogger.ConfigureLogger(loggerFactory.CreateLogger("DataFusionSharp"), LogLevel.Information);

// Init runtime and single session
using var runtime = DataFusionRuntime.Create();
using var session = runtime.CreateSessionContext();

// Load customers CSV into an in-memory object store
using var store = runtime.CreateInMemoryStore();

var customersCsvBytes = await File.ReadAllBytesAsync(Path.Combine("Data", "orders", "csv", "customers.csv"));
await store.PutAsync("customers.csv", customersCsvBytes);

session.RegisterInMemoryObjectStore("memory://", store);
await session.RegisterCsvAsync("customers", "memory:///customers.csv");

// Register orders from a regular file on disk
await session.RegisterCsvAsync("orders", Path.Combine("Data", "orders", "csv", "orders.csv"));

// Create a connection wrapper around the SessionContext for ADO.NET operations.
await using var connection = session.AsConnection();


// QueryAsync<T> – map rows to a strongly-typed record
Console.WriteLine("=== Completed orders per customer (in-memory JOIN file) ===");

var summaries = await connection.QueryAsync<OrderSummary>(
    """
    SELECT
        c.customer_name     AS CustomerName,
        c.country           AS Country,
        COUNT(*)            AS OrderCount,
        SUM(o.order_amount) AS TotalAmount
    FROM customers AS c
        JOIN orders AS o ON c.customer_id = o.customer_id
    WHERE o.order_status = @status
    GROUP BY c.customer_name, c.country
    ORDER BY TotalAmount DESC
    """,
    new { status = "Completed" });

foreach (var s in summaries)
    Console.WriteLine($"  {s.CustomerName,-25} {s.Country,-10}  orders: {s.OrderCount,3}  total: {s.TotalAmount,10:N0}");

// Model types

record OrderSummary(string CustomerName, string Country, long OrderCount, long TotalAmount);
