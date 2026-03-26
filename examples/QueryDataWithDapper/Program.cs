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
DataFusionNativeLogger.ConfigureLogger(loggerFactory.CreateLogger("DataFusionSharp"), LogLevel.Debug);

// Init runtime and single session
using var runtime = DataFusionRuntime.Create();
using var session = runtime.CreateSessionContext();

// Register CSV tables directly on the SessionContext before using ADO.NET.
// Table registration is a DataFusion-specific operation and is done outside the connection.
var dataPath = Path.Combine(AppContext.BaseDirectory, "Data", "orders", "csv");
await session.RegisterCsvAsync("orders",    Path.Combine(dataPath, "orders.csv"));
await session.RegisterCsvAsync("customers", Path.Combine(dataPath, "customers.csv"));


// Create a connection wrapper around the SessionContext for ADO.NET operations.
await using var connection = session.AsConnection();


// QueryAsync<T> – map rows to a strongly-typed record
Console.WriteLine("=== Completed orders per customer (Dapper QueryAsync<T>) ===");

var summaries = await connection.QueryAsync<OrderSummary>(
    """
    SELECT
        c.customer_name     AS CustomerName,
        COUNT(*)            AS OrderCount,
        SUM(o.order_amount) AS TotalAmount
    FROM orders AS o
        JOIN customers AS c ON o.customer_id = c.customer_id
    WHERE o.order_status = @status
    GROUP BY c.customer_name
    ORDER BY TotalAmount DESC
    """,
    new { status = "Completed" });

foreach (var s in summaries)
    Console.WriteLine($"  {s.CustomerName,-25}  orders: {s.OrderCount,3}  total: {s.TotalAmount,10:N0}");


// QueryAsync (dynamic) – no mapping class needed
Console.WriteLine("\n=== All customers (dynamic rows) ===");

var customers = await connection.QueryAsync("SELECT * FROM customers ORDER BY customer_id");
foreach (var c in customers)
    Console.WriteLine($"  [{c.customer_id}] {c.customer_name,-25} ({c.country}, {c.customer_segment})");


// QueryFirstOrDefaultAsync – fetch a single row
Console.WriteLine("\n=== Largest single order ===");

var largest = await connection.QueryFirstOrDefaultAsync<SingleOrder>(
    """
    SELECT
        o.order_id    AS OrderId,
        c.customer_name AS CustomerName,
        o.order_amount  AS Amount,
        o.order_status  AS Status
    FROM orders AS o
        JOIN customers AS c ON o.customer_id = c.customer_id
    ORDER BY o.order_amount DESC
    LIMIT 1
    """);

if (largest is not null)
    Console.WriteLine($"  Order #{largest.OrderId} – {largest.CustomerName}: {largest.Amount:N0} ({largest.Status})");

// ExecuteScalarAsync – single aggregate value
Console.WriteLine("\n=== Aggregate scalars ===");

var totalOrders = await connection.ExecuteScalarAsync<long>("SELECT COUNT(*) FROM orders");
var totalRevenue = await connection.ExecuteScalarAsync<long>("SELECT SUM(order_amount) FROM orders WHERE order_status = @status", new { status = "Completed" });

Console.WriteLine($"  Total orders  : {totalOrders}");
Console.WriteLine($"  Completed rev.: {totalRevenue:N0}");


// Model types

record OrderSummary(string CustomerName, long OrderCount, long TotalAmount);

record SingleOrder(long OrderId, string CustomerName, long Amount, string Status);
