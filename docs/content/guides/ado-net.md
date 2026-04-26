---
sidebar_position: 7
title: ADO.NET Provider
---

# ADO.NET Provider

`DataFusionSharp.Data` provides a standard `System.Data.Common` implementation (`DbConnection`, `DbCommand`, `DbDataReader`) that wraps a `SessionContext`. This lets you use DataFusion with Dapper, or any other ADO.NET-compatible library.

## Installation

```bash
dotnet add package DataFusionSharp.Data
```

## Creating a Connection

Register your data sources on a `SessionContext`, then wrap it as a `DbConnection`:

```csharp
using DataFusionSharp;
using DataFusionSharp.Data;

using var runtime = DataFusionRuntime.Create();
using var session = runtime.CreateSessionContext();

// Register tables first -- this is a DataFusion-specific operation
await session.RegisterCsvAsync("orders", "path/to/orders.csv");
await session.RegisterParquetAsync("products", "path/to/products.parquet");

// Create a standard DbConnection
await using var connection = session.AsConnection();
```

By default, disposing the connection also disposes the underlying `SessionContext`. Pass `leaveOpen: true` to keep the session alive:

```csharp
await using var connection = session.AsConnection(leaveOpen: true);
```

## Executing Commands

Use `DataFusionSharpCommand` directly for raw ADO.NET access:

```csharp
await using var cmd = connection.CreateCommand();
cmd.CommandText = "SELECT COUNT(*) FROM orders WHERE status = @status";
cmd.Parameters.Add(new DataFusionSharpParameter("@status", "Completed"));

var count = (long)(await cmd.ExecuteScalarAsync())!;
```

The `@paramName` syntax in SQL is automatically translated to DataFusion's native `$paramName` format, so standard ADO.NET parameter conventions work out of the box.

## Using with Dapper

### Strongly-Typed Queries

Map rows directly to C# records or classes with `QueryAsync<T>`. Column aliases must match property names:

```csharp
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

record OrderSummary(string CustomerName, long OrderCount, long TotalAmount);
```

### Dynamic Queries

When you don't need a mapping class, omit the type parameter:

```csharp
var customers = await connection.QueryAsync(
    "SELECT * FROM customers ORDER BY customer_id");

foreach (var c in customers)
    Console.WriteLine($"[{c.customer_id}] {c.customer_name}");
```

### Single-Row Queries

Fetch one row (or `null`) with `QueryFirstOrDefaultAsync<T>`:

```csharp
var largest = await connection.QueryFirstOrDefaultAsync<SingleOrder>(
    """
    SELECT o.order_id AS OrderId, c.customer_name AS CustomerName, o.order_amount AS Amount
    FROM orders AS o
        JOIN customers AS c ON o.customer_id = c.customer_id
    ORDER BY o.order_amount DESC
    LIMIT 1
    """);

record SingleOrder(long OrderId, string CustomerName, long Amount);
```

### Scalar Queries

Return a single aggregate value with `ExecuteScalarAsync<T>`:

```csharp
var totalOrders = await connection.ExecuteScalarAsync<long>(
    "SELECT COUNT(*) FROM orders");

var revenue = await connection.ExecuteScalarAsync<long>(
    "SELECT SUM(order_amount) FROM orders WHERE order_status = @status",
    new { status = "Completed" });
```

## Parameters

Parameter names accept `@`, `$`, or `:` prefixes -- all are normalized internally. The `@param` syntax in SQL is auto-translated to `$param` for DataFusion. Dapper's anonymous objects (`new { status = "Completed" }`) work seamlessly.

### Type Mapping

| .NET Type        | DataFusion ScalarValue   |
|------------------|--------------------------|
| `bool`           | `Boolean`                |
| `sbyte`          | `Int8`                   |
| `short`          | `Int16`                  |
| `int`            | `Int32`                  |
| `long`           | `Int64`                  |
| `byte`           | `UInt8`                  |
| `ushort`         | `UInt16`                 |
| `uint`           | `UInt32`                 |
| `ulong`          | `UInt64`                 |
| `float`          | `Float32`                |
| `double`         | `Float64`                |
| `decimal`        | `Decimal128`             |
| `string`         | `Utf8`                   |
| `char`           | `Utf8`                   |
| `byte[]`         | `Binary`                 |
| `DateOnly`       | `Date32`                 |
| `TimeOnly`       | `Time64Microsecond`      |
| `DateTime`       | `TimestampMicrosecond`   |
| `DateTimeOffset` | `TimestampMicrosecond`   |
| `TimeSpan`       | `DurationMicrosecond`    |

Null values use `DbType` to infer the appropriate ScalarValue variant. Unknown types fall back to `Utf8` via `ToString()`.

You can also pass a `ScalarValue` directly as a parameter value for full control over the DataFusion type.

## Data Reader

`DataFusionSharpDataReader` is a forward-only, read-only reader that streams Arrow record batches from DataFusion.

```csharp
await using var cmd = connection.CreateCommand();
cmd.CommandText = "SELECT order_id, customer_name, order_amount FROM orders";
await using var reader = await cmd.ExecuteReaderAsync();

while (await reader.ReadAsync())
{
    var id = reader.GetInt64(0);
    var name = reader.GetString(1);
    var amount = reader.GetInt64(2);
}
```

Key characteristics:

- **Typed getters**: `GetBoolean`, `GetByte`, `GetInt16`, `GetInt32`, `GetInt64`, `GetFloat`, `GetDouble`, `GetDecimal`, `GetString`, `GetDateTime`, `GetGuid`
- **Column lookup**: `GetOrdinal(name)` is case-insensitive; indexer by name (`reader["column"]`) also works
- **Null checking**: Use `IsDBNull(ordinal)` before typed getters; accessing a null value throws `InvalidCastException`
- **Dispose**: Always dispose the reader to release native Arrow memory

## Limitations

- **No transactions** -- `BeginTransaction()` throws `NotSupportedException`
- **No stored procedures** -- only `CommandType.Text` is supported
- **ExecuteNonQuery returns -1** -- DataFusion is an analytical engine and does not track affected row counts
- **Single result set** -- `NextResult()` always returns `false`
- **No synchronous Cancel** -- `Cancel()` throws `NotSupportedException`; however, `CancellationToken` is fully supported on `ExecuteScalarAsync`, `ExecuteNonQueryAsync`, and `ExecuteReaderAsync`, propagating cancellation to the native runtime
- **Read-only** -- DataFusion does not support INSERT/UPDATE/DELETE on registered tables

For the core DataFusion parameter syntax and ScalarValue types, see [Querying Data](./querying-data).
