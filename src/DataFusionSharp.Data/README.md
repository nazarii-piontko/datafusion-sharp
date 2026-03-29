# DataFusionSharp.Data

ADO.NET provider for [DataFusionSharp](https://www.nuget.org/packages/DataFusionSharp) — enables standard .NET data access patterns (`DbConnection`, `DbCommand`, `DbDataReader`) on top of the high-performance [Apache DataFusion](https://arrow.apache.org/datafusion/) query engine.

> **Note:** This is an independent community project and is not officially associated with or endorsed by the Apache Software Foundation or the Apache DataFusion project.

## Installation

```bash
dotnet add package DataFusionSharp.Data
```

This package depends on [DataFusionSharp](https://www.nuget.org/packages/DataFusionSharp), which will be installed automatically.

## Quick Start

### Raw ADO.NET

```csharp
using DataFusionSharp;
using DataFusionSharp.Data;

// Create runtime and session (managed by the core DataFusionSharp library)
using var runtime = DataFusionRuntime.Create();
using var session = runtime.CreateSessionContext();

// Register data sources on the session
await session.RegisterCsvAsync("orders", "path/to/orders.csv");

// Create an ADO.NET connection wrapping the session
await using var connection = session.AsConnection();
await connection.OpenAsync();

// Execute a query with parameters
await using var command = connection.CreateCommand();
command.CommandText = "SELECT order_id, order_amount FROM orders WHERE order_status = @status";
command.Parameters.Add(new DataFusionSharpParameter("@status", "Completed"));

await using var reader = await command.ExecuteReaderAsync();
while (await reader.ReadAsync())
    Console.WriteLine($"  Order #{reader.GetInt64(0)}: {reader.GetInt64(1)}");
```

### With Dapper

DataFusionSharp.Data is compatible with [Dapper](https://github.com/DapperLib/Dapper) and other libraries that work through `DbConnection`:

```csharp
using Dapper;
using DataFusionSharp;
using DataFusionSharp.Data;

using var runtime = DataFusionRuntime.Create();
using var session = runtime.CreateSessionContext();
await session.RegisterCsvAsync("orders", "path/to/orders.csv");

await using var connection = session.AsConnection();

// Strongly-typed query
var results = await connection.QueryAsync<OrderSummary>(
    "SELECT customer_id AS CustomerId, SUM(order_amount) AS Total FROM orders GROUP BY customer_id");

foreach (var r in results)
    Console.WriteLine($"  Customer {r.CustomerId}: {r.Total}");

// Scalar query with parameters (@param syntax is auto-translated to DataFusion's $param)
var count = await connection.ExecuteScalarAsync<long>(
    "SELECT COUNT(*) FROM orders WHERE order_status = @status",
    new { status = "Completed" });

record OrderSummary(long CustomerId, long Total);
```

## Features

- **`DbConnection`** — wraps a `SessionContext`; `Open`/`Close` manage logical state only (no network connection)
- **`DbCommand`** — executes SQL queries against DataFusion; returns `DbDataReader` or scalar values
- **`DbDataReader`** — forward-only, read-only, streaming row reader over Arrow `RecordBatch` results
- **`DbParameter`** — named parameters with prefix-insensitive lookup (`@`, `$`, `:` prefixes all accepted)
- **Parameter translation** — `@param` placeholders in SQL are automatically translated to DataFusion's native `$param` syntax
- **Dapper compatible** — works out-of-the-box with Dapper's `QueryAsync<T>`, `ExecuteScalarAsync`, and other extensions
- **Extension method** — `session.AsConnection()` for convenient connection creation

## Requirements

- .NET 8.0 or later
- [DataFusionSharp](https://www.nuget.org/packages/DataFusionSharp) (installed automatically)
- Supported platforms:
  - Linux (x64, arm64)
  - Windows (x64)
  - macOS (arm64)

## Documentation

For more information, examples, and source code, visit the [GitHub repository](https://github.com/nazarii-piontko/datafusion-sharp).

See the [QueryDataWithDapper example](https://github.com/nazarii-piontko/datafusion-sharp/tree/main/examples/QueryDataWithDapper) for a complete working sample.

## License

DataFusionSharp is licensed under the Apache License 2.0. See [LICENSE.txt](https://github.com/nazarii-piontko/datafusion-sharp/tree/main/LICENSE.txt) for details.

This project contains bindings to Apache DataFusion, which is also licensed under Apache License 2.0.
See [NOTICE.txt](https://github.com/nazarii-piontko/datafusion-sharp/tree/main/NOTICE.txt) for attribution details.

## Acknowledgments

- [Apache DataFusion](https://arrow.apache.org/datafusion/) - The underlying query engine
- [Apache Arrow](https://arrow.apache.org/) - Columnar memory format
- The Apache Software Foundation

---

Apache®, Apache DataFusion™, Apache Arrow™, and the Apache feather logo are trademarks of The Apache Software Foundation.

