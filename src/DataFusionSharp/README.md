# DataFusionSharp

.NET bindings for [Apache DataFusion](https://arrow.apache.org/datafusion/), a fast, extensible query engine built on Apache Arrow for high-performance analytical query processing.

> **Note:** This is an independent community project and is not officially associated with or endorsed by the Apache Software Foundation or the Apache DataFusion project.

## Installation

```bash
dotnet add package DataFusionSharp
```

## Quick Start

```csharp
using DataFusionSharp;

await using var runtime = DataFusionRuntime.Create();
using var context = runtime.CreateSessionContext();

// Register a CSV file as a table
await context.RegisterCsvAsync("orders", "path/to/orders.csv");

// Execute SQL query
using var df = await context.SqlAsync(
    "SELECT customer_id, sum(amount) AS total FROM orders GROUP BY customer_id");

// Display results to console
await df.ShowAsync();

// Access schema
var schema = await df.GetSchemaAsync();
foreach (var field in schema.FieldsList)
    Console.WriteLine($"- {field.Name}: {field.DataType}");

// Collect as Arrow batches
var data = await df.CollectAsync();
foreach (var batch in data.Batches)
{
    // Process Arrow RecordBatch...
}
```

## Features

- **Runtime Management** - Create and manage Tokio runtime with configurable threads
- **Session Context** - Execute SQL queries with full DataFusion SQL support
- **Data Sources** - Read and write CSV, Parquet, and JSON files
- **DataFrame API** - Count rows, get schema, collect data, and stream results

## Requirements

- .NET 8.0 or later
- Supported platforms:
  - Linux (x64, arm64)
  - Windows (x64)
  - macOS (arm64)

## Documentation

For more information, examples, and source code, visit the [GitHub repository](https://github.com/nazarii-piontko/datafusion-sharp).

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
