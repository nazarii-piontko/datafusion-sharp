# DataFusionSharp

[![CI](https://github.com/nazarii-piontko/datafusion-sharp/actions/workflows/ci.yml/badge.svg)](https://github.com/nazarii-piontko/datafusion-sharp/actions/workflows/ci.yml)
[![.NET](https://img.shields.io/badge/.NET-8.0+-purple.svg)](https://dotnet.microsoft.com/download)
[![Rust](https://img.shields.io/badge/Rust-1.93+-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE.txt)

.NET bindings for [Apache DataFusion](https://arrow.apache.org/datafusion/), a fast, extensible query engine built on Apache Arrow for high-performance analytical query processing.

> **Note:** This is an independent community project and is not officially associated with or endorsed by the Apache Software Foundation or the Apache DataFusion project.

## Features

| Component        | Feature                                      | Status | Notes                                     |
|------------------|----------------------------------------------|--------|-------------------------------------------|
| **Runtime**      | Create Tokio runtime                         | ✅      | Configurable threads                      |
|                  | Graceful shutdown                            | ✅      |                                           |
|                  | Multiple runtime instances                   | ✅      |                                           |
| **Session**      | Create session context                       | ✅      |                                           |
|                  | Execute SQL queries                          | ✅      | Returns DataFrame                         |
| **Data Sources** |                                              |        |                                           |
|                  | CSV read                                     | 🟡     | Basic, no options exposed                 |
|                  | CSV write                                    | 🟡     | Basic, no options exposed                 |
|                  | Parquet read                                 | 🟡     | Basic, no options exposed                 |
|                  | Parquet write                                | 🟡     | Basic, no options exposed                 |
|                  | JSON read                                    | 🟡     | Basic, no options exposed                 |
|                  | JSON write                                   | 🟡     | Basic, no options exposed                 |
|                  | In-memory tables                             | ❌      |                                           |
| **DataFrame**    |                                              |        |                                           |
|                  | Count rows                                   | ✅      | `CountAsync()`                            |
|                  | Get schema                                   | ✅      | `GetSchemaAsync()` → Arrow Schema         |
|                  | Collect all data                             | ✅      | `CollectAsync()` → RecordBatches          |
|                  | Stream results                               | ✅      | `ExecuteStreamAsync()` → IAsyncEnumerable |
|                  | Show/print                                   | ✅      | `ShowAsync()`, `ToStringAsync()`          |
|                  | Select, Aggregate, Join, Filter, Limit, Sort | ❌      | Use SQL instead                           |
|                  | Explain plan                                 | ❌      |                                           |
|                  | Write to file                                | ❌      |                                           |
| **Arrow**        | Apache Arrow support                         | ✅      | Via Apache.Arrow nuget package            |
| **Advanced**     |                                              |        |                                           |
|                  | UDF registration                             | ❌      |                                           |
|                  | Catalog management                           | ❌      |                                           |
|                  | Table providers                              | ❌      |                                           |
| **Platforms**    | Linux x64                                    | ✅      |                                           |
|                  | Linux arm64                                  | ❌      |                                           |
|                  | Windows x64                                  | ❌      |                                           |
|                  | macOS arm64                                  | ❌      |                                           |

✅ Implemented    🟡 Partially implemented    ❌ Not yet implemented

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

## Requirements

- .NET 8.0 or later
- Supported platforms:
  - Linux (x64, arm64)
  - Windows (x64)
  - macOS (arm64)

## Building from Source

### Prerequisites

- .NET 10.0 SDK or later
- Rust toolchain (1.93+) - Install from https://rustup.rs

### Build Steps

1. **Clone the repository:**
   ```bash
   git clone https://github.com/nazarii-piontko/datafusion-sharp.git
   cd datafusion-sharp
   ```

2. **Build the project:**
   ```bash
   dotnet build -c Release
   ```

   This will automatically:
   - Compile the Rust native library (via cargo)
   - Build the .NET library
   - Link the native library into the managed library

3. **Run tests:**
   ```bash
   dotnet test -c Release
   ```

## Documentation

TODO: Documentation is in progress. Please refer to the [source code](src/DataFusionSharp) and [examples](examples) for usage details.

## Project Structure

- [**src/DataFusionSharp/**](src/DataFusionSharp) - Core .NET library with managed wrappers
- [**native/**](native) - Rust FFI layer bridging .NET to Apache DataFusion
- [**tests/DataFusionSharp.Tests/**](tests/DataFusionSharp.Tests) - Integration tests
- [**tests/DataFusionSharp.Benchmark/**](tests/DataFusionSharp.Benchmark) - Performance benchmarks with native reference implementation
- [**examples/**](examples) - Example usage and sample data

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

DataFusionSharp is licensed under the Apache License 2.0. See [LICENSE.txt](LICENSE.txt) for details.

This project contains bindings to Apache DataFusion, which is also licensed under Apache License 2.0.
See [NOTICE.txt](NOTICE.txt) for attribution details.

## Acknowledgments

- [Apache DataFusion](https://arrow.apache.org/datafusion/) - The underlying query engine
- [Apache Arrow](https://arrow.apache.org/) - Columnar memory format
- The Apache Software Foundation

## Related Projects

- [Apache DataFusion](https://github.com/apache/datafusion) - Rust implementation
- [datafusion-python](https://github.com/apache/datafusion-python) - Python bindings
- [datafusion-java](https://github.com/apache/datafusion-java) - Java bindings

---

Apache®, Apache DataFusion™, Apache Arrow™, and the Apache feather logo are trademarks of The Apache Software Foundation.
