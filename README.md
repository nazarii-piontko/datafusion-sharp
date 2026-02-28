# DataFusionSharp

[![CI](https://github.com/nazarii-piontko/datafusion-sharp/actions/workflows/ci.yml/badge.svg)](https://github.com/nazarii-piontko/datafusion-sharp/actions/workflows/ci.yml)
[![.NET](https://img.shields.io/badge/.NET-8.0+-purple.svg)](https://dotnet.microsoft.com/download)
[![Rust](https://img.shields.io/badge/Rust-1.93+-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE.txt)

.NET bindings for [Apache DataFusion](https://arrow.apache.org/datafusion/), a fast, extensible query engine built on Apache Arrow for high-performance analytical query processing.

> **Note:** This is an independent community project and is not officially associated with or endorsed by the Apache Software Foundation or the Apache DataFusion project.

## Features

| Component        | Feature                                      | Status | Notes                                             |
|------------------|----------------------------------------------|--------|---------------------------------------------------|
| **Runtime**      | Tokio runtime                                | ✅      | Configurable threads, supports multiple instances |
| **Session**      | Create session context                       | ✅      |                                                   |
|                  | Execute SQL queries                          | ✅      | Returns DataFrame, supports parameters            |
| **Data Sources** | CSV read                                     | ✅     |                                                   |
|                  | CSV write                                    | ✅     |                                                   |
|                  | Parquet read                                 | 🟡     | Basic, no options exposed                         |
|                  | Parquet write                                | 🟡     | Basic, no options exposed                         |
|                  | JSON read                                    | ✅     |                                                   |
|                  | JSON write                                   | ✅     |                                                   |
|                  | In-memory tables                             | ❌      |                                                   |
| **DataFrame**    | Count rows                                   | ✅      | `CountAsync()`                                    |
|                  | Get schema                                   | ✅      | `GetSchemaAsync()` → Arrow Schema                 |
|                  | Collect all data                             | ✅      | `CollectAsync()` → RecordBatches                  |
|                  | Stream results                               | ✅      | `ExecuteStreamAsync()` → IAsyncEnumerable         |
|                  | Show/print                                   | ✅      | `ShowAsync()`, `ToStringAsync()`                  |
|                  | Select, Aggregate, Join, Filter, Limit, Sort | ❌      | Use SQL instead                                   |
|                  | Explain plan                                 | ❌      |                                                   |
| **Arrow**        | Apache Arrow support                         | ✅      | Via Apache.Arrow nuget package                    |
|                  | Zero copy support                            | ✅      |                                                   |
| **Advanced**     | UDF registration                             | ❌      |                                                   |
|                  | Catalog management                           | ❌      |                                                   |
|                  | Table providers                              | ❌      |                                                   |
| **Platforms**    | Linux x64                                    | ✅      |                                                   |
|                  | Linux arm64                                  | ✅      |                                                   |
|                  | Windows x64                                  | ✅      |                                                   |
|                  | macOS arm64                                  | ✅      |                                                   |

✅ Implemented    🟡 Partially implemented    ❌ Not yet implemented

## Installation
```bash
dotnet add package DataFusionSharp
```

## Quick Start

```csharp
using DataFusionSharp;

// Create runtime, which manages Tokio runtime and native resources, per application lifetime
using var runtime = DataFusionRuntime.Create();

// Create session context, which manages query execution and state, per logical session lifetime
using var context = runtime.CreateSessionContext();

// Register a CSV file as a table (supports CSV, Parquet, JSONL)
await context.RegisterCsvAsync("orders", "path/to/orders.csv");
// await context.RegisterParquetAsync("orders", "path/to/orders.parquet");
// await context.RegisterJsonAsync("orders", "path/to/orders.json");

// Execute SQL query
using var df = await context.SqlAsync( "SELECT customer_id, sum(amount) AS total FROM orders GROUP BY customer_id");

// Display results to console
await df.ShowAsync();

// Access schema
var schema = await df.GetSchemaAsync();
foreach (var field in schema.FieldsList)
    ... // Process schema field (name, type, etc.)

// Collect as Arrow batches
using var collectedData = await df.CollectAsync();
foreach (var batch in collectedData.Batches)
    ... // Process Arrow RecordBatch...

// Collect as stream of Arrow batches
using var stream = await df.ExecuteStreamAsync();
await foreach (var batch in stream)
    ... // Process streamed RecordBatch...
```

## Requirements

- .NET 8.0 or later
- Supported platforms:
  - Linux (x64, arm64)
  - Windows (x64)
  - macOS (arm64)

## Building from Source

### Prerequisites

- .NET 10.0 SDK or later (how to install: https://learn.microsoft.com/en-us/dotnet/core/install/)
- Rust 1.93+ (how to install: https://rustup.rs)
- Protobuf compiler `protoc` (how to install: https://protobuf.dev/installation/)

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

## SonarQube

[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=nazarii-piontko_datafusion-sharp&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=nazarii-piontko_datafusion-sharp)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=nazarii-piontko_datafusion-sharp&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=nazarii-piontko_datafusion-sharp)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=nazarii-piontko_datafusion-sharp&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=nazarii-piontko_datafusion-sharp)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=nazarii-piontko_datafusion-sharp&metric=sqale_index)](https://sonarcloud.io/summary/new_code?id=nazarii-piontko_datafusion-sharp)

[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=nazarii-piontko_datafusion-sharp&metric=bugs)](https://sonarcloud.io/summary/new_code?id=nazarii-piontko_datafusion-sharp)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=nazarii-piontko_datafusion-sharp&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=nazarii-piontko_datafusion-sharp)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=nazarii-piontko_datafusion-sharp&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=nazarii-piontko_datafusion-sharp)

[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=nazarii-piontko_datafusion-sharp&metric=coverage)](https://sonarcloud.io/summary/new_code?id=nazarii-piontko_datafusion-sharp)

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=nazarii-piontko_datafusion-sharp&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=nazarii-piontko_datafusion-sharp)

[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=nazarii-piontko_datafusion-sharp&metric=ncloc)](https://sonarcloud.io/summary/new_code?id=nazarii-piontko_datafusion-sharp)


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
