# DataFusionSharp

.NET bindings for [Apache DataFusion](https://arrow.apache.org/datafusion/), a fast, extensible query engine built on Apache Arrow for high-performance analytical query processing.

> **Note:** This is an independent community project and is not officially associated with or endorsed by the Apache Software Foundation or the Apache DataFusion project.

## Features

TODO: List key features of the library

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
  - Linux (x64)
  - Windows (x64) 🚧 Planned
  - macOS (x64, ARM64) 🚧 Planned

## Building from Source

### Prerequisites

- .NET 8.0 SDK or later
- Rust toolchain (1.70+) - Install from https://rustup.rs

### Build Steps

TODO: Provide detailed build instructions

## Documentation

TODO: Link to or provide documentation for the library

## Project Structure

TODO: Describe the project structure if necessary

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
