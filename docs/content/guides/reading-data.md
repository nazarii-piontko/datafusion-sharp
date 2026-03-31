---
sidebar_position: 3
title: Reading Data
---

# Reading Data

Register files as named tables, then query them with SQL.

## CSV

```csharp
// Simple registration
await context.RegisterCsvAsync("orders", "data/orders.csv");

// With custom options
await context.RegisterCsvAsync("orders", "data/orders.csv", new CsvReadOptions
{
    HasHeader = true,
    Delimiter = ';',
    SchemaInferMaxRecords = 1000,
});
```

### CsvReadOptions

| Property                | Type                              | Description                                       |
|-------------------------|-----------------------------------|---------------------------------------------------|
| `HasHeader`             | `bool?`                           | Whether the file has a header row (default: true) |
| `Delimiter`             | `char?`                           | Column delimiter character (default: ',')         |
| `Quote`                 | `char?`                           | Quote character (default, '"')                    |
| `Escape`                | `char?`                           | Escape character                                  |
| `Terminator`            | `char?`                           | Line terminator character                         |
| `Comment`               | `char?`                           | Comment character                                 |
| `NewlinesInValues`      | `bool?`                           | Support newlines inside quoted values             |
| `Schema`                | `Schema?`                         | Explicit Arrow schema                             |
| `SchemaInferMaxRecords` | `ulong?`                          | Max rows for schema inference                     |
| `FileExtension`         | `string?`                         | File extension filter (default: .csv)             |
| `FileCompressionType`   | `CompressionType?`                | Compression type                                  |
| `NullRegex`             | `string?`                         | Regex pattern for null values                     |
| `TruncatedRows`         | `bool?`                           | Allow truncated rows                              |
| `TablePartitionCols`    | `IReadOnlyList<PartitionColumn>?` | [Hive partition columns](./hive-partitioning)     |

## Parquet

```csharp
await context.RegisterParquetAsync("events", "data/events.parquet");

// With options
await context.RegisterParquetAsync("events", "data/events.parquet", new ParquetReadOptions
{
    ParquetPruning = true,
    SkipMetadata = false,
});
```

### ParquetReadOptions

| Property             | Type                              | Description                                   |
|----------------------|-----------------------------------|-----------------------------------------------|
| `Schema`             | `Schema?`                         | Explicit Arrow schema                         |
| `FileExtension`      | `string?`                         | File extension filter (default: .parquet)     |
| `TablePartitionCols` | `IReadOnlyList<PartitionColumn>?` | [Hive partition columns](./hive-partitioning) |
| `ParquetPruning`     | `bool?`                           | Prune row groups using predicates             |
| `SkipMetadata`       | `bool?`                           | Skip metadata in file schema                  |

## JSON

```csharp
await context.RegisterJsonAsync("logs", "data/logs.json");

// With options
await context.RegisterJsonAsync("logs", "data/logs.json", new JsonReadOptions
{
    SchemaInferMaxRecords = 500,
});
```

### JsonReadOptions

| Property                | Type                              | Description                                   |
|-------------------------|-----------------------------------|-----------------------------------------------|
| `Schema`                | `Schema?`                         | Explicit Arrow schema                         |
| `SchemaInferMaxRecords` | `ulong?`                          | Max rows for schema inference                 |
| `FileExtension`         | `string?`                         | File extension filter (default: .json)        |
| `FileCompressionType`   | `CompressionType?`                | Compression type                              |
| `TablePartitionCols`    | `IReadOnlyList<PartitionColumn>?` | [Hive partition columns](./hive-partitioning) |

## RecordBatch

Register an in-memory Arrow `RecordBatch` as a queryable table:

```csharp
using Apache.Arrow;

var idArray = new Int64Array.Builder().Append(1).Append(2).Build();
var nameArray = new StringArray.Builder().Append("Alice").Append("Bob").Build();

var schema = new Schema.Builder()
    .Field(new Field("id", Int64Type.Default, false))
    .Field(new Field("name", StringType.Default, true))
    .Build();

using var batch = new RecordBatch(schema, [idArray, nameArray], 2);

context.RegisterBatch("users", batch);
```

This is useful when you have data already in Arrow format or need to inject programmatically created data into SQL queries -- for example, to join in-memory lookup tables with file-based data:

```csharp
await context.RegisterCsvAsync("orders", "data/orders.csv");
context.RegisterBatch("statuses", statusBatch);

using var df = await context.SqlAsync(
    "SELECT o.order_id, s.description FROM orders o JOIN statuses s ON o.status = s.name");
```

> **Note:** `RegisterBatch` is synchronous, unlike the async file-based registration methods.

## CompressionType

Applies to `CsvReadOptions.FileCompressionType` and `JsonReadOptions.FileCompressionType`:

`Gzip`, `Bzip2`, `Xz`, `Zstd`, `Uncompressed` (default)

## Deregistering Tables

```csharp
await context.DeregisterTableAsync("orders");
```

## Registering Directories

All registration methods accept both file paths and directory paths.

When pointing to a directory, DataFusion reads all matching files within it.

This is especially useful with [Hive-style partitioning](./hive-partitioning).
