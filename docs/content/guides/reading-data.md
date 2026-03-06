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
    Delimiter = ',',
    SchemaInferMaxRecords = 1000,
});
```

### CsvReadOptions

| Property | Type | Description |
|----------|------|-------------|
| `HasHeader` | `bool?` | Whether the file has a header row |
| `Delimiter` | `char?` | Column delimiter character |
| `Quote` | `char?` | Quote character |
| `Escape` | `char?` | Escape character |
| `Terminator` | `char?` | Line terminator character |
| `Comment` | `char?` | Comment character |
| `NewlinesInValues` | `bool?` | Support newlines inside quoted values |
| `Schema` | `Schema?` | Explicit Arrow schema |
| `SchemaInferMaxRecords` | `ulong?` | Max rows for schema inference |
| `FileExtension` | `string?` | File extension filter |
| `FileCompressionType` | `CompressionType?` | Compression type |
| `NullRegex` | `string?` | Regex pattern for null values |
| `TruncatedRows` | `bool?` | Allow truncated rows |
| `TablePartitionCols` | `IReadOnlyList<PartitionColumn>?` | [Hive partition columns](./hive-partitioning) |

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

| Property | Type | Description |
|----------|------|-------------|
| `Schema` | `Schema?` | Explicit Arrow schema |
| `FileExtension` | `string?` | File extension filter |
| `TablePartitionCols` | `IReadOnlyList<PartitionColumn>?` | [Hive partition columns](./hive-partitioning) |
| `ParquetPruning` | `bool?` | Prune row groups using predicates |
| `SkipMetadata` | `bool?` | Skip metadata in file schema |

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

| Property | Type | Description |
|----------|------|-------------|
| `Schema` | `Schema?` | Explicit Arrow schema |
| `SchemaInferMaxRecords` | `ulong?` | Max rows for schema inference |
| `FileExtension` | `string?` | File extension filter |
| `FileCompressionType` | `CompressionType?` | Compression type |
| `TablePartitionCols` | `IReadOnlyList<PartitionColumn>?` | [Hive partition columns](./hive-partitioning) |

## CompressionType

Applies to `CsvReadOptions.FileCompressionType` and `JsonReadOptions.FileCompressionType`:

`Gzip`, `Bzip2`, `Xz`, `Zstd`, `Uncompressed`

## Deregistering Tables

```csharp
await context.DeregisterTableAsync("orders");
```

## Registering Directories

All registration methods accept both file paths and directory paths. When pointing to a directory, DataFusion reads all matching files within it. This is especially useful with [Hive-style partitioning](./hive-partitioning).
