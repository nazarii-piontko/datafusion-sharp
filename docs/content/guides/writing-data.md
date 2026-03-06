---
sidebar_position: 4
title: Writing Data
---

# Writing Data

Write DataFrame results to CSV, Parquet, or JSON files.

## Basic Writes

```csharp
using var df = await context.SqlAsync("SELECT * FROM orders");

// Write to different formats
await df.WriteCsvAsync("output/orders.csv");
await df.WriteParquetAsync("output/orders.parquet");
await df.WriteJsonAsync("output/orders.json");
```

## DataFrameWriteOptions

Shared options for all write formats:

```csharp
var writeOptions = new DataFrameWriteOptions
{
    InsertOp = InsertOp.Overwrite,
    IsSingleFileOutput = true,
};

await df.WriteCsvAsync("output/", dataFrameWriteOptions: writeOptions);
```

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `InsertOp` | `InsertOp` | `Append` | Insert behavior: `Append`, `Overwrite`, or `Replace` |
| `IsSingleFileOutput` | `bool` | `false` | Coalesce all partitions into a single file |
| `PartitionBy` | `IEnumerable<string>` | `[]` | Columns for [Hive-style partitioned writes](./hive-partitioning) |

## CSV Write Options

```csharp
await df.WriteCsvAsync("output/", csvWriteOptions: new CsvWriteOptions
{
    HasHeader = true,
    Delimiter = ',',
    Compression = CompressionType.Gzip,
});
```

### CsvWriteOptions

| Property | Type | Description |
|----------|------|-------------|
| `HasHeader` | `bool?` | Write a header row |
| `Delimiter` | `char?` | Column delimiter |
| `Quote` | `char?` | Quote character |
| `Escape` | `char?` | Escape character |
| `Compression` | `CompressionType?` | Output compression |
| `DateFormat` | `string?` | Date format string |
| `DatetimeFormat` | `string?` | Datetime format string |
| `TimestampFormat` | `string?` | Timestamp format string |
| `TimestampTzFormat` | `string?` | Timestamp with timezone format |
| `TimeFormat` | `string?` | Time format string |
| `NullValue` | `string?` | String representation of null |
| `DoubleQuote` | `bool?` | Double-quote special characters |
| `NewlinesInValues` | `bool?` | Support newlines in quoted values |
| `Terminator` | `char?` | Line terminator character |
| `Comment` | `char?` | Comment character |
| `NullRegex` | `string?` | Regex pattern for null values |

## Parquet Write Options

```csharp
await df.WriteParquetAsync("output/", parquetWriteOptions: new ParquetWriteOptions
{
    Compression = ParquetCompression.Snappy,
    MaxRowGroupSize = 1_000_000,
});
```

### ParquetWriteOptions

| Property | Type | Description |
|----------|------|-------------|
| `Compression` | `ParquetCompression?` | Compression codec |
| `MaxRowGroupSize` | `ulong?` | Maximum rows per row group |

### ParquetCompression

`Uncompressed`, `Snappy`, `Gzip`, `Brotli`, `Lz4`, `Lz4Raw`, `Zstd`

## JSON Write Options

```csharp
await df.WriteJsonAsync("output/", jsonWriteOptions: new JsonWriteOptions
{
    Compression = CompressionType.Zstd,
});
```

### JsonWriteOptions

| Property | Type | Description |
|----------|------|-------------|
| `Compression` | `CompressionType?` | Output compression |
