---
sidebar_position: 6
title: Hive-Style Partitioning
---

# Hive-Style Partitioning

Hive-style partitioning encodes column values in directory paths. For example:

```
products/
  category=electronics/
    data.csv
  category=clothing/
    data.csv
  category=food/
    data.csv
```

DataFusion automatically discovers partition values from the directory structure and exposes them as regular columns in SQL.

## Reading Partitioned Data

Use `TablePartitionCols` on the read options to declare partition columns:

```csharp
using Apache.Arrow.Types;
using DataFusionSharp.Formats;
using DataFusionSharp.Formats.Csv;

await context.RegisterCsvAsync("products", "products/", new CsvReadOptions
{
    TablePartitionCols = [new PartitionColumn("category", StringType.Default)]
});

// 'category' is now a regular column
using var df = await context.SqlAsync(
    "SELECT name, price, category FROM products WHERE category = 'electronics'");
```

DataFusion prunes partitions automatically — only directories matching the filter are read.

### Multiple Partition Columns

```csharp
using DataFusionSharp.Formats.Parquet;

// Directory layout: data/puYear=2024/puMonth=1/part-0.parquet
await context.RegisterParquetAsync("trips", "data/", new ParquetReadOptions
{
    TablePartitionCols = [
        new PartitionColumn("puYear", Int32Type.Default),
        new PartitionColumn("puMonth", Int32Type.Default),
    ]
});

using var df = await context.SqlAsync(
    "SELECT * FROM trips WHERE puYear = 2024 AND puMonth = 1");
```

### PartitionColumn

```csharp
public record PartitionColumn(string Name, IArrowType ArrowType);
```

The `ArrowType` must match the actual values in the directory names. Common choices:
- `StringType.Default` — for text values
- `Int32Type.Default` — for integer values
- `BooleanType.Default` — for boolean values

### Supported Formats

`TablePartitionCols` is available on all read options:
- `CsvReadOptions`
- `ParquetReadOptions`
- `JsonReadOptions`

## Writing Partitioned Data

Use `DataFrameWriteOptions.PartitionBy` to write Hive-style partitioned output:

```csharp
using DataFusionSharp.Formats;

var writeOptions = new DataFrameWriteOptions
{
    PartitionBy = ["country"]
};

await df.WriteCsvAsync("output/", dataFrameWriteOptions: writeOptions);
```

This produces:

```
output/
  country=US/
    part-0.csv
  country=UK/
    part-0.csv
```

Works with all write formats:

```csharp
await df.WriteParquetAsync("output/", dataFrameWriteOptions: writeOptions);
await df.WriteJsonAsync("output/", dataFrameWriteOptions: writeOptions);
```

## Object Stores + Partitioning

Combine object stores with partitioned reads for remote data:

```csharp
// Register S3 store
context.RegisterS3ObjectStore("s3://arrow-datasets", new S3ObjectStoreOptions
{
    BucketName = "arrow-datasets",
    Region = "us-east-2",
    SkipSignature = true,
});

// Register partitioned Parquet dataset on S3
await context.RegisterParquetAsync("nyc_trips",
    "s3://arrow-datasets/nyc-trips/", new ParquetReadOptions
    {
        TablePartitionCols = [
            new PartitionColumn("year", Int32Type.Default),
            new PartitionColumn("month", Int32Type.Default),
        ]
    });

// Query with partition pruning
using var df = await context.SqlAsync(
    "SELECT * FROM nyc_trips WHERE year = 2024 AND month = 1 LIMIT 10");
```
