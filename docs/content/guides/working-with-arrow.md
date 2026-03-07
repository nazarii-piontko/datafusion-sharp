---
sidebar_position: 2
title: Working with Arrow
---

# Working with Apache Arrow

## What is Apache Arrow?

[Apache Arrow](https://arrow.apache.org/) is a columnar in-memory data format designed for efficient analytics. It enables zero-copy data sharing between systems and languages.

DataFusion uses Arrow as its native data format — all query results are returned as Arrow data structures.

## Columnar vs Row-based

Traditional databases return data row by row. Arrow stores data **column by column** — all values of a single column are stored contiguously in memory. This layout is optimized for analytical queries that scan and aggregate columns.

When you execute a query with DataFusionSharp, you don't get row objects. Instead, you get **typed column arrays** where each column is a separate array of values:

```
Row-based:
┌───────┬─────┐
│ Alice │ 100 │
│ Bob   │ 250 │
│ Carol │  75 │
└───────┴─────┘
```

```
Columnar (Arrow):
  names:   ["Alice", "Bob", "Carol"]
  amounts: [100, 250, 75]
```

## RecordBatch

A `RecordBatch` is Arrow's unit of data — a group of rows stored in columnar form. Each batch has:

- A **Schema** describing column names and types
- A set of **typed column arrays**, one per column

Query results may arrive as **multiple batches**. Use `CollectAsync()` to get all batches at once, or `ExecuteStreamAsync()` to process them as a stream:

```csharp
// All at once
using var result = await df.CollectAsync();
foreach (var batch in result.Batches)
{
    // process batch
}

// Streaming
await using var stream = await df.ExecuteStreamAsync();
await foreach (var batch in stream)
{
    // process batch
}
```

## Why You Need to Cast

`batch.Column()` returns `IArrowArray` — a generic interface. To read actual values, you cast to the concrete array type:

```csharp
var ids = (Int64Array)batch.Column("id");
long? firstId = ids.GetValue(0);
```

The cast is necessary because Arrow arrays are strongly typed at the storage level, but the `RecordBatch` API returns the common interface. Without casting, you can't access typed values.

## Some Type Mapping

| SQL Type           | Arrow Array Type | C# Value Type     |
|--------------------|------------------|-------------------|
| `BOOLEAN`          | `BooleanArray`   | `bool?`           |
| `TINYINT`          | `Int8Array`      | `sbyte?`          |
| `SMALLINT`         | `Int16Array`     | `short?`          |
| `INT`              | `Int32Array`     | `int?`            |
| `BIGINT`           | `Int64Array`     | `long?`           |
| `FLOAT`            | `FloatArray`     | `float?`          |
| `DOUBLE`           | `DoubleArray`    | `double?`         |
| `VARCHAR` / `TEXT` | `StringArray`    | `string?`         |
| `BINARY`           | `BinaryArray`    | `byte[]?`         |
| `DATE`             | `Date32Array`    | `DateTime?`       |
| `TIMESTAMP`        | `TimestampArray` | `DateTimeOffset?` |

## Reading Values

### Direct cast + index

Cast the column and access values by row index:

```csharp
using var result = await df.CollectAsync();
foreach (var batch in result.Batches)
{
    var names = (StringArray)batch.Column("name");
    var amounts = (Int64Array)batch.Column("amount");

    for (int i = 0; i < batch.Length; i++)
    {
        Console.WriteLine($"{names.GetString(i)}: {amounts.GetValue(i)}");
    }
}
```

### Extension methods

DataFusionSharp provides extension methods that return `IEnumerable<T?>`, making columns easy to use with LINQ:

```csharp
using DataFusionSharp; // for extension methods

foreach (var batch in result.Batches)
{
    var names = batch.Column("name").AsString();
    var amounts = batch.Column("amount").AsInt64();

    foreach (var (name, amount) in names.Zip(amounts))
    {
        Console.WriteLine($"{name}: {amount}");
    }
}
```

Available extensions: `AsString()`, `AsInt64()`, `AsDouble()`, `AsBool()`.

## Handling Nulls

Arrow arrays are nullable. Values come back as nullable C# types (`long?`, `string?`, etc.). Always account for nulls:

```csharp
var amounts = (Int64Array)batch.Column("amount");
for (int i = 0; i < batch.Length; i++)
{
    long? value = amounts.GetValue(i);
    if (value is null)
        Console.WriteLine("NULL");
    else
        Console.WriteLine(value.Value);
}
```

With extension methods, null handling works naturally with LINQ:

```csharp
var total = batch.Column("amount").AsInt64()
    .Where(v => v.HasValue)
    .Sum(v => v!.Value);
```

## String Columns

DataFusion may return string data as `StringArray`, `StringViewArray`, or `LargeStringArray` depending on the query plan. Rather than guessing the concrete type, use the `.AsString()` extension method which handles all three:

```csharp
// Works regardless of the underlying string array type
var names = batch.Column("name").AsString();
```

If you cast directly, prefer checking the type first or using pattern matching.
