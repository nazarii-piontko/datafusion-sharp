---
sidebar_position: 2
title: Querying Data
---

# Querying Data

## Executing SQL

```csharp
using var df = await context.SqlAsync("SELECT * FROM orders WHERE amount > 100");
```

## Parameterized Queries

Use named parameters with `$paramName` syntax to safely bind values:

```csharp
using var df = await context.SqlAsync(
    "SELECT * FROM orders WHERE status = $status AND amount > $min_amount",
    [("status", "Completed"), ("min_amount", 50.0)]);
```

Parameters are passed as a collection of `NamedScalarValueAndMetadata` (the naming is similar to original DataFusion library).

Thanks to implicit conversions, you can pass C# primitives directly:

```csharp
// These are equivalent

// int -> ScalarValue.Int32
[("id", 42)]

// explicit ScalarValue
[("id", new ScalarValue.Int32(42))]

// full form without metadata
[new NamedScalarValueAndMetadata("id", new ScalarValueAndMetadata(new ScalarValue.Int32(42)))]

// full form with metadata
[new NamedScalarValueAndMetadata("id", new ScalarValueAndMetadata(new ScalarValue.Int32(42), new Dictionary<string, string>()))]
```

## ScalarValue Types

`ScalarValue` is a type hierarchy mirroring DataFusion's Rust `ScalarValue` enum. Common types with implicit conversions:

| C# Type          | ScalarValue            |
|------------------|------------------------|
| `bool`           | `Boolean`              |
| `float`          | `Float32`              |
| `double`         | `Float64`              |
| `decimal`        | `Decimal128`           |
| `sbyte`          | `Int8`                 |
| `short`          | `Int16`                |
| `int`            | `Int32`                |
| `long`           | `Int64`                |
| `byte`           | `UInt8`                |
| `ushort`         | `UInt16`               |
| `uint`           | `UInt32`               |
| `ulong`          | `UInt64`               |
| `string`         | `Utf8`                 |
| `byte[]`         | `Binary`               |
| `DateOnly`       | `Date32`               |
| `TimeOnly`       | `Time64Microsecond`    |
| `TimeSpan`       | `DurationMillisecond`  |
| `DateTimeOffset` | `TimestampMillisecond` |

Additional types available (construct explicitly): `Float16`, `Date64`, `TimestampSecond`, `TimestampMicrosecond`, `TimestampNanosecond`, `Decimal256`, `LargeUtf8`, `LargeBinary`, and more.

All nullable variants pass `null` to represent SQL `NULL`:

```csharp
[("name", (string?)null)]
```

or

```csharp
[("name", ScalarValue.Utf8.Null)]
```

## Result Methods

| Method                 | Returns                    | Description                        |
|------------------------|----------------------------|------------------------------------|
| `CountAsync()`         | `ulong`                    | Number of rows                     |
| `ShowAsync(limit?)`    | —                          | Print formatted table to stdout    |
| `ToStringAsync()`      | `string`                   | Formatted table as string          |
| `CollectAsync()`       | `DataFrameCollectedResult` | All record batches in memory       |
| `ExecuteStreamAsync()` | `DataFrameStream`          | Async enumerable of record batches |

## Working with RecordBatch

Arrow `RecordBatch` columns are typed arrays. Cast to the appropriate type to access values. For background on Arrow's columnar model and why casting is needed, see [Working with Arrow](./working-with-arrow).

```csharp
using var result = await df.CollectAsync();
foreach (var batch in result.Batches)
{
    var names = (StringArray)batch.Column(0); // access by index or name
    var amounts = (Int64Array)batch.Column(1); // access by index or name

    for (int i = 0; i < batch.Length; i++)
    {
        Console.WriteLine($"{names.GetString(i)}: {amounts.GetValue(i)}");
    }
}
```

Common Arrow array types: `StringArray`, `StringViewArray`, `Int32Array`, `Int64Array`, `FloatArray`, `DoubleArray`, `BooleanArray`, `BinaryArray`, `Date32Array`, `TimestampArray`.

### Extension Methods for Column Access

Instead of casting to typed arrays manually, you can use extension methods that return `IEnumerable<T?>` for LINQ-friendly access:

```csharp
using var result = await df.CollectAsync();
foreach (var batch in result.Batches)
{
    var names = batch.Column("name").AsString(); // IEnumerable<string?>
    var amounts = batch.Column("amount").AsInt64(); // IEnumerable<long?>
    var prices = batch.Column("price").AsDouble(); // IEnumerable<double?>
    var active = batch.Column("active").AsBool(); // IEnumerable<bool?>

    foreach (var name in names)
        Console.WriteLine(name);
}
```

These methods are defined in `ArrayExtensions` and work on some `IArrowArray` column.

## Binding Parameters to a DataFrame

`WithParameters` binds parameter values to a `DataFrame` created from a parameterized SQL query. It accepts the same `NamedScalarValueAndMetadata` as `SqlAsync` and returns the same `DataFrame` instance for chaining:

```csharp
using var df = await context.SqlAsync("SELECT * FROM orders WHERE id = $id");
df.WithParameters([("id", 123)]);
await df.ShowAsync();
```

## Reusing a Query Plan

`Clone()` creates a copy of the query plan, which is useful when you want to execute the same parameterized query with different parameter values:

```csharp
using var df = await context.SqlAsync("SELECT * FROM orders WHERE status = $status");
using var df1 = df.Clone();
using var df2 = df.Clone();

df1.WithParameters([("status", "Completed")]);
df2.WithParameters([("status", "Pending")]);

await df1.ShowAsync();
await df2.ShowAsync();
```
