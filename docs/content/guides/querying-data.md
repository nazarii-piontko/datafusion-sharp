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

Parameters are passed as a collection of `NamedScalarValueAndMetadata` tuples. Thanks to implicit conversions, you can pass C# primitives directly:

```csharp
// These are equivalent
[("id", 42)]                                     // int -> ScalarValue.Int32
[("id", new ScalarValue.Int32(42))]              // explicit ScalarValue
[("id", (ScalarValue.Int32(42), (ArrowType?)null))]  // full tuple form
```

## ScalarValue Types

`ScalarValue` is a type hierarchy mirroring DataFusion's Rust `ScalarValue` enum. Common types with implicit conversions:

| C# Type | ScalarValue | Notes |
|---------|-------------|-------|
| `bool` | `Boolean` | |
| `float` | `Float32` | |
| `double` | `Float64` | |
| `sbyte` | `Int8` | |
| `short` | `Int16` | |
| `int` | `Int32` | |
| `long` | `Int64` | |
| `byte` | `UInt8` | |
| `ushort` | `UInt16` | |
| `uint` | `UInt32` | |
| `ulong` | `UInt64` | |
| `string` | `Utf8` | |
| `byte[]` | `Binary` | |

Additional types available (construct explicitly): `Float16`, `Date32`, `Date64`, `TimestampSecond`, `TimestampMillisecond`, `TimestampMicrosecond`, `TimestampNanosecond`, `Decimal128`, `Decimal256`, `LargeUtf8`, `LargeBinary`, and more.

All nullable variants pass `null` to represent SQL `NULL`:

```csharp
[("name", (string?)null)]  // NULL parameter
```

## Result Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `CountAsync()` | `ulong` | Number of rows |
| `ShowAsync(limit?)` | — | Print formatted table to stdout |
| `ToStringAsync()` | `string` | Formatted table as string |
| `CollectAsync()` | `DataFrameCollectedResult` | All record batches in memory |
| `ExecuteStreamAsync()` | `DataFrameStream` | Async enumerable of record batches |

## Working with RecordBatch

Arrow `RecordBatch` columns are typed arrays. Cast to the appropriate type to access values:

```csharp
using var result = await df.CollectAsync();
foreach (var batch in result.Batches)
{
    var names = (StringArray)batch.Column(0);
    var amounts = (Int64Array)batch.Column(1);

    for (int i = 0; i < batch.Length; i++)
    {
        Console.WriteLine($"{names.GetString(i)}: {amounts.GetValue(i)}");
    }
}
```

Common Arrow array types: `StringArray`, `StringViewArray`, `Int32Array`, `Int64Array`, `FloatArray`, `DoubleArray`, `BooleanArray`, `BinaryArray`, `Date32Array`, `TimestampArray`.

## Reusing a Query Plan

`DataFrame` is consumed by terminal operations. Use `Clone()` to reuse:

```csharp
using var df = await context.SqlAsync("SELECT * FROM orders");
using var df2 = df.Clone();

ulong count = await df.CountAsync();
await df2.ShowAsync();
```
