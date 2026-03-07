---
sidebar_position: 2
title: Quick Start
---

# Quick Start

This walkthrough covers the full lifecycle: creating a runtime, registering data, executing SQL, and consuming results.

## Full Example

```csharp
using Apache.Arrow;
using Apache.Arrow.Types;
using DataFusionSharp;
using DataFusionSharp.Formats;
using DataFusionSharp.Formats.Csv;

// 1. Create a runtime (one per application)
using var runtime = DataFusionRuntime.Create();

// 2. Create a session context (one per logical session)
using var context = runtime.CreateSessionContext();

// 3. Register data sources
await context.RegisterCsvAsync("customers", "Data/customers.csv");
await context.RegisterCsvAsync("orders", "Data/orders.csv");

// 4. Execute a SQL query with a named parameter
using var df = await context.SqlAsync(
    """
    SELECT
        c.customer_name,
        sum(o.order_amount) AS total_amount
    FROM orders AS o
        JOIN customers AS c ON o.customer_id = c.customer_id
    WHERE o.order_status = $order_status
    GROUP BY c.customer_name
    ORDER BY c.customer_name
    """,
    [("order_status", "Completed")]);

// 5. Display results to console
await df.ShowAsync();
```

## Consuming Results

There are several ways to work with query results:

### Print to Console

```csharp
// Print a formatted table to stdout
await df.ShowAsync();

// Get as a string instead
string text = await df.ToStringAsync();
```

### Row Count

```csharp
ulong count = await df.CountAsync();
```

### Collect All Data

```csharp
using var result = await df.CollectAsync();
foreach (var batch in result.Batches)
{
    for (int r = 0; r < batch.Length; r++)
    {
        var name = ((StringArray)batch.Column(0)).GetString(r);
        var amount = ((Int64Array)batch.Column(1)).GetValue(r);
        Console.WriteLine($"{name}: {amount}");
    }
}
```

### Stream Results

For large datasets, streaming processes one batch at a time:

```csharp
using var stream = await df.ExecuteStreamAsync();
await foreach (var batch in stream)
{
    // Process each RecordBatch as it arrives
}
```

## Schema Inspection

```csharp
var schema = df.GetSchema();
foreach (var field in schema.FieldsList)
    Console.WriteLine($"{field.Name}: {field.DataType}");
```

## Next Steps

- [Core Concepts](../guides/core-concepts) — understand the runtime, session, and DataFrame lifecycle
- [Querying Data](../guides/querying-data) — parameters, scalar types, and result patterns
- [Reading Data](../guides/reading-data) — CSV, Parquet, and JSON options
