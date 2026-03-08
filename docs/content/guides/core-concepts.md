---
sidebar_position: 1
title: Core Concepts
---

# Core Concepts

DataFusionSharp follows a three-tier architecture: **Runtime** -> **SessionContext** -> **DataFrame**.

## DataFusionRuntime

The runtime manages a [Tokio](https://tokio.rs/) async runtime — the Rust equivalent of a thread pool with async task scheduling. It is the entry point for all DataFusion operations.

```csharp
// Default configuration
using var runtime = DataFusionRuntime.Create();

// Custom thread pool
using var runtime = DataFusionRuntime.Create(workerThreads: 4, maxBlockingThreads: 8);
```

**Guidelines:**
- Create **one runtime per application** — it is thread-safe and designed to be shared.
- Dispose when the application shuts down. The runtime owns all native resources.

## SessionContext

A session context maintains its own catalog of registered tables and configuration state.

```csharp
using var context = runtime.CreateSessionContext();

// Register tables, execute queries...
await context.RegisterCsvAsync("orders", "orders.csv");
using var df = await context.SqlAsync("SELECT * FROM orders");
```

**Guidelines:**
- Create one per logical session or unit of work.
- **Not thread-safe** — do not call methods on the same instance from multiple threads concurrently.
- Multiple contexts can coexist on the same runtime with independent catalogs.

## DataFrame

A DataFrame represents a lazy query plan. The query is **not executed** until a materialization operation is called.

```csharp
// Lazy — just builds the plan
using var df = await context.SqlAsync("SELECT * FROM orders WHERE amount > 100");

// Materialization operations — these trigger execution
await df.ShowAsync();                    // Print to stdout
string text = await df.ToStringAsync();  // Get as string
ulong count = await df.CountAsync();     // Row count
using var data = await df.CollectAsync();       // All batches in memory
using var stream = await df.ExecuteStreamAsync(); // Streaming batches
```

**Guidelines:**
- **Not thread-safe** — do not call methods concurrently.
- Always dispose DataFrames, collected results, and streams — they reference native memory.

## Resource Lifecycle

All core types implement `IDisposable`. Use `using` statements to ensure proper cleanup:

```csharp
using var runtime = DataFusionRuntime.Create();
using var context = runtime.CreateSessionContext();

await context.RegisterCsvAsync("data", "data.csv");
using var df = await context.SqlAsync("SELECT * FROM data");
using var result = await df.CollectAsync();

// Process result.Batches...
// Everything is cleaned up when the using blocks exit
```

The data returned by `CollectAsync()` and `ExecuteStreamAsync()` uses **zero-copy Arrow import** — it references memory owned by the native DataFusion runtime. This means:
- Data access is fast (no copying between native and managed memory).
- You **must dispose** the result before the runtime is shut down.
- Do not access Arrow data after disposing the result.
