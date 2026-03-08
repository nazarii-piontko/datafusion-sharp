---
slug: /
sidebar_position: 1
title: Home
---

# DataFusionSharp

[![CI](https://github.com/nazarii-piontko/datafusion-sharp/actions/workflows/ci.yml/badge.svg)](https://github.com/nazarii-piontko/datafusion-sharp/actions/workflows/ci.yml)
[![.NET](https://img.shields.io/badge/.NET-8.0+-purple.svg)](https://dotnet.microsoft.com/download)
[![Rust](https://img.shields.io/badge/Rust-1.93+-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/nazarii-piontko/datafusion-sharp/blob/main/LICENSE.txt)

.NET bindings for [Apache DataFusion](https://arrow.apache.org/datafusion/), a fast, extensible query engine built on Apache Arrow for high-performance analytical query processing.

> **Note:** This is an independent community project and is not officially associated with or endorsed by the Apache Software Foundation or the Apache DataFusion project.

## Features

- **SQL queries** — Execute SQL against CSV, Parquet, and JSON files
- **Apache Arrow** — Zero-copy data exchange via the Arrow columnar format
- **Object stores** — Query data on S3, Azure Blob Storage, Google Cloud Storage, or local filesystem
- **Hive partitioning** — Read and write Hive-style partitioned datasets
- **Parameterized queries** — Bind named parameters with type-safe `ScalarValue` types
- **Cross-platform** — Linux (x64/arm64), Windows (x64), macOS (arm64)

## Quick Install

```bash
dotnet add package DataFusionSharp
```

## Minimal Example

```csharp
using DataFusionSharp;

using var runtime = DataFusionRuntime.Create();
using var context = runtime.CreateSessionContext();

await context.RegisterCsvAsync("orders", "orders.csv");

using var df = await context.SqlAsync("SELECT customer_id, sum(amount) AS total FROM orders GROUP BY customer_id");
await df.ShowAsync();
```

## Next Steps

- [Installation](./getting-started/installation) — prerequisites and platform support
- [Quick Start](./getting-started/quick-start) — full working example walkthrough
- [Core Concepts](./guides/core-concepts) — understand the runtime, session, and DataFrame model
