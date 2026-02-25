# DataFusionSharp MemoryLeak Test

## Overview
This sample runs stress queries to help profile memory usage in DataFusionSharp.
It executes the stress test workload defined in `Program.cs` and is intended to be run under memory profiler.
Library author uses [`heattrack`](https://github.com/KDE/heaptrack) for this purpose, but any memory profiler that supports native code should work.

## Prerequisites
- Linux
- `dotnet` SDK
- `heaptrack` (and optionally `heaptrack_gui`) or another memory profiler that supports native code (e.g. `valgrind`)

## Build
Run from the repository root:

```bash
dotnet build tests/DataFusionSharp.MemoryLeakTest/DataFusionSharp.MemoryLeakTest.csproj -c Release
```

## Run with `heaptrack`
Run from the repository root. Use the built executable from `bin/Release` so `heaptrack`
profiles the app directly:

```bash
heaptrack tests/DataFusionSharp.MemoryLeakTest/bin/Release/net10.0/DataFusionSharp.MemoryLeakTest
```

`heaptrack` writes a file named like `heaptrack.<process>.<pid>.gz` in the current working directory.

If `heaptrack_gui` is installed, `heaptrack` will open the GUI automatically after capture.

## Analyze results
CLI:

```bash
heaptrack --analyze heaptrack.<process>.<pid>.gz
```

GUI (optional):

```bash
heaptrack_gui heaptrack.<process>.<pid>.gz
```

## Run with `valgrind`
Run from the repository root. Use the built executable from `bin/Release` so `valgrind` profiles the app directly:
```bash
valgrind --leak-check=full --track-origins=yes tests/DataFusionSharp.MemoryLeakTest/bin/Release/net10.0/DataFusionSharp.MemoryLeakTest
```