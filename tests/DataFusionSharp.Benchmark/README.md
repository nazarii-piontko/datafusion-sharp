# DataFusionSharp Benchmarks

Performance benchmarks for DataFusionSharp using [BenchmarkDotNet](https://benchmarkdotnet.org/).

## Running Benchmarks

```bash
cd tests/DataFusionSharp.Benchmark
dotnet run -c Release
```

## Benchmark Scenarios

### DataFrameBenchmarks

Tests DataFrame operations with varying row counts (1, 100, 10,000, 1,000,000 rows).

| Benchmark        | Description                       |
|------------------|-----------------------------------|
| `CountAsync`     | Count rows in DataFrame           |
| `GetSchemaAsync` | Retrieve Arrow schema             |
| `CollectAsync`   | Collect all data as RecordBatches |

## Results

```
BenchmarkDotNet v0.15.8, Linux Ubuntu 24.04.3 LTS (Noble Numbat)
Intel Xeon Platinum 8275CL CPU 3.00GHz, 1 CPU, 2 logical cores and 1 physical core
.NET SDK 10.0.102
  [Host]     : .NET 8.0.23 (8.0.23, 8.0.2325.60607), X64 RyuJIT x86-64-v4
  DefaultJob : .NET 8.0.23 (8.0.23, 8.0.2325.60607), X64 RyuJIT x86-64-v4
```

| Method         | RowCount |         Mean |      Error |     StdDev |   Gen0 | Allocated |
|----------------|----------|-------------:|-----------:|-----------:|-------:|----------:|
| CountAsync     | 1        |   500.201 us |  0.3151 us |  0.2631 us |      - |     208 B |
| GetSchemaAsync | 1        |     4.112 us |  0.0136 us |  0.0120 us | 0.0534 |    1112 B |
| CollectAsync   | 1        |   324.755 us |  0.7113 us |  0.6306 us |      - |    1712 B |
| CountAsync     | 100      |   500.173 us |  0.2189 us |  0.1828 us |      - |     208 B |
| GetSchemaAsync | 100      |     4.059 us |  0.0168 us |  0.0157 us | 0.0534 |    1112 B |
| CollectAsync   | 100      |   323.459 us |  0.7462 us |  0.6231 us |      - |    1712 B |
| CountAsync     | 10000    |   537.283 us |  0.9703 us |  0.8601 us |      - |     208 B |
| GetSchemaAsync | 10000    |     3.914 us |  0.0087 us |  0.0072 us | 0.0534 |    1112 B |
| CollectAsync   | 10000    |   357.141 us |  1.4852 us |  1.3893 us |      - |    2128 B |
| CountAsync     | 1000000  | 3,358.221 us | 25.1665 us | 23.5408 us |      - |     208 B |
| GetSchemaAsync | 1000000  |     3.919 us |  0.0103 us |  0.0091 us | 0.0534 |    1112 B |
| CollectAsync   | 1000000  | 3,318.863 us | 65.2399 us | 84.8303 us |      - |   54569 B |

## Reference Results From Native DataFusion

```
dataframe      fastest       │ slowest       │ median        │ mean          │ samples │ iters
├─ collect                   │               │               │               │         │
│  ├─ 1        192.8 µs      │ 742.9 µs      │ 203.6 µs      │ 213.8 µs      │ 100     │ 100
│  ├─ 100      196 µs        │ 251.6 µs      │ 201.8 µs      │ 209.8 µs      │ 100     │ 100
│  ├─ 10000    239.7 µs      │ 293.9 µs      │ 246.3 µs      │ 253.3 µs      │ 100     │ 100
│  ╰─ 1000000  1.878 ms      │ 5.764 ms      │ 5.038 ms      │ 4.523 ms      │ 100     │ 100
├─ count                     │               │               │               │         │
│  ├─ 1        425.4 µs      │ 753.7 µs      │ 451.4 µs      │ 454.3 µs      │ 100     │ 100
│  ├─ 100      430.1 µs      │ 489.4 µs      │ 449 µs        │ 451.5 µs      │ 100     │ 100
│  ├─ 10000    464.7 µs      │ 531.9 µs      │ 489 µs        │ 491.4 µs      │ 100     │ 100
│  ╰─ 1000000  2.852 ms      │ 3.435 ms      │ 3.19 ms       │ 3.231 ms      │ 100     │ 100
╰─ get_schema                │               │               │               │         │
   ├─ 1        3.081 ns      │ 3.389 ns      │ 3.356 ns      │ 3.354 ns      │ 100     │ 102400
   ├─ 100      3.08 ns       │ 3.368 ns      │ 3.355 ns      │ 3.354 ns      │ 100     │ 102400
   ├─ 10000    3.347 ns      │ 3.367 ns      │ 3.349 ns      │ 3.351 ns      │ 100     │ 102400
   ╰─ 1000000  3.08 ns       │ 26.05 ns      │ 3.355 ns      │ 3.621 ns      │ 100     │ 102400
```

## Comparison

| Method    |  RowCount | .NET (µs) | Rust (µs) |    Δ Time (µs) |
|-----------|----------:|----------:|----------:|---------------:|
| Count     |         1 |   500.201 |   454.300 |    **+45.901** |
| Count     |       100 |   500.173 |   451.500 |    **+48.673** |
| Count     |    10,000 |   537.283 |   491.400 |    **+45.883** |
| Count     | 1,000,000 | 3,358.221 | 3,231.000 |   **+127.221** |
|           |           |           |           |                |
| Collect   |         1 |   324.755 |   213.800 |   **+110.955** |
| Collect   |       100 |   323.459 |   209.800 |   **+113.659** |
| Collect   |    10,000 |   357.141 |   253.300 |   **+103.841** |
| Collect   | 1,000,000 | 3,318.863 | 4,523.000 | **−1,204.137** |
|           |           |           |           |                |
| GetSchema |         1 |     4.112 |  0.003354 |     **+4.109** |
| GetSchema |       100 |     4.059 |  0.003354 |     **+4.056** |
| GetSchema |    10,000 |     3.914 |  0.003351 |     **+3.911** |
| GetSchema | 1,000,000 |     3.919 |  0.003621 |     **+3.915** |

## Conclusion

Overall, the benchmarks show that the .NET wrapper adds a **mostly constant FFI overhead** on top of the native Rust execution:

* **~4 µs per call** for trivial operations (`GetSchema`).
* **~45–50 µs** for `Count`.
* **~100–115 µs** for `Collect` largely independent of row count. 

For small and medium workloads this overhead dominates, clearly visible when compared to nanosecond-level native Rust timings, while for large workloads (1M rows) the execution cost begins to outweigh the boundary cost and results converge, with some variance even allowing .NET to match or exceed Rust in isolated cases.

## Notes

- Benchmarks measure end-to-end time including native interop overhead
- Native Rust benchmarks source available in [`./native-reference-bench`](./native-reference-bench) directory
- Results may vary based on hardware and system load
