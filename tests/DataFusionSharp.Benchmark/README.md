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
Neoverse-N1, 2 physical cores
.NET SDK 10.0.103
[Host]     : .NET 10.0.3 (10.0.3, 10.0.326.7603), Arm64 RyuJIT armv8.0-a
DefaultJob : .NET 10.0.3 (10.0.3, 10.0.326.7603), Arm64 RyuJIT armv8.0-a
```

| Method         | RowCount | Mean         | Error     | StdDev    | Gen0      | Gen1      | Gen2      | Allocated |
|--------------- |--------- |-------------:|----------:|----------:|----------:|----------:|----------:|----------:|
| CountAsync     | 1        |   617.201 us | 2.8458 us | 2.5227 us |         - |         - |         - |     240 B |
| GetSchemaAsync | 1        |     1.809 us | 0.0029 us | 0.0027 us |    0.0687 |         - |         - |    1176 B |
| CollectAsync   | 1        |   361.289 us | 3.7899 us | 3.5451 us |         - |         - |         - |    1824 B |
| CountAsync     | 100      |   616.386 us | 3.4186 us | 3.1978 us |         - |         - |         - |     240 B |
| GetSchemaAsync | 100      |     1.726 us | 0.0031 us | 0.0029 us |    0.0687 |         - |         - |    1176 B |
| CollectAsync   | 100      |   361.824 us | 3.0167 us | 2.8218 us |         - |         - |         - |    1824 B |
| CountAsync     | 10000    |   620.959 us | 1.8969 us | 1.7743 us |         - |         - |         - |     240 B |
| GetSchemaAsync | 10000    |     1.828 us | 0.0023 us | 0.0022 us |    0.0687 |         - |         - |    1176 B |
| CollectAsync   | 10000    |   385.928 us | 1.3691 us | 1.2137 us |   18.0664 |   18.0664 |   18.0664 |    2302 B |
| CountAsync     | 1000000  | 3,415.513 us | 8.8833 us | 8.3094 us |         - |         - |         - |     240 B |
| GetSchemaAsync | 1000000  |     1.685 us | 0.0020 us | 0.0019 us |    0.0687 |         - |         - |    1176 B |
| CollectAsync   | 1000000  | 3,001.268 us | 5.0928 us | 4.2527 us | 1230.4688 | 1230.4688 | 1230.4688 |   59737 B |

## Reference Results From Native DataFusion

```
dataframe      fastest       │ slowest       │ median        │ mean          │ samples │ iters
├─ collect                   │               │               │               │         │
│  ├─ 1        272.6 µs      │ 1.037 ms      │ 292.5 µs      │ 303.6 µs      │ 100     │ 100
│  ├─ 100      277.9 µs      │ 344.6 µs      │ 292.9 µs      │ 297.6 µs      │ 100     │ 100
│  ├─ 10000    332.7 µs      │ 407.5 µs      │ 347.7 µs      │ 351.4 µs      │ 100     │ 100
│  ╰─ 1000000  5.375 ms      │ 5.853 ms      │ 5.514 ms      │ 5.525 ms      │ 100     │ 100
├─ count                     │               │               │               │         │
│  ├─ 1        537.3 µs      │ 1.049 ms      │ 610.9 µs      │ 614.7 µs      │ 100     │ 100
│  ├─ 100      563.5 µs      │ 679.1 µs      │ 618.8 µs      │ 620.8 µs      │ 100     │ 100
│  ├─ 10000    564.7 µs      │ 717.8 µs      │ 631.7 µs      │ 629.7 µs      │ 100     │ 100
│  ╰─ 1000000  3.165 ms      │ 3.443 ms      │ 3.345 ms      │ 3.336 ms      │ 100     │ 100
╰─ get_schema                │               │               │               │         │
   ├─ 1        5.655 ns      │ 5.687 ns      │ 5.671 ns      │ 5.671 ns      │ 100     │ 51200
   ├─ 100      5.654 ns      │ 53.5 ns       │ 5.671 ns      │ 6.149 ns      │ 100     │ 51200
   ├─ 10000    5.654 ns      │ 5.687 ns      │ 5.671 ns      │ 5.67 ns       │ 100     │ 51200
   ╰─ 1000000  5.654 ns      │ 5.687 ns      │ 5.671 ns      │ 5.67 ns       │ 100     │ 51200
```

## Notes

- Benchmarks measure end-to-end time including native interop overhead
- Native Rust benchmarks source available in [`./native-reference-bench`](./native-reference-bench) directory
- Results may vary based on hardware and system load
