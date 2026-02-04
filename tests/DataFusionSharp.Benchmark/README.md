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
BenchmarkDotNet v0.15.8, Linux Debian GNU/Linux 12 (bookworm)
Intel Core i7-10510U CPU 1.80GHz, 1 CPU, 8 logical and 4 physical cores
.NET SDK 10.0.100
  [Host]     : .NET 8.0.20 (8.0.20, 8.0.2025.41914), X64 RyuJIT x86-64-v3
  DefaultJob : .NET 8.0.20 (8.0.20, 8.0.2025.41914), X64 RyuJIT x86-64-v3


| Method         | RowCount | Mean         | Error      | StdDev     | Gen0   | Allocated |
|--------------- |--------- |-------------:|-----------:|-----------:|-------:|----------:|
| CountAsync     | 1        | 1,024.932 us | 20.3613 us | 40.6638 us |      - |     208 B |
| GetSchemaAsync | 1        |     4.764 us |  0.0624 us |  0.0554 us | 0.2594 |    1112 B |
| CollectAsync   | 1        |   568.739 us | 11.0437 us | 14.3599 us |      - |    1712 B |
| CountAsync     | 100      | 1,022.946 us | 20.1709 us | 47.1487 us |      - |     208 B |
| GetSchemaAsync | 100      |     4.789 us |  0.0741 us |  0.0693 us | 0.2594 |    1112 B |
| CollectAsync   | 100      |   568.024 us | 11.3468 us | 12.6120 us |      - |    1712 B |
| CountAsync     | 10000    |   955.131 us | 18.9728 us | 21.8491 us |      - |     208 B |
| GetSchemaAsync | 10000    |     4.916 us |  0.0738 us |  0.0616 us | 0.2594 |    1112 B |
| CollectAsync   | 10000    |   620.891 us | 12.0163 us | 12.3399 us |      - |    2128 B |
| CountAsync     | 1000000  | 4,192.886 us | 60.6931 us | 56.7723 us |      - |     208 B |
| GetSchemaAsync | 1000000  |     4.697 us |  0.0322 us |  0.0269 us | 0.2594 |    1112 B |
| CollectAsync   | 1000000  | 5,780.556 us | 90.7016 us | 84.8423 us | 7.8125 |   54571 B |
```

## Notes

- Benchmarks measure end-to-end time including native interop overhead
- Memory diagnostics enabled via `[MemoryDiagnoser]`
- Results may vary based on hardware and system load
