using Apache.Arrow;
using BenchmarkDotNet.Attributes;

namespace DataFusionSharp.Benchmark;

[MemoryDiagnoser]
public class DataFrameBenchmarks
{
    [Params(1, 100, 10000, 1000000)]
    public int RowCount { get; set; }

    private DataFusionRuntime _runtime = null!;
    private SessionContext _context = null!;
    private DataFrame _dataFrame = null!;

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _runtime = DataFusionRuntime.Create();
        _context = _runtime.CreateSessionContext();
        
        var sql = $"SELECT s.value AS id FROM generate_series(1, {RowCount}) AS s";
        _dataFrame = await _context.SqlAsync(sql);
    }
    
    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _dataFrame.Dispose();
        _context.Dispose();
        _runtime.Dispose();
    }

    [Benchmark]
    public Task<ulong> CountAsync() => _dataFrame.CountAsync();

    [Benchmark]
    public Task<Schema> GetSchemaAsync() => _dataFrame.GetSchemaAsync();

    [Benchmark]
    public Task<DataFrameCollectedData> CollectAsync() => _dataFrame.CollectAsync();
}
