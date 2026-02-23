using Apache.Arrow;

namespace DataFusionSharp.Tests;

[Trait("Category", "Stress")]
public sealed class StressTests : IDisposable
{
    private readonly DataFusionRuntime _runtime = DataFusionRuntime.Create();

    public delegate Task QueryFunc(DataFusionRuntime runtime);
    
    /// <summary>
    /// Tests that the runtime can handle multiple concurrent sessions executing queries simultaneously.
    /// Each query runs in its own session context (as sessions are not thread-safe).
    /// </summary>
    /// <remarks>
    /// This test is not deterministic and may fail if the runtime does not handle concurrency properly,
    /// but it can also fail due to other reasons (e.g., resource exhaustion).
    /// Therefore, if this test fails, it should be investigated further to determine the root cause.
    /// </remarks>
    [Theory(Timeout = 300_000)]
    [ClassData(typeof(StressTestsQueriesData))]
    public async Task ConcurrentSessions_HandleMultipleQueries_Successfully(QueryFunc queryFunc)
    {
        // Force running the query on a thread pool thread to simulate concurrent access from multiple threads.
        Task RunQueryAsync() => Task.Run(() => queryFunc(_runtime));

        // Ensure meaningful concurrency.
        var concurrencyLevel = Environment.ProcessorCount * 2;
        
        // Limit total queries but allow for a large number to increase the chance of catching concurrency issues.
        var totalQueries = Math.Min(concurrencyLevel * 256, 32_000);
        
        // Start with concurrencyLevel queries and then, as each query completes, start a new one until totalQueries queries in total.
        var tasks = Enumerable.Range(0, concurrencyLevel).Select(_ => RunQueryAsync()).ToHashSet();
        for (var i = concurrencyLevel; i < totalQueries; ++i)
        {
            var task = await Task.WhenAny(tasks);
            
            // If any task failed/canceled, stop starting new tasks to avoid system overwhelming.
            // The test will fail on await the remaining tasks below.
            if (task.IsFaulted || task.IsCanceled)
                break;
            
            // Start a new query to maintain the concurrency level, but only if the previous task completed successfully.
            tasks.Remove(task);
            tasks.Add(RunQueryAsync());
        }
        
        // Wait for all remaining tasks to complete.
        // If any of the tasks failed, this will throw an exception and fail the test.
        await Task.WhenAll(tasks);
    }
    
    public void Dispose()
    {
        // Use shutdown as we want to fail the test if the runtime fails.
        _runtime.Shutdown();
        _runtime.Dispose();
    }
}

public sealed class StressTestsQueriesData : TheoryData<StressTests.QueryFunc>
{
    public StressTestsQueriesData()
    {
        Add(StressTestsQueries.Query_WithCollect);
        Add(StressTestsQueries.Query_WithStream);
    }
}

public static class StressTestsQueries
{
    public static async Task Query_WithCollect(DataFusionRuntime runtime)
    {
        ArgumentNullException.ThrowIfNull(runtime);

        var rowsCount = GetRandomRowsCount();

        using var context = runtime.CreateSessionContext();
        using var dataFrame = await context.SqlAsync($"SELECT s.value AS id, 'Generated value for collect #' || s.value AS val, 'Collect constant value' as const_val FROM generate_series(1, {rowsCount}) AS s");
            
        using var collected = await dataFrame.CollectAsync();
        
        var rows = new List<Row>(rowsCount);
        foreach (var batch in collected.Batches)
        {
            var idArr = (Int64Array)batch.Column("id");
            var valArr = (StringArray)batch.Column("val");
            var constValArr = (StringArray)batch.Column("const_val");
            for (var i = 0; i < batch.Length; i++)
                rows.Add(new Row(idArr.GetValue(i)!.Value, valArr.GetString(i)!, constValArr.GetString(i)!));
        }
        rows.Sort((x, y) => x.Id.CompareTo(y.Id));

        for (var i = 0; i < rows.Count; i++)
        {
            var row = rows[i];
            if (i + 1 != row.Id || row.Value != $"Generated value for collect #{row.Id}" || row.ConstValue != "Collect constant value")
                Assert.Fail($"Unexpected row data for id {row.Id} in query with {rowsCount} rows: {row}");
        }
    }
    
    public static async Task Query_WithStream(DataFusionRuntime runtime)
    {
        ArgumentNullException.ThrowIfNull(runtime);

        var rowsCount = GetRandomRowsCount();

        using var context = runtime.CreateSessionContext();
        using var dataFrame = await context.SqlAsync($"SELECT s.value AS id, 'Generated value for stream #' || s.value AS val, 'Stream constant value' as const_val FROM generate_series(1, {rowsCount}) AS s");
            
        using var stream = await dataFrame.ExecuteStreamAsync();

        var rows = new List<Row>(rowsCount);
        await foreach (var batch in stream)
        {
            var idArr = (Int64Array)batch.Column("id");
            var valArr = (StringArray)batch.Column("val");
            var constValArr = (StringArray)batch.Column("const_val");
            for (var i = 0; i < batch.Length; i++)
                rows.Add(new Row(idArr.GetValue(i)!.Value, valArr.GetString(i)!, constValArr.GetString(i)!));
        }
        rows.Sort((x, y) => x.Id.CompareTo(y.Id));

        for (var i = 0; i < rows.Count; i++)
        {
            var row = rows[i];
            if (i + 1 != row.Id || row.Value != $"Generated value for stream #{row.Id}" || row.ConstValue != "Stream constant value")
                Assert.Fail($"Unexpected row data for id {row.Id} in query with {rowsCount} rows: {row}");
        }
    }
    
    private record struct Row(long Id, string Value, string ConstValue);
    
    private static int GetRandomRowsCount()
    {
        const int minRows = 2;
        const int maxRows = 64_000;
        
        return Random.Shared.Next(minRows, maxRows);
    }
}