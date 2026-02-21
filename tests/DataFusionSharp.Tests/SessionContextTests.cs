using System.Collections.Concurrent;
using Apache.Arrow;

namespace DataFusionSharp.Tests;

public sealed class SessionContextTests : IDisposable
{
    private readonly DataFusionRuntime _runtime = DataFusionRuntime.Create();

    [Fact]
    public void CreateSessionContext_ReturnsContext()
    {
        // Act
        using var context = _runtime.CreateSessionContext();

        // Assert
        Assert.NotNull(context);
    }

    [Fact]
    public async Task SqlAsync_ReturnsDataFrame()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();

        // Act
        using var dataFrame = await context.SqlAsync("SELECT 1");

        // Assert
        Assert.NotNull(dataFrame);
    }

    [Theory]
    [InlineData("customers")]
    [InlineData("клієнти")]
    public async Task SqlAsync_WithMissingTable_Throws(string tableName)
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();

        // Act & Assert
        var exception = await Assert.ThrowsAsync<DataFusionException>(async () =>
        {
            using var df = await context.SqlAsync($"SELECT * FROM {tableName}");
        });
        
        Assert.Contains(tableName, exception.Message, StringComparison.Ordinal);
    }

    public void Dispose()
    {
        _runtime.Dispose();
    }
}

[Trait("Category", "Stress")]
public sealed class StressTests : IDisposable
{
    private readonly DataFusionRuntime _runtime = DataFusionRuntime.Create();

    public delegate Task QueryFunc(DataFusionRuntime runtime, int rows);
    
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
    [MemberData(nameof(ConcurrentSessions_HandleMultipleQueries_Successfully_Cases))]
    public async Task ConcurrentSessions_HandleMultipleQueries_Successfully(QueryFunc queryFunc)
    {
        var activeTasks = new ConcurrentDictionary<int, bool>();

        // Force running the query on a thread pool thread to simulate concurrent access from multiple threads.
        Task RunQueryAsync(int rows) => Task.Run(() =>
        {
            // Track the number of concurrent threads executing queries to ensure we have meaningful concurrency.
            activeTasks.TryAdd(Environment.CurrentManagedThreadId, true);
            
            return queryFunc(_runtime, rows);
        });

        // Ensure meaningful concurrency.
        var concurrencyLevel = Math.Max(Environment.ProcessorCount * 2, 8);
        
        // Limit total queries but allow for a large number to increase the chance of catching concurrency issues.
        var totalQueries = Math.Min(concurrencyLevel * 2_000, 64_000);
        
        // Use a fixed seed to make the test deterministic
        var rnd = new Random(42);
        const int minRows = 2;
        const int maxRows = 64_000;
        
        // Start with concurrencyLevel queries and then, as each query completes, start a new one until totalQueries queries in total.
        var tasks = Enumerable.Range(0, concurrencyLevel).Select(_ => RunQueryAsync(rnd.Next(minRows, maxRows))).ToHashSet();
        for (var i = concurrencyLevel; i < totalQueries; ++i)
        {
            var task = await Task.WhenAny(tasks);
            
            // If any task failed/canceled, stop starting new tasks to avoid system overwhelming.
            // The test will fail on await the remaining tasks below.
            if (task.IsFaulted || task.IsCanceled)
                break;
            
            // Start a new query to maintain the concurrency level, but only if the previous task completed successfully.
            tasks.Remove(task);
            tasks.Add(RunQueryAsync(rnd.Next(minRows, maxRows)));

            // Periodically force a Gen 1 GC collection to increase the chance of catching issues related to memory management and finalization under concurrent load.
            if (i % (concurrencyLevel * 200) == 0)
            {
                _ = Task.Factory.StartNew(
                    () => GC.Collect(1, GCCollectionMode.Forced),
                    cancellationToken: CancellationToken.None,
                    creationOptions: TaskCreationOptions.LongRunning,
                    scheduler: TaskScheduler.Current);
            }
        }
        
        // Wait for all remaining tasks to complete.
        // If any of the tasks failed, this will throw an exception and fail the test.
        await Task.WhenAll(tasks);
        
        Assert.True(activeTasks.Count >= Environment.ProcessorCount, 
            $"Expected at least {Environment.ProcessorCount} concurrent threads, but got {activeTasks.Count}");
    }
    
    // ReSharper disable once InconsistentNaming
    public static IEnumerable<object[]> ConcurrentSessions_HandleMultipleQueries_Successfully_Cases =>
    [
        [new QueryFunc(Query_WithCollect)],
        [new QueryFunc(Query_WithStream)]
    ];
    
    private static async Task Query_WithCollect(DataFusionRuntime runtime, int rows)
    {    
        using var context = runtime.CreateSessionContext();
        using var dataFrame = await context.SqlAsync($"SELECT s.value AS id FROM generate_series(1, {rows}) AS s");
            
        using var collected = await dataFrame.CollectAsync();
        Assert.NotEmpty(collected.Batches); // Simple check to ensure we got any batches back.

        var ids = new List<long>();
        foreach (var batch in collected.Batches)
            ids.AddRange(((Int64Array)batch.Column("id")).Values);
        ids.Sort();

        for (var i = 0; i < rows; i++)
        {
            if (i + 1 != ids[i])
                Assert.Fail($"Expected id {i + 1} but got {ids[i]} in query with {rows} rows");
        }
    }
    
    private static async Task Query_WithStream(DataFusionRuntime runtime, int rows)
    {    
        using var context = runtime.CreateSessionContext();
        using var dataFrame = await context.SqlAsync($"SELECT s.value AS id FROM generate_series(1, {rows}) AS s");
        
        using var stream = await dataFrame.ExecuteStreamAsync();
        Assert.NotNull(stream);

        var ids = new List<long>();
        await foreach (var batch in stream)
            ids.AddRange(((Int64Array)batch.Column("id")).Values);
        ids.Sort();

        for (var i = 0; i < rows; i++)
        {
            if (i + 1 != ids[i])
                Assert.Fail($"Expected id {i + 1} but got {ids[i]} in query with {rows} rows");
        }
    }

    public void Dispose()
    {
        _runtime.Dispose();
    }
}