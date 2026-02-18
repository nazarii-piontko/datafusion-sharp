using System.Collections.Concurrent;

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
    
    /// <summary>
    /// Tests that the runtime can handle multiple concurrent sessions executing queries simultaneously.
    /// Each query runs in its own session context (as sessions are not thread-safe).
    /// </summary>
    /// <remarks>
    /// This test is not deterministic and may fail if the runtime does not handle concurrency properly,
    /// but it can also fail due to other reasons (e.g., resource exhaustion).
    /// Therefore, if this test fails, it should be investigated further to determine the root cause.
    /// </remarks>
    [Fact(Timeout = 300_000)]
    [Trait("Category", "Stress")]
    public async Task ConcurrentSessions_HandleMultipleQueries_Successfully()
    {
        var activeTasks = new ConcurrentDictionary<int, bool>();
        
        async Task QueryAsync()
        {
            // Track the number of concurrent threads executing queries to ensure we have meaningful concurrency.
            activeTasks.TryAdd(Environment.CurrentManagedThreadId, true);
            
            using var context = _runtime.CreateSessionContext();
            using var dataFrame = await context.SqlAsync("SELECT s.value AS id FROM generate_series(1, 3) AS s");
            
            var schema = await dataFrame.GetSchemaAsync();
            Assert.NotNull(schema); // Simple check to ensure the query executed properly.
            
            var collectedData = await dataFrame.CollectAsync();
            Assert.Single(collectedData.Batches); // Simple check to ensure we got any batches back.
        }

        // Force running the query on a thread pool thread to simulate concurrent access from multiple threads.
        Task RunQueryAsync() => Task.Run(QueryAsync);

        // Ensure meaningful concurrency.
        var concurrencyLevel = Math.Max(Environment.ProcessorCount * 2, 8);
        
        // Limit total queries but allow for a large number to increase the chance of catching concurrency issues.
        var totalQueries = Math.Min(concurrencyLevel * 2_000, 64_000);
        
        // Start with concurrencyLevel queries and then, as each query completes, start a new one until totalQueries queries in total.
        var tasks = Enumerable.Range(0, concurrencyLevel).Select(_ => RunQueryAsync()).ToHashSet();
        for (var i = concurrencyLevel; i < totalQueries; ++i)
        {
            var task = await Task.WhenAny(tasks);
            
            // If any task failed/canceled, stop starting new tasks to avoid system overwhelming.
            // The test will fail on await the remaining tasks below.
            if (task.IsFaulted || task.IsCanceled)
                break;
            
            tasks.Remove(task);
            tasks.Add(RunQueryAsync());
        }
        
        // Wait for all remaining tasks to complete.
        // If any of the tasks failed, this will throw an exception and fail the test.
        await Task.WhenAll(tasks);
        
        Assert.True(activeTasks.Count >= Environment.ProcessorCount, 
            $"Expected at least {Environment.ProcessorCount} concurrent threads, but got {activeTasks.Count}");
    }

    public void Dispose()
    {
        _runtime.Dispose();
    }
}