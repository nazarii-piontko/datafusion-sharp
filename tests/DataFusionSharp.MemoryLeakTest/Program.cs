using DataFusionSharp.Tests;
using Xunit.Sdk;

Console.WriteLine("=== Test started ===");

var test = new StressTests(new TestOutputHelper());
try
{
    Console.WriteLine("=== Running ConcurrentSessions_HandleMultipleQueries_Successfully with Query_WithSchema ===");
    await test.ConcurrentSessions_HandleMultipleQueries_Successfully(StressTestsQueries.Query_WithSchema);
    
    Console.WriteLine("=== Running ConcurrentSessions_HandleMultipleQueries_Successfully with Query_WithStream ===");
    await test.ConcurrentSessions_HandleMultipleQueries_Successfully(StressTestsQueries.Query_WithStream);
    
    Console.WriteLine("=== Running ConcurrentSessions_HandleMultipleQueries_Successfully with Query_WithCollect ===");
    await test.ConcurrentSessions_HandleMultipleQueries_Successfully(StressTestsQueries.Query_WithCollect);
    
    Console.WriteLine("=== Running ConcurrentSessions_HandleMultipleQueries_Successfully with Query_WithCancellation_DuringCollect ===");
    await test.ConcurrentSessions_HandleMultipleQueries_Successfully(StressTestsQueries.Query_WithCancellation_DuringCollect);
}
finally
{
    test.Dispose();
}

#if MEMORY_TEST
Console.WriteLine($"Live RuntimeSafeHandle instances: {DataFusionSharp.Interop.RuntimeSafeHandle.LiveInstances}");
Console.WriteLine($"Live SessionContextSafeHandle instances: {DataFusionSharp.Interop.SessionContextSafeHandle.LiveInstances}");
Console.WriteLine($"Live DataFrameSafeHandle instances: {DataFusionSharp.Interop.DataFrameSafeHandle.LiveInstances}");
Console.WriteLine($"Live DataFrameStreamSafeHandle instances: {DataFusionSharp.Interop.DataFrameStreamSafeHandle.LiveInstances}");
Console.WriteLine($"Live InMemoryStoreSafeHandle instances: {DataFusionSharp.Interop.InMemoryStoreSafeHandle.LiveInstances}");

Console.WriteLine($"Live AsyncOperation instances: {DataFusionSharp.Interop.AsyncOperation.LiveInstances}");
Console.WriteLine($"Live AsyncOperation tokens: {DataFusionSharp.Interop.AsyncOperation.LiveCancellationTokens}");
Console.WriteLine($"Live SyncOperation instances: {DataFusionSharp.Interop.SyncOperation.LiveInstances}");
#endif

Console.WriteLine("=== Forcing GC to check for memory leaks ===");
GC.Collect(2, GCCollectionMode.Forced, true, true);

Console.WriteLine("=== Test completed ===");
